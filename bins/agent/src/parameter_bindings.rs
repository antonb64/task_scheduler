use std::{
    collections::{BTreeMap, HashSet},
    ffi::OsString,
    path::PathBuf,
};

use anyhow::{Result, bail};
use scheduler_core::{
    ExecutionAssignment, ParameterBinding, ParameterBindingSource, ParameterBindingValueType,
    resolve_parameter_bindings, validate_parameter_bindings,
};
use serde_json::Value;

const MAX_CONFIGURED_BINDING_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
pub struct ParameterBindingResolver {
    allowed_environment: HashSet<String>,
    secret_roots: Vec<PathBuf>,
    max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailableBindings {
    pub environment: Vec<String>,
    pub secret_files: Vec<String>,
}

impl ParameterBindingResolver {
    pub fn new(
        allowed_environment: &[String],
        secret_roots: &[PathBuf],
        max_bytes: usize,
    ) -> Result<Self> {
        if max_bytes == 0 || max_bytes > MAX_CONFIGURED_BINDING_BYTES {
            bail!("binding_max_bytes must be between 1 and 1048576");
        }
        if allowed_environment
            .iter()
            .any(|name| !is_portable_environment_name(name))
        {
            bail!("allowed environment bindings must use portable environment-variable syntax");
        }
        let mut roots = Vec::with_capacity(secret_roots.len());
        for root in secret_roots {
            if !root.is_absolute() {
                bail!("secret binding roots must be absolute directories");
            }
            let canonical = root
                .canonicalize()
                .map_err(|_| anyhow::anyhow!("a configured secret binding root is unavailable"))?;
            if !canonical.is_dir() {
                bail!("secret binding roots must be directories");
            }
            roots.push(canonical);
        }
        let allowed_environment = allowed_environment.iter().cloned().collect();
        Ok(Self {
            allowed_environment,
            secret_roots: roots,
            max_bytes,
        })
    }

    pub async fn resolve_assignment(
        &self,
        assignment: &ExecutionAssignment,
    ) -> Result<ExecutionAssignment> {
        let Some(late) = &assignment.snapshot.late_bindings else {
            return Ok(assignment.clone());
        };
        validate_parameter_bindings(&late.bindings, &late.executor_template)
            .map_err(|_| anyhow::anyhow!("parameter binding definition is invalid"))?;
        let mut values = BTreeMap::new();
        for (parameter, binding) in &late.bindings {
            let bytes = match binding.source {
                ParameterBindingSource::Environment => self.read_environment(binding)?,
                ParameterBindingSource::SecretFile => self.read_secret_file(binding).await?,
            };
            let value = decode_binding_value(&bytes, binding.value_type)
                .map_err(|_| anyhow::anyhow!("parameter binding value is invalid"))?;
            values.insert(parameter.clone(), value);
        }
        let mut resolved = assignment.clone();
        resolved.snapshot = resolve_parameter_bindings(&assignment.snapshot, &values)
            .map_err(|_| anyhow::anyhow!("resolved parameter bindings failed validation"))?;
        Ok(resolved)
    }

    pub async fn available_bindings(&self) -> Result<AvailableBindings> {
        let mut environment = self
            .allowed_environment
            .iter()
            .filter(|name| {
                std::env::var_os(name)
                    .and_then(|value| os_string_bytes(value, self.max_bytes).ok())
                    .is_some()
            })
            .cloned()
            .collect::<Vec<_>>();
        environment.sort();

        let mut secret_files = HashSet::new();
        for root in &self.secret_roots {
            let mut entries = tokio::fs::read_dir(root)
                .await
                .map_err(|_| anyhow::anyhow!("a configured secret binding root is unreadable"))?;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|_| anyhow::anyhow!("a configured secret binding root is unreadable"))?
            {
                let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                    continue;
                };
                if !is_safe_logical_name(&name) {
                    continue;
                }
                let Ok(canonical) = tokio::fs::canonicalize(entry.path()).await else {
                    continue;
                };
                if !canonical.starts_with(root) {
                    continue;
                }
                let Ok(metadata) = tokio::fs::metadata(&canonical).await else {
                    continue;
                };
                if metadata.is_file() && metadata.len() <= self.max_bytes as u64 {
                    secret_files.insert(name);
                }
            }
        }
        let mut secret_files = secret_files.into_iter().collect::<Vec<_>>();
        secret_files.sort();
        Ok(AvailableBindings {
            environment,
            secret_files,
        })
    }

    fn read_environment(&self, binding: &ParameterBinding) -> Result<Vec<u8>> {
        if !self.allowed_environment.contains(&binding.name) {
            bail!("environment parameter binding is not allowed on this agent");
        }
        let value = std::env::var_os(&binding.name)
            .ok_or_else(|| anyhow::anyhow!("environment parameter binding is unavailable"))?;
        os_string_bytes(value, self.max_bytes)
    }

    async fn read_secret_file(&self, binding: &ParameterBinding) -> Result<Vec<u8>> {
        if self.secret_roots.is_empty() {
            bail!("secret-file parameter bindings are not configured on this agent");
        }
        for root in &self.secret_roots {
            let candidate = root.join(&binding.name);
            let canonical = match tokio::fs::canonicalize(&candidate).await {
                Ok(path) => path,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => bail!("secret-file parameter binding is unreadable"),
            };
            if !canonical.starts_with(root) {
                bail!("secret-file parameter binding escapes its configured root");
            }
            let metadata = tokio::fs::metadata(&canonical)
                .await
                .map_err(|_| anyhow::anyhow!("secret-file parameter binding is unreadable"))?;
            if !metadata.is_file() {
                bail!("secret-file parameter binding is not a regular file");
            }
            if metadata.len() > self.max_bytes as u64 {
                bail!("parameter binding exceeds the configured size limit");
            }
            let bytes = tokio::fs::read(&canonical)
                .await
                .map_err(|_| anyhow::anyhow!("secret-file parameter binding is unreadable"))?;
            ensure_size(&bytes, self.max_bytes)?;
            return Ok(bytes);
        }
        bail!("secret-file parameter binding is unavailable")
    }
}

fn is_portable_environment_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    (1..=128).contains(&bytes.len())
        && bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
}

fn is_safe_logical_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    (1..=128).contains(&bytes.len())
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'.' | b'_' | b'-'))
        && !matches!(name, "." | "..")
}

#[cfg(unix)]
fn os_string_bytes(value: OsString, max_bytes: usize) -> Result<Vec<u8>> {
    use std::os::unix::ffi::OsStringExt;
    let bytes = value.into_vec();
    ensure_size(&bytes, max_bytes)?;
    Ok(bytes)
}

#[cfg(not(unix))]
fn os_string_bytes(value: OsString, max_bytes: usize) -> Result<Vec<u8>> {
    let value = value
        .into_string()
        .map_err(|_| anyhow::anyhow!("environment parameter binding is not valid Unicode"))?;
    let bytes = value.into_bytes();
    ensure_size(&bytes, max_bytes)?;
    Ok(bytes)
}

fn ensure_size(bytes: &[u8], max_bytes: usize) -> Result<()> {
    if bytes.len() > max_bytes {
        bail!("parameter binding exceeds the configured size limit");
    }
    Ok(())
}

fn decode_binding_value(bytes: &[u8], value_type: ParameterBindingValueType) -> Result<Value> {
    match value_type {
        ParameterBindingValueType::String => Ok(Value::String(
            String::from_utf8(bytes.to_vec())
                .map_err(|_| anyhow::anyhow!("binding is not valid UTF-8"))?,
        )),
        ParameterBindingValueType::Integer => {
            let value = utf8_trimmed(bytes)?
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("binding is not an integer"))?;
            Ok(Value::Number(value.into()))
        }
        ParameterBindingValueType::Number => {
            let value: Value = serde_json::from_slice(bytes)
                .map_err(|_| anyhow::anyhow!("binding is not a number"))?;
            if !value.is_number() {
                bail!("binding is not a number");
            }
            Ok(value)
        }
        ParameterBindingValueType::Boolean => match utf8_trimmed(bytes)? {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => bail!("binding is not a boolean"),
        },
        ParameterBindingValueType::Json => {
            serde_json::from_slice(bytes).map_err(|_| anyhow::anyhow!("binding is not valid JSON"))
        }
    }
}

fn utf8_trimmed(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes)
        .map(str::trim)
        .map_err(|_| anyhow::anyhow!("binding is not valid UTF-8"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use scheduler_core::{
        CommandSpec, ExecutionPolicy, ExecutionSnapshot, ExecutorSpec, LateBindingSnapshot,
        ParameterBindingSource,
    };
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;

    fn assignment(binding: ParameterBinding) -> ExecutionAssignment {
        let template = ExecutorSpec::Command(CommandSpec {
            program: "runner".into(),
            args: Vec::new(),
            env: BTreeMap::from([("BOUND_VALUE".into(), "{{params.credential}}".into())]),
            working_directory: None,
        });
        ExecutionAssignment {
            schedule_id: Uuid::new_v4(),
            run_id: Uuid::new_v4(),
            attempt_id: Uuid::new_v4(),
            attempt_number: 1,
            lease_token: "lease".into(),
            lease_seconds: 60,
            snapshot: ExecutionSnapshot {
                executor: template.clone(),
                policy: ExecutionPolicy::default(),
                required_labels: BTreeMap::new(),
                blueprint_digest: "test-blueprint".into(),
                parameters_digest: "nonsecret".into(),
                late_bindings: Some(LateBindingSnapshot {
                    executor_template: template,
                    parameters_schema: serde_json::json!({
                        "type": "object",
                        "required": ["credential"],
                        "properties": {"credential": {"type": "string"}}
                    }),
                    parameters: serde_json::json!({}),
                    bindings: BTreeMap::from([("credential".into(), binding)]),
                }),
            },
        }
    }

    fn secret_binding(name: &str) -> ParameterBinding {
        ParameterBinding {
            source: ParameterBindingSource::SecretFile,
            name: name.into(),
            value_type: ParameterBindingValueType::String,
            sensitive: true,
        }
    }

    #[test]
    fn decodes_supported_types_strictly() {
        assert_eq!(
            decode_binding_value(b"text\n", ParameterBindingValueType::String).unwrap(),
            Value::String("text\n".into())
        );
        assert_eq!(
            decode_binding_value(b" -42 \n", ParameterBindingValueType::Integer).unwrap(),
            serde_json::json!(-42)
        );
        assert_eq!(
            decode_binding_value(b"1.25", ParameterBindingValueType::Number).unwrap(),
            serde_json::json!(1.25)
        );
        assert_eq!(
            decode_binding_value(b"9007199254740993", ParameterBindingValueType::Number).unwrap(),
            serde_json::json!(9_007_199_254_740_993_u64)
        );
        assert_eq!(
            decode_binding_value(b" true \n", ParameterBindingValueType::Boolean).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            decode_binding_value(b"{\"nested\":true}", ParameterBindingValueType::Json).unwrap(),
            serde_json::json!({"nested": true})
        );
        assert!(decode_binding_value(b"TRUE", ParameterBindingValueType::Boolean).is_err());
        assert!(decode_binding_value(b"NaN", ParameterBindingValueType::Number).is_err());
    }

    #[tokio::test]
    async fn resolves_secret_file_without_mutating_the_persistable_assignment() {
        const SECRET: &str = "unique-secret-value-never-persist";
        let root = tempdir().unwrap();
        std::fs::write(root.path().join("task-token"), SECRET).unwrap();
        let resolver = ParameterBindingResolver::new(&[], &[root.path().to_path_buf()], 1024)
            .expect("resolver");
        let original = assignment(secret_binding("task-token"));
        let persisted = serde_json::to_string(&original).unwrap();
        let resolved = resolver
            .resolve_assignment(&original)
            .await
            .expect("resolve");

        assert!(!persisted.contains(SECRET));
        assert!(original.snapshot.late_bindings.is_some());
        assert!(resolved.snapshot.late_bindings.is_none());
        let ExecutorSpec::Command(command) = resolved.snapshot.executor else {
            panic!("command");
        };
        assert_eq!(command.env["BOUND_VALUE"], SECRET);
    }

    #[tokio::test]
    async fn advertises_names_without_returning_values() {
        const SECRET: &str = "advertisement-must-not-contain-this";
        let root = tempdir().unwrap();
        std::fs::write(root.path().join("task-token"), SECRET).unwrap();
        std::fs::create_dir(root.path().join("not-a-secret-file")).unwrap();
        let resolver = ParameterBindingResolver::new(
            &["PATH".into(), "VARIABLE_THAT_IS_NOT_PRESENT".into()],
            &[root.path().to_path_buf()],
            64 * 1024,
        )
        .unwrap();
        let available = resolver.available_bindings().await.unwrap();
        assert_eq!(available.environment, ["PATH"]);
        assert_eq!(available.secret_files, ["task-token"]);
        assert!(!format!("{available:?}").contains(SECRET));
    }

    #[tokio::test]
    async fn size_and_decode_errors_do_not_expose_values_or_paths() {
        const SECRET: &str = "secret-sentinel-that-must-not-leak";
        let root = tempdir().unwrap();
        std::fs::write(root.path().join("bad-number"), SECRET).unwrap();
        let resolver = ParameterBindingResolver::new(&[], &[root.path().to_path_buf()], 1024)
            .expect("resolver");
        let mut binding = secret_binding("bad-number");
        binding.value_type = ParameterBindingValueType::Integer;
        let error = resolver
            .resolve_assignment(&assignment(binding))
            .await
            .expect_err("invalid number")
            .to_string();
        assert!(!error.contains(SECRET));
        assert!(!error.contains("bad-number"));
        assert!(!error.contains(&root.path().display().to_string()));

        let small = ParameterBindingResolver::new(&[], &[root.path().to_path_buf()], 4).unwrap();
        let error = small
            .resolve_assignment(&assignment(secret_binding("bad-number")))
            .await
            .expect_err("oversize")
            .to_string();
        assert_eq!(error, "parameter binding exceeds the configured size limit");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_cannot_escape_a_secret_root() {
        use std::os::unix::fs::symlink;

        let root = tempdir().unwrap();
        let outside = tempdir().unwrap();
        std::fs::write(outside.path().join("token"), "secret").unwrap();
        symlink(
            outside.path().join("token"),
            root.path().join("linked-token"),
        )
        .unwrap();
        let resolver = ParameterBindingResolver::new(&[], &[root.path().to_path_buf()], 1024)
            .expect("resolver");
        let error = resolver
            .resolve_assignment(&assignment(secret_binding("linked-token")))
            .await
            .expect_err("escape")
            .to_string();
        assert_eq!(
            error,
            "secret-file parameter binding escapes its configured root"
        );
    }

    #[tokio::test]
    async fn environment_sources_require_an_explicit_allowlist() {
        let binding = ParameterBinding {
            source: ParameterBindingSource::Environment,
            name: "PATH".into(),
            value_type: ParameterBindingValueType::String,
            sensitive: true,
        };
        let resolver = ParameterBindingResolver::new(&[], &[], 1024).unwrap();
        let error = resolver
            .resolve_assignment(&assignment(binding.clone()))
            .await
            .expect_err("not allowed")
            .to_string();
        assert_eq!(
            error,
            "environment parameter binding is not allowed on this agent"
        );

        let resolver = ParameterBindingResolver::new(&["PATH".into()], &[], 64 * 1024).unwrap();
        resolver
            .resolve_assignment(&assignment(binding))
            .await
            .expect("PATH is present and allowlisted");
    }

    #[test]
    fn rejects_relative_or_excessive_bootstrap_configuration() {
        assert!(ParameterBindingResolver::new(&[], &[PathBuf::from("relative")], 1024).is_err());
        assert!(ParameterBindingResolver::new(&[], &[], 0).is_err());
        assert!(ParameterBindingResolver::new(&[], &[], MAX_CONFIGURED_BINDING_BYTES + 1).is_err());
        assert!(ParameterBindingResolver::new(&["NOT-PORTABLE".into()], &[], 1024).is_err());
    }
}
