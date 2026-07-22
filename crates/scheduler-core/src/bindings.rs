use std::collections::BTreeMap;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ExecutionSnapshot, ExecutorSpec, validate_parameters};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParameterBindingSource {
    Environment,
    SecretFile,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParameterBindingValueType {
    #[default]
    String,
    Integer,
    Number,
    Boolean,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ParameterBinding {
    pub source: ParameterBindingSource,
    pub name: String,
    #[serde(default)]
    pub value_type: ParameterBindingValueType,
    #[serde(default = "default_sensitive")]
    pub sensitive: bool,
}

fn default_sensitive() -> bool {
    true
}

/// Information needed to render an executor after an agent has resolved its
/// local bindings. This structure deliberately contains binding references,
/// never resolved values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LateBindingSnapshot {
    pub executor_template: ExecutorSpec,
    pub parameters_schema: Value,
    pub parameters: Value,
    pub bindings: BTreeMap<String, ParameterBinding>,
}

pub fn validate_parameter_bindings(
    bindings: &BTreeMap<String, ParameterBinding>,
    executor: &ExecutorSpec,
) -> Result<()> {
    for (parameter, binding) in bindings {
        validate_parameter_name(parameter)?;
        validate_binding_source_name(binding)?;
        if binding.sensitive {
            validate_sensitive_binding_usage(parameter, executor)?;
        }
    }
    Ok(())
}

fn validate_parameter_name(name: &str) -> Result<()> {
    if !is_safe_identifier(name, 128) {
        bail!(
            "parameter binding names must be 1-128 ASCII letters, digits, underscores, or hyphens"
        );
    }
    if name.contains('.') {
        bail!("parameter bindings must name a top-level parameter");
    }
    Ok(())
}

fn validate_binding_source_name(binding: &ParameterBinding) -> Result<()> {
    match binding.source {
        ParameterBindingSource::Environment => {
            let bytes = binding.name.as_bytes();
            let valid = (1..=128).contains(&bytes.len())
                && bytes
                    .first()
                    .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
                && bytes
                    .iter()
                    .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_');
            if !valid {
                bail!("environment binding names must use portable environment-variable syntax");
            }
        }
        ParameterBindingSource::SecretFile => {
            if !is_safe_logical_name(&binding.name, 128)
                || matches!(binding.name.as_str(), "." | "..")
                || binding.name.contains(['/', '\\', ':'])
            {
                bail!("secret-file bindings must use a logical name, not a path");
            }
        }
    }
    Ok(())
}

fn is_safe_identifier(value: &str, max: usize) -> bool {
    let bytes = value.as_bytes();
    (1..=max).contains(&bytes.len())
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'-'))
}

fn is_safe_logical_name(value: &str, max: usize) -> bool {
    let bytes = value.as_bytes();
    (1..=max).contains(&bytes.len())
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'.' | b'_' | b'-'))
}

fn validate_sensitive_binding_usage(parameter: &str, executor: &ExecutorSpec) -> Result<()> {
    let prohibited = match executor {
        ExecutorSpec::Command(command) => {
            template_references(&command.program, parameter)
                || command
                    .args
                    .iter()
                    .any(|value| template_references(value, parameter))
                || command
                    .working_directory
                    .as_deref()
                    .is_some_and(|value| template_references(value, parameter))
        }
        ExecutorSpec::ExcelMacro(excel) => {
            template_references(&excel.workbook_path, parameter)
                || excel
                    .module_name
                    .as_deref()
                    .is_some_and(|value| template_references(value, parameter))
                || template_references(&excel.macro_name, parameter)
        }
    };
    if prohibited {
        bail!(
            "sensitive parameter bindings may only be used in command environment values or Excel arguments"
        );
    }
    Ok(())
}

fn template_references(template: &str, parameter: &str) -> bool {
    let mut remaining = template;
    while let Some(start) = remaining.find("{{params.") {
        let expression = &remaining[start + 9..];
        let Some(end) = expression.find("}}") else {
            return false;
        };
        let path = expression[..end].trim();
        if path == parameter
            || path
                .strip_prefix(parameter)
                .is_some_and(|suffix| suffix.starts_with('.'))
        {
            return true;
        }
        remaining = &expression[end + 2..];
    }
    false
}

pub fn resolve_parameter_bindings(
    snapshot: &ExecutionSnapshot,
    values: &BTreeMap<String, Value>,
) -> Result<ExecutionSnapshot> {
    let Some(late) = &snapshot.late_bindings else {
        if values.is_empty() {
            return Ok(snapshot.clone());
        }
        bail!("resolved binding values were supplied for an execution without bindings");
    };
    if values.len() != late.bindings.len()
        || late.bindings.keys().any(|name| !values.contains_key(name))
        || values.keys().any(|name| !late.bindings.contains_key(name))
    {
        bail!("resolved parameter bindings do not match the execution snapshot");
    }
    let mut parameters = late.parameters.clone();
    let object = parameters
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("bound execution parameters must be a JSON object"))?;
    for (name, value) in values {
        object.insert(name.clone(), value.clone());
    }
    validate_parameters(&late.parameters_schema, &parameters)?;
    let executor = crate::blueprint::render_executor(&late.executor_template, &parameters)?;
    let mut resolved = snapshot.clone();
    resolved.executor = executor;
    resolved.late_bindings = None;
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommandSpec, ExecutionPolicy};

    fn binding(source: ParameterBindingSource, name: &str) -> ParameterBinding {
        ParameterBinding {
            source,
            name: name.into(),
            value_type: ParameterBindingValueType::String,
            sensitive: true,
        }
    }

    #[test]
    fn rejects_paths_and_nonportable_environment_names() {
        let executor = ExecutorSpec::Command(CommandSpec {
            program: "runner".into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            working_directory: None,
        });
        for name in ["../token", "folder/token", "C:\\token", ".", ".."] {
            let bindings = BTreeMap::from([(
                "credential".into(),
                binding(ParameterBindingSource::SecretFile, name),
            )]);
            assert!(validate_parameter_bindings(&bindings, &executor).is_err());
        }
        let bindings = BTreeMap::from([(
            "credential".into(),
            binding(ParameterBindingSource::Environment, "NOT-PORTABLE"),
        )]);
        assert!(validate_parameter_bindings(&bindings, &executor).is_err());
    }

    #[test]
    fn sensitive_command_bindings_are_confined_to_environment_values() {
        let bindings = BTreeMap::from([(
            "token".into(),
            binding(ParameterBindingSource::Environment, "TASK_TOKEN"),
        )]);
        let mut command = CommandSpec {
            program: "runner".into(),
            args: Vec::new(),
            env: BTreeMap::from([("TASK_TOKEN".into(), "{{params.token}}".into())]),
            working_directory: None,
        };
        validate_parameter_bindings(&bindings, &ExecutorSpec::Command(command.clone()))
            .expect("environment use");
        command.args.push("--token={{params.token}}".into());
        let error = validate_parameter_bindings(&bindings, &ExecutorSpec::Command(command))
            .expect_err("argument leakage");
        assert!(!error.to_string().contains("TASK_TOKEN"));

        let nested_bindings = BTreeMap::from([(
            "credentials".into(),
            ParameterBinding {
                source: ParameterBindingSource::SecretFile,
                name: "credentials-json".into(),
                value_type: ParameterBindingValueType::Json,
                sensitive: true,
            },
        )]);
        let nested = ExecutorSpec::Command(CommandSpec {
            program: "runner".into(),
            args: vec!["--token={{params.credentials.token}}".into()],
            env: BTreeMap::new(),
            working_directory: None,
        });
        assert!(validate_parameter_bindings(&nested_bindings, &nested).is_err());
    }

    #[test]
    fn resolution_validates_and_removes_late_metadata() {
        let bindings = BTreeMap::from([(
            "token".into(),
            binding(ParameterBindingSource::Environment, "TASK_TOKEN"),
        )]);
        let template = ExecutorSpec::Command(CommandSpec {
            program: "runner".into(),
            args: Vec::new(),
            env: BTreeMap::from([("TASK_TOKEN".into(), "{{params.token}}".into())]),
            working_directory: None,
        });
        let snapshot = ExecutionSnapshot {
            executor: template.clone(),
            policy: ExecutionPolicy::default(),
            required_labels: BTreeMap::new(),
            blueprint_digest: "test-blueprint".into(),
            parameters_digest: "safe-digest".into(),
            parameters: Some(serde_json::json!({})),
            sensitive_parameter_paths: vec!["/token".into()],
            late_bindings: Some(LateBindingSnapshot {
                executor_template: template,
                parameters_schema: serde_json::json!({
                    "type": "object",
                    "required": ["token"],
                    "properties": {"token": {"type": "string", "minLength": 8}}
                }),
                parameters: serde_json::json!({}),
                bindings,
            }),
        };
        let secret = "do-not-persist-this-secret";
        let resolved = resolve_parameter_bindings(
            &snapshot,
            &BTreeMap::from([("token".into(), Value::String(secret.into()))]),
        )
        .expect("resolve");
        assert!(resolved.late_bindings.is_none());
        let ExecutorSpec::Command(command) = resolved.executor else {
            panic!("command");
        };
        assert_eq!(command.env["TASK_TOKEN"], secret);
        let persisted = serde_json::to_string(&snapshot).expect("snapshot JSON");
        assert!(!persisted.contains(secret));
    }
}
