use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{Blueprint, ExecutionSnapshot, ExecutorSpec, ResolvedScheduleSnapshot};

pub fn parse_blueprint(bytes: &[u8], media_type: Option<&str>) -> Result<Blueprint> {
    let defaults = crate::ExecutionPolicy::default();
    parse_blueprint_with_defaults(
        bytes,
        media_type,
        defaults.max_attempts,
        defaults.timeout_seconds,
    )
}

pub fn parse_blueprint_with_defaults(
    bytes: &[u8],
    media_type: Option<&str>,
    default_max_attempts: u32,
    default_timeout_seconds: u64,
) -> Result<Blueprint> {
    let document: Value = if media_type.is_some_and(|value| value.contains("yaml")) {
        serde_yaml::from_slice(bytes).context("invalid YAML blueprint")?
    } else {
        serde_json::from_slice(bytes)
            .or_else(|_| serde_yaml::from_slice(bytes))
            .context("invalid blueprint document")?
    };
    let uses_default_attempts = document.pointer("/policy/max_attempts").is_none();
    let uses_default_timeout = document.pointer("/policy/timeout_seconds").is_none();
    let mut blueprint: Blueprint =
        serde_json::from_value(document).context("invalid blueprint document")?;
    if uses_default_attempts {
        blueprint.policy.max_attempts = default_max_attempts;
    }
    if uses_default_timeout {
        blueprint.policy.timeout_seconds = default_timeout_seconds;
    }
    validate_blueprint(&blueprint)?;
    Ok(blueprint)
}

pub fn validate_blueprint(blueprint: &Blueprint) -> Result<()> {
    if blueprint.api_version != "scheduler/v1" {
        bail!("unsupported blueprint api_version; expected scheduler/v1");
    }
    if blueprint.policy.max_attempts == 0 {
        bail!("max_attempts must be at least one");
    }
    if blueprint.policy.timeout_seconds == 0 {
        bail!("timeout_seconds must be at least one");
    }
    match &blueprint.executor {
        ExecutorSpec::Command(command) if command.program.trim().is_empty() => {
            bail!("command program cannot be empty")
        }
        ExecutorSpec::ExcelMacro(excel) => {
            if excel.workbook_path.trim().is_empty() || excel.macro_name.trim().is_empty() {
                bail!("Excel workbook path and macro name are required");
            }
            if excel.args.len() > 30 {
                bail!("Excel Application.Run supports at most 30 arguments");
            }
            for argument in &excel.args {
                if !matches!(
                    argument,
                    Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
                ) {
                    bail!(
                        "Excel arguments must be JSON strings, numbers, booleans, null, or parameter expressions"
                    );
                }
            }
        }
        _ => {}
    }
    jsonschema::validator_for(&blueprint.parameters_schema).context("invalid parameters_schema")?;
    Ok(())
}

pub fn validate_parameters(schema: &Value, parameters: &Value) -> Result<()> {
    let validator = jsonschema::validator_for(schema).context("invalid parameters schema")?;
    let errors = validator
        .iter_errors(parameters)
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        bail!("parameters failed validation: {}", errors.join("; "));
    }
    Ok(())
}

pub fn merge_parameters(base: &Value, overrides: &Value) -> Result<Value> {
    if !base.is_object() || !overrides.is_object() {
        bail!("base parameters and overrides must be JSON objects");
    }
    let mut merged = base.clone();
    deep_merge(&mut merged, overrides);
    Ok(merged)
}

fn deep_merge(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => {
            for (key, value) in patch {
                if let Some(existing) = target.get_mut(key) {
                    deep_merge(existing, value);
                } else {
                    target.insert(key.clone(), value.clone());
                }
            }
        }
        (target, patch) => *target = patch.clone(),
    }
}

pub fn resolve_snapshot(
    resolved: &ResolvedScheduleSnapshot,
    parameters: &Value,
    schedule_labels: &BTreeMap<String, String>,
) -> Result<ExecutionSnapshot> {
    validate_parameters(&resolved.blueprint.parameters_schema, parameters)?;
    let executor = render_executor(&resolved.blueprint.executor, parameters)?;
    let mut required_labels = resolved.blueprint.required_labels.clone();
    required_labels.extend(schedule_labels.clone());
    let parameters_digest = hex::encode(Sha256::digest(serde_json::to_vec(parameters)?));
    Ok(ExecutionSnapshot {
        executor,
        policy: resolved.blueprint.policy.clone(),
        required_labels,
        parameters_digest,
    })
}

fn render_executor(executor: &ExecutorSpec, parameters: &Value) -> Result<ExecutorSpec> {
    match executor {
        ExecutorSpec::Command(command) => Ok(ExecutorSpec::Command(crate::CommandSpec {
            program: command.program.clone(),
            args: command
                .args
                .iter()
                .map(|value| render_string(value, parameters))
                .collect::<Result<_>>()?,
            env: command
                .env
                .iter()
                .map(|(key, value)| Ok((key.clone(), render_string(value, parameters)?)))
                .collect::<Result<_>>()?,
            working_directory: command
                .working_directory
                .as_deref()
                .map(|value| render_string(value, parameters))
                .transpose()?,
        })),
        ExecutorSpec::ExcelMacro(excel) => {
            let args = excel
                .args
                .iter()
                .map(|value| render_value(value, parameters))
                .collect::<Result<Vec<_>>>()?;
            if args.iter().any(|argument| {
                !matches!(
                    argument,
                    Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
                )
            }) {
                bail!("resolved Excel arguments must be JSON scalar values");
            }
            Ok(ExecutorSpec::ExcelMacro(crate::ExcelMacroSpec {
                workbook_path: render_string(&excel.workbook_path, parameters)?,
                macro_name: render_string(&excel.macro_name, parameters)?,
                args,
                read_only: excel.read_only,
                save_changes: excel.save_changes,
                visible: excel.visible,
            }))
        }
    }
}

fn render_value(value: &Value, parameters: &Value) -> Result<Value> {
    if let Value::String(template) = value {
        if let Some(path) = exact_placeholder(template) {
            return lookup(parameters, path).cloned();
        }
        return Ok(Value::String(render_string(template, parameters)?));
    }
    Ok(value.clone())
}

fn render_string(template: &str, parameters: &Value) -> Result<String> {
    let mut output = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{params.") {
        output.push_str(&rest[..start]);
        let expression = &rest[start + 9..];
        let Some(end) = expression.find("}}") else {
            bail!("unterminated parameter placeholder");
        };
        let path = expression[..end].trim();
        let value = lookup(parameters, path)?;
        match value {
            Value::String(value) => output.push_str(value),
            Value::Number(value) => output.push_str(&value.to_string()),
            Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            Value::Null => {}
            _ => bail!("embedded parameter {path} must be a scalar"),
        }
        rest = &expression[end + 2..];
    }
    output.push_str(rest);
    Ok(output)
}

fn exact_placeholder(template: &str) -> Option<&str> {
    template
        .strip_prefix("{{params.")
        .and_then(|value| value.strip_suffix("}}"))
        .map(str::trim)
}

fn lookup<'a>(parameters: &'a Value, path: &str) -> Result<&'a Value> {
    let mut value = parameters;
    for component in path.split('.') {
        value = value
            .get(component)
            .with_context(|| format!("missing parameter {path}"))?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommandSpec, ExcelMacroSpec, ExecutionPolicy};

    #[test]
    fn applies_runtime_policy_defaults_only_when_blueprint_fields_are_missing() {
        let missing = br#"
api_version: scheduler/v1
executor:
  kind: command
  program: runner
policy:
  initial_backoff_seconds: 2
"#;
        let blueprint = parse_blueprint_with_defaults(missing, Some("application/yaml"), 7, 42)
            .expect("blueprint");
        assert_eq!(blueprint.policy.max_attempts, 7);
        assert_eq!(blueprint.policy.timeout_seconds, 42);

        let explicit = br#"
api_version: scheduler/v1
executor:
  kind: command
  program: runner
policy:
  max_attempts: 2
  timeout_seconds: 9
"#;
        let blueprint = parse_blueprint_with_defaults(explicit, Some("application/yaml"), 7, 42)
            .expect("blueprint");
        assert_eq!(blueprint.policy.max_attempts, 2);
        assert_eq!(blueprint.policy.timeout_seconds, 9);
    }

    #[test]
    fn renders_structured_parameters_without_a_shell() {
        let resolved = ResolvedScheduleSnapshot {
            blueprint: Blueprint {
                api_version: "scheduler/v1".into(),
                executor: ExecutorSpec::Command(CommandSpec {
                    program: "runner".into(),
                    args: vec!["--name={{params.user.name}}".into()],
                    env: BTreeMap::new(),
                    working_directory: None,
                }),
                parameters_schema: serde_json::json!({"type": "object"}),
                required_labels: BTreeMap::new(),
                policy: ExecutionPolicy::default(),
            },
            base_parameters: serde_json::json!({}),
            blueprint_source_version: None,
            parameters_source_version: None,
        };
        let snapshot = resolve_snapshot(
            &resolved,
            &serde_json::json!({"user": {"name": "Ada; rm -rf /"}}),
            &BTreeMap::new(),
        )
        .expect("render");
        let ExecutorSpec::Command(command) = snapshot.executor else {
            panic!("wrong executor");
        };
        assert_eq!(command.args, ["--name=Ada; rm -rf /"]);
    }

    #[test]
    fn rejects_non_scalar_resolved_excel_arguments() {
        let resolved = ResolvedScheduleSnapshot {
            blueprint: Blueprint {
                api_version: "scheduler/v1".into(),
                executor: ExecutorSpec::ExcelMacro(ExcelMacroSpec {
                    workbook_path: "C:\\Tasks\\book.xlsm".into(),
                    macro_name: "Module.Run".into(),
                    args: vec![Value::String("{{params.payload}}".into())],
                    read_only: true,
                    save_changes: false,
                    visible: false,
                }),
                parameters_schema: serde_json::json!({"type": "object"}),
                required_labels: BTreeMap::new(),
                policy: ExecutionPolicy::default(),
            },
            base_parameters: serde_json::json!({}),
            blueprint_source_version: None,
            parameters_source_version: None,
        };

        let error = resolve_snapshot(
            &resolved,
            &serde_json::json!({"payload": {"nested": true}}),
            &BTreeMap::new(),
        )
        .expect_err("object arguments are not COM scalar variants");
        assert!(error.to_string().contains("scalar"));
    }
}
