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
            validate_excel_spec(excel, false)?;
        }
        _ => {}
    }
    jsonschema::validator_for(&blueprint.parameters_schema).context("invalid parameters_schema")?;
    Ok(())
}

fn validate_excel_spec(excel: &crate::ExcelMacroSpec, rendered: bool) -> Result<()> {
    if excel.workbook_path.trim().is_empty() || excel.macro_name.trim().is_empty() {
        bail!("Excel workbook path and macro name are required");
    }
    if let Some(module_name) = &excel.module_name {
        if module_name.trim().is_empty() {
            bail!("Excel module_name cannot be blank");
        }
        if (rendered || !module_name.contains("{{params.")) && module_name.contains(['.', '!']) {
            bail!("Excel module_name must be an unqualified standard module name");
        }
        if (rendered || !excel.macro_name.contains("{{params."))
            && excel.macro_name.contains(['.', '!'])
        {
            bail!("Excel macro_name must be unqualified when module_name is set");
        }
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
    Ok(())
}

pub fn validate_parameters(schema: &Value, parameters: &Value) -> Result<()> {
    let validator = jsonschema::validator_for(schema).context("invalid parameters schema")?;
    let errors = validator
        .iter_errors(parameters)
        .map(|error| {
            let schema_path = match error.schema_path.as_str() {
                "" => "<root>",
                path => path,
            };
            let keyword = error
                .schema_path
                .as_str()
                .rsplit('/')
                .find(|component| !component.is_empty())
                .unwrap_or("unknown");
            // `instance_path` is deliberately excluded. Object keys are runtime
            // parameter data and can themselves contain credentials or other
            // values that must never reach APIs, audit logs, or telemetry.
            format!("`{keyword}` validation failed (schema path {schema_path})")
        })
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
            let rendered = crate::ExcelMacroSpec {
                workbook_path: render_string(&excel.workbook_path, parameters)?,
                module_name: excel
                    .module_name
                    .as_deref()
                    .map(|value| render_string(value, parameters))
                    .transpose()?,
                macro_name: render_string(&excel.macro_name, parameters)?,
                args,
                read_only: excel.read_only,
                save_changes: excel.save_changes,
                visible: excel.visible,
            };
            validate_excel_spec(&rendered, true)?;
            Ok(ExecutorSpec::ExcelMacro(rendered))
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
                    module_name: None,
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

    #[test]
    fn process_id_example_preserves_all_vba_arguments_and_long_bounds() {
        let blueprint = parse_blueprint(
            include_bytes!("../../../examples/blueprints/process-id.yaml"),
            Some("application/yaml"),
        )
        .expect("processID blueprint");
        let parameters: Value = serde_json::from_slice(include_bytes!(
            "../../../examples/parameters/process-id.json"
        ))
        .expect("processID parameters");
        let resolved = ResolvedScheduleSnapshot {
            blueprint,
            base_parameters: parameters.clone(),
            blueprint_source_version: None,
            parameters_source_version: None,
        };

        let snapshot = resolve_snapshot(&resolved, &parameters, &BTreeMap::new())
            .expect("resolve processID example");
        let ExecutorSpec::ExcelMacro(excel) = snapshot.executor else {
            panic!("wrong executor");
        };
        assert_eq!(excel.module_name.as_deref(), Some("ProcessModule"));
        assert_eq!(excel.macro_name, "processID");
        assert_eq!(excel.args.len(), 17);
        assert_eq!(
            excel.args,
            vec![
                serde_json::json!(2_147_483_647_i64),
                serde_json::json!("Monthly Processing.xlsm"),
                serde_json::json!("operations@example.com;finance@example.com"),
                serde_json::json!("CURRENT_AND_ARCHIVED"),
                serde_json::json!("Ada Lovelace"),
                serde_json::json!("Processing result – July"),
                serde_json::json!("Line 1\nLine 2 with 'quotes' and {{literal braces}}"),
                serde_json::json!(true),
                serde_json::json!(false),
                serde_json::json!("SELECT * FROM CurrentData WHERE Status = 'Ready'"),
                serde_json::json!(""),
                serde_json::json!(""),
                serde_json::json!(""),
                serde_json::json!(""),
                serde_json::json!(false),
                serde_json::json!("example-user"),
                serde_json::json!("example-password"),
            ]
        );
        assert!(excel.args[0].is_i64());
        assert!(excel.args[1..7].iter().all(Value::is_string));
        assert!(excel.args[7..9].iter().all(Value::is_boolean));
        assert!(excel.args[9..14].iter().all(Value::is_string));
        assert!(excel.args[14].is_boolean());
        assert!(excel.args[15..17].iter().all(Value::is_string));

        for boundary in [-2_147_483_648_i64, 2_147_483_647_i64] {
            let mut boundary_parameters = parameters.clone();
            boundary_parameters["id"] = serde_json::json!(boundary);
            resolve_snapshot(&resolved, &boundary_parameters, &BTreeMap::new())
                .expect("VBA Long boundary should validate");
        }
        for outside in [-2_147_483_649_i64, 2_147_483_648_i64] {
            let mut invalid_parameters = parameters.clone();
            invalid_parameters["id"] = serde_json::json!(outside);
            let error = resolve_snapshot(&resolved, &invalid_parameters, &BTreeMap::new())
                .expect_err("value outside VBA Long range must be rejected");
            assert!(error.to_string().contains("parameters failed validation"));
        }
    }

    #[test]
    fn excel_application_run_accepts_thirty_macro_arguments_but_not_thirty_one() {
        let mut blueprint = Blueprint {
            api_version: "scheduler/v1".into(),
            executor: ExecutorSpec::ExcelMacro(ExcelMacroSpec {
                workbook_path: "C:\\Tasks\\book.xlsm".into(),
                module_name: None,
                macro_name: "Module.Run".into(),
                args: vec![Value::Null; 30],
                read_only: true,
                save_changes: false,
                visible: false,
            }),
            parameters_schema: serde_json::json!({"type": "object"}),
            required_labels: BTreeMap::new(),
            policy: ExecutionPolicy::default(),
        };
        validate_blueprint(&blueprint).expect("Application.Run supports 30 arguments");

        let ExecutorSpec::ExcelMacro(excel) = &mut blueprint.executor else {
            unreachable!();
        };
        excel.args.push(Value::Null);
        let error = validate_blueprint(&blueprint).expect_err("31 arguments must be rejected");
        assert!(error.to_string().contains("at most 30"));
    }

    #[test]
    fn renders_separate_excel_module_and_rejects_double_qualification() {
        let blueprint = parse_blueprint(
            br#"api_version: scheduler/v1
executor:
  kind: excel_macro
  workbook_path: 'C:\Tasks\book.xlsm'
  module_name: '{{params.module}}'
  macro_name: '{{params.macro}}'
parameters_schema:
  type: object
"#,
            Some("application/yaml"),
        )
        .expect("templated module blueprint must pass pre-render validation");
        let resolved = ResolvedScheduleSnapshot {
            blueprint,
            base_parameters: serde_json::json!({}),
            blueprint_source_version: None,
            parameters_source_version: None,
        };

        let snapshot = resolve_snapshot(
            &resolved,
            &serde_json::json!({"module": "PublicModule", "macro": "RunTask"}),
            &BTreeMap::new(),
        )
        .expect("separate module and macro render");
        let ExecutorSpec::ExcelMacro(excel) = snapshot.executor else {
            panic!("wrong executor");
        };
        assert_eq!(excel.module_name.as_deref(), Some("PublicModule"));
        assert_eq!(excel.macro_name, "RunTask");

        let error = resolve_snapshot(
            &resolved,
            &serde_json::json!({"module": "PublicModule", "macro": "Other.RunTask"}),
            &BTreeMap::new(),
        )
        .expect_err("a separate module cannot be combined with a qualified macro");
        assert!(error.to_string().contains("must be unqualified"));

        let error = resolve_snapshot(
            &resolved,
            &serde_json::json!({"module": "Injected.Module", "macro": "RunTask"}),
            &BTreeMap::new(),
        )
        .expect_err("a rendered module must remain unqualified");
        assert!(error.to_string().contains("standard module name"));
    }

    #[test]
    fn legacy_qualified_excel_macro_without_module_name_remains_valid() {
        let blueprint = parse_blueprint(
            br#"{
                "api_version": "scheduler/v1",
                "executor": {
                    "kind": "excel_macro",
                    "workbook_path": "C:\\\\Tasks\\\\Legacy.xlsm",
                    "macro_name": "LegacyModule.Run"
                }
            }"#,
            Some("application/json"),
        )
        .expect("legacy blueprint");
        let ExecutorSpec::ExcelMacro(excel) = blueprint.executor else {
            panic!("wrong executor");
        };
        assert!(excel.module_name.is_none());
        assert_eq!(excel.macro_name, "LegacyModule.Run");
    }

    #[test]
    fn parameter_validation_errors_are_actionable_without_exposing_values() {
        const SECRET: &str = "UNIQUE-SECRET-SENTINEL-7f19099d-do-not-log";
        let schema = serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["credential"],
            "properties": {
                "credential": {
                    "type": "string",
                    "maxLength": 8
                }
            }
        });
        let mut parameters = serde_json::json!({"credential": SECRET});
        parameters
            .as_object_mut()
            .expect("object parameters")
            .insert(SECRET.into(), serde_json::json!("unexpected dynamic key"));

        let validation_error = validate_parameters(&schema, &parameters)
            .expect_err("oversized credential must be rejected")
            .to_string();
        assert!(!validation_error.contains(SECRET));
        assert!(validation_error.contains("`maxLength`"));
        assert!(validation_error.contains("schema path /properties/credential/maxLength"));
        assert!(validation_error.contains("`additionalProperties`"));
        assert!(validation_error.contains("schema path /additionalProperties"));

        let resolved = ResolvedScheduleSnapshot {
            blueprint: Blueprint {
                api_version: "scheduler/v1".into(),
                executor: ExecutorSpec::Command(CommandSpec {
                    program: "runner".into(),
                    args: vec!["{{params.credential}}".into()],
                    env: BTreeMap::new(),
                    working_directory: None,
                }),
                parameters_schema: schema,
                required_labels: BTreeMap::new(),
                policy: ExecutionPolicy::default(),
            },
            base_parameters: parameters.clone(),
            blueprint_source_version: None,
            parameters_source_version: None,
        };
        let resolution_error = resolve_snapshot(&resolved, &parameters, &BTreeMap::new())
            .expect_err("snapshot resolution must retain safe validation diagnostics")
            .to_string();
        assert!(!resolution_error.contains(SECRET));
        assert!(resolution_error.contains("`maxLength`"));
        assert!(resolution_error.contains("schema path /properties/credential/maxLength"));
        assert!(resolution_error.contains("`additionalProperties`"));
        assert!(resolution_error.contains("schema path /additionalProperties"));
    }
}
