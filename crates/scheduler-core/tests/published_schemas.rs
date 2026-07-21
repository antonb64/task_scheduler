use std::{fs, path::Path};

use scheduler_core::{AdapterRegistry, ConnectorConfig, ScheduleSpec};
use serde_json::Value;

fn repository_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("scheduler-core lives under <repository>/crates")
}

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(
        &fs::read(path).unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display())),
    )
    .unwrap_or_else(|error| panic!("invalid JSON in {}: {error}", path.display()))
}

fn read_yaml_or_json(path: &Path) -> Value {
    let bytes =
        fs::read(path).unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("yaml" | "yml") => serde_yaml::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("invalid YAML in {}: {error}", path.display())),
        Some("json") => serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("invalid JSON in {}: {error}", path.display())),
        extension => panic!("unsupported example extension {extension:?}"),
    }
}

fn validator(schema_name: &str) -> jsonschema::Validator {
    let schema_path = repository_root().join("schemas").join(schema_name);
    let schema = read_json(&schema_path);
    jsonschema::meta::validate(&schema).unwrap_or_else(|error| {
        panic!(
            "{} is not valid JSON Schema: {error}",
            schema_path.display()
        )
    });
    jsonschema::validator_for(&schema)
        .unwrap_or_else(|error| panic!("cannot compile {}: {error}", schema_path.display()))
}

fn assert_valid(validator: &jsonschema::Validator, instance: &Value, label: &str) {
    let errors = validator
        .iter_errors(instance)
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    assert!(
        errors.is_empty(),
        "{label} failed schema validation: {errors:#?}"
    );
}

fn example_documents(directory: &str) -> Vec<std::path::PathBuf> {
    let mut paths = fs::read_dir(repository_root().join("examples").join(directory))
        .unwrap_or_else(|error| panic!("cannot read examples/{directory}: {error}"))
        .map(|entry| entry.expect("example directory entry").path())
        .filter(|path| {
            matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("json" | "yaml" | "yml")
            )
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

#[test]
fn every_published_schema_is_valid_draft_2020_12() {
    for schema in [
        "blueprint-v1.schema.json",
        "schedule-v1.schema.json",
        "connectors-v1.schema.json",
    ] {
        let _ = validator(schema);
    }
}

#[test]
fn every_example_blueprint_matches_the_published_schema_and_rust_model() {
    let validator = validator("blueprint-v1.schema.json");
    let examples = example_documents("blueprints");
    assert!(!examples.is_empty(), "expected blueprint examples");

    for path in examples {
        let instance = read_yaml_or_json(&path);
        assert_valid(&validator, &instance, &path.display().to_string());
        let bytes = fs::read(&path).expect("blueprint bytes");
        scheduler_core::blueprint::parse_blueprint(&bytes, Some("application/yaml"))
            .unwrap_or_else(|error| {
                panic!(
                    "{} does not match the Rust blueprint model: {error:#}",
                    path.display()
                )
            });
    }
}

#[test]
fn every_example_schedule_matches_the_published_schema_and_rust_model() {
    let validator = validator("schedule-v1.schema.json");
    let examples = example_documents("schedules");
    assert!(!examples.is_empty(), "expected schedule examples");

    for path in examples {
        let instance = read_yaml_or_json(&path);
        assert_valid(&validator, &instance, &path.display().to_string());
        let schedule: ScheduleSpec = serde_json::from_value(instance).unwrap_or_else(|error| {
            panic!(
                "{} does not match the Rust schedule model: {error}",
                path.display()
            )
        });
        assert!(
            !schedule.name.trim().is_empty(),
            "{} has an empty name",
            path.display()
        );
        url::Url::parse(&schedule.blueprint_ref.uri).unwrap_or_else(|error| {
            panic!("{} has an invalid blueprint URI: {error}", path.display())
        });
        url::Url::parse(&schedule.parameters_ref.uri).unwrap_or_else(|error| {
            panic!("{} has an invalid parameters URI: {error}", path.display())
        });
        if let Some(cron) = &schedule.cron {
            scheduler_core::schedule::parse_cron(cron).unwrap_or_else(|error| {
                panic!(
                    "{} has invalid cron configuration: {error:#}",
                    path.display()
                )
            });
        }
    }
}

#[test]
fn representative_connector_config_matches_schema_and_runtime() {
    let document = br#"
api_version: scheduler/connectors/v1
connectors:
  corporate-artifacts:
    base_url: https://connector.example.test/scheduler
    bearer_token_env: SCHEDULER_CORPORATE_CONNECTOR_TOKEN
    allowed_kinds: [blueprint, parameters]
    connect_timeout_seconds: 5
    timeout_seconds: 20
    allow_insecure_http: false
"#;
    let instance: Value = serde_yaml::from_slice(document).expect("representative YAML");
    assert_valid(
        &validator("connectors-v1.schema.json"),
        &instance,
        "representative connector config",
    );

    let config = ConnectorConfig::from_slice(document).expect("Rust connector config model");
    let mut runtime_config = config.clone();
    for connector in runtime_config.connectors.values_mut() {
        // Startup resolves this optional name from the real environment. The
        // published-schema test must not require or mutate operator secrets.
        connector.bearer_token_env = None;
    }
    let mut registry =
        AdapterRegistry::with_defaults(Vec::new(), Default::default()).expect("default adapters");
    registry
        .register_connectors(runtime_config)
        .expect("valid connector configuration");
}

#[test]
fn published_blueprint_schema_enforces_excel_scalar_and_argument_limits() {
    let validator = validator("blueprint-v1.schema.json");
    let mut instance = serde_json::json!({
        "api_version": "scheduler/v1",
        "executor": {
            "kind": "excel_macro",
            "workbook_path": "C:\\Tasks\\Book.xlsm",
            "module_name": "Module",
            "macro_name": "Run",
            "args": vec![Value::Null; 30]
        },
        "parameters_schema": {"type": "object"}
    });
    assert_valid(&validator, &instance, "30-argument Excel blueprint");

    instance["executor"]["args"] = Value::Array(vec![Value::Null; 31]);
    assert!(
        !validator.is_valid(&instance),
        "31 Excel arguments must be rejected"
    );

    instance["executor"]["args"] = serde_json::json!([{"not": "a scalar"}]);
    assert!(
        !validator.is_valid(&instance),
        "structured Excel arguments must be rejected"
    );

    instance["executor"]["args"] = Value::Array(Vec::new());
    instance["executor"]["module_name"] = Value::Null;
    instance["executor"]["macro_name"] = Value::String("Module.Run".into());
    assert_valid(
        &validator,
        &instance,
        "qualified macro without a separate module",
    );

    instance["executor"]["module_name"] = Value::String("   ".into());
    assert!(
        !validator.is_valid(&instance),
        "a blank Excel module must be rejected"
    );
    instance["executor"]["module_name"] = Value::String("Module.Qualified".into());
    assert!(
        !validator.is_valid(&instance),
        "a static Excel module name must be unqualified"
    );
    instance["executor"]["module_name"] = Value::String("{{params.module}}".into());
    instance["executor"]["macro_name"] = Value::String("{{params.macro}}".into());
    assert_valid(
        &validator,
        &instance,
        "templated separate Excel module and macro",
    );
    instance["executor"]["module_name"] = Value::String("Module".into());
    instance["executor"]["macro_name"] = Value::String("Other.Run".into());
    assert!(
        !validator.is_valid(&instance),
        "a static macro must be unqualified when module_name is present"
    );
    instance["executor"]["module_name"] = Value::from(123);
    assert!(
        !validator.is_valid(&instance),
        "a non-string Excel module must be rejected"
    );
}

#[test]
fn schemas_match_runtime_unknown_property_behavior() {
    let blueprint_validator = validator("blueprint-v1.schema.json");
    let blueprint = serde_json::json!({
        "api_version": "scheduler/v1",
        "executor": {"kind": "command", "program": "runner", "future_field": true},
        "future_field": {"accepted": true}
    });
    assert_valid(&blueprint_validator, &blueprint, "extended blueprint");
    serde_json::from_value::<scheduler_core::Blueprint>(blueprint)
        .expect("Rust blueprint model ignores unknown fields");

    let schedule_validator = validator("schedule-v1.schema.json");
    let schedule = serde_json::json!({
        "name": "extended schedule",
        "blueprint_ref": {"uri": "file:///tasks/blueprint.yaml", "future_field": true},
        "parameters_ref": {"uri": "file:///tasks/parameters.json"},
        "future_field": true
    });
    assert_valid(&schedule_validator, &schedule, "extended schedule");
    serde_json::from_value::<ScheduleSpec>(schedule)
        .expect("Rust schedule model ignores unknown fields");

    let connector_validator = validator("connectors-v1.schema.json");
    let connector = serde_json::json!({
        "api_version": "scheduler/connectors/v1",
        "future_field": true
    });
    assert!(!connector_validator.is_valid(&connector));
    assert!(serde_json::from_value::<ConnectorConfig>(connector).is_err());
}
