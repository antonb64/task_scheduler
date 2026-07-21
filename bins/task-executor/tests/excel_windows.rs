#![cfg(windows)]

use std::{collections::BTreeMap, process::Stdio};

use scheduler_core::{
    ExcelMacroSpec, ExecutionAssignment, ExecutionOutcome, ExecutionPolicy, ExecutionSnapshot,
    ExecutorSpec, FailureCode, FailureOrigin,
};
use tokio::{io::AsyncWriteExt, process::Command};
use uuid::Uuid;

fn basic_arguments() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!("value"),
        serde_json::json!(42),
        serde_json::json!(true),
    ]
}

async fn run_macro(
    module_name: &str,
    macro_name: &str,
    args: Vec<serde_json::Value>,
) -> scheduler_core::ExecutionResult {
    let workbook = std::env::var("SCHEDULER_TEST_XLSM")
        .expect("SCHEDULER_TEST_XLSM must point to the licensed Excel test workbook");
    let assignment = ExecutionAssignment {
        schedule_id: Uuid::new_v4(),
        run_id: Uuid::new_v4(),
        attempt_id: Uuid::new_v4(),
        attempt_number: 1,
        lease_token: Uuid::new_v4().to_string(),
        lease_seconds: 60,
        snapshot: ExecutionSnapshot {
            executor: ExecutorSpec::ExcelMacro(ExcelMacroSpec {
                workbook_path: workbook,
                module_name: Some(module_name.into()),
                macro_name: macro_name.into(),
                args,
                read_only: true,
                save_changes: false,
                visible: false,
            }),
            policy: ExecutionPolicy {
                timeout_seconds: 60,
                ..ExecutionPolicy::default()
            },
            required_labels: BTreeMap::new(),
            blueprint_digest: "test-blueprint".into(),
            parameters_digest: "test".into(),
            late_bindings: None,
        },
    };
    let mut child = Command::new(env!("CARGO_BIN_EXE_task-executor"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("executor");
    let mut stdin = child.stdin.take().expect("stdin");
    stdin
        .write_all(serde_json::to_string(&assignment).expect("json").as_bytes())
        .await
        .expect("write");
    stdin
        .write_all(b"\n{\"keepalive\":true}\n")
        .await
        .expect("write");
    drop(stdin);
    let output = child.wait_with_output().await.expect("wait");
    serde_json::from_slice(&output.stdout).expect("result")
}

#[tokio::test]
#[ignore = "requires interactive Windows runner with licensed Excel and test workbook"]
async fn excel_zero_and_one_map_to_scheduler_outcomes() {
    let success = run_macro("TestModule", "ReturnZero", basic_arguments()).await;
    assert_eq!(success.outcome, ExecutionOutcome::Succeeded);
    assert_eq!(success.exit_code, Some(0));
    assert!(success.diagnostic.is_none());

    let failure = run_macro("TestModule", "ReturnOne", basic_arguments()).await;
    assert_eq!(failure.outcome, ExecutionOutcome::Failed);
    assert_eq!(failure.exit_code, Some(1));
    let diagnostic = failure.diagnostic.expect("diagnostic");
    assert_eq!(diagnostic.code, FailureCode::ExcelMacroReturnedFailure);
    assert_eq!(diagnostic.origin, FailureOrigin::ExcelMacro);
}

#[tokio::test]
#[ignore = "requires interactive Windows runner with licensed Excel and test workbook"]
async fn excel_vba_error_and_process_crash_are_distinguished() {
    let vba_error = run_macro("TestModule", "RaiseVbaError", basic_arguments()).await;
    assert_eq!(vba_error.outcome, ExecutionOutcome::InfrastructureError);
    let diagnostic = vba_error.diagnostic.expect("VBA diagnostic");
    assert_eq!(diagnostic.code, FailureCode::ExcelMacroFailed);
    assert_eq!(diagnostic.origin, FailureOrigin::ExcelMacro);
    assert!(diagnostic.status.expect("COM status").hresult.is_some());

    let crash = run_macro("TestModule", "CrashExcel", basic_arguments()).await;
    assert_eq!(crash.outcome, ExecutionOutcome::InfrastructureError);
    let diagnostic = crash.diagnostic.expect("crash diagnostic");
    assert_eq!(diagnostic.code, FailureCode::ExcelProcessCrashed);
    assert_eq!(diagnostic.origin, FailureOrigin::ExcelHostProcess);
}

#[tokio::test]
#[ignore = "requires interactive Windows runner with licensed Excel and test workbook"]
async fn excel_process_id_signature_preserves_all_seventeen_values_and_order() {
    // Keep this argument list synchronized with the licensed workbook fixture
    // contract documented in docs/testing.md.
    let result = run_macro(
        "TestModule",
        "ValidateProcessIdArguments",
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
        ],
    )
    .await;

    assert_eq!(result.outcome, ExecutionOutcome::Succeeded);
    assert_eq!(result.exit_code, Some(0));
    assert!(result.diagnostic.is_none());
}
