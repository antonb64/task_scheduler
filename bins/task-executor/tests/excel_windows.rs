#![cfg(windows)]

use std::{collections::BTreeMap, process::Stdio};

use scheduler_core::{
    ExcelMacroSpec, ExecutionAssignment, ExecutionOutcome, ExecutionPolicy, ExecutionSnapshot,
    ExecutorSpec, FailureCode, FailureOrigin,
};
use tokio::{io::AsyncWriteExt, process::Command};
use uuid::Uuid;

async fn run_macro(macro_name: &str) -> scheduler_core::ExecutionResult {
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
                macro_name: macro_name.into(),
                args: vec![
                    serde_json::json!("value"),
                    serde_json::json!(42),
                    serde_json::json!(true),
                ],
                read_only: true,
                save_changes: false,
                visible: false,
            }),
            policy: ExecutionPolicy {
                timeout_seconds: 60,
                ..ExecutionPolicy::default()
            },
            required_labels: BTreeMap::new(),
            parameters_digest: "test".into(),
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
    let success = run_macro("TestModule.ReturnZero").await;
    assert_eq!(success.outcome, ExecutionOutcome::Succeeded);
    assert_eq!(success.exit_code, Some(0));
    assert!(success.diagnostic.is_none());

    let failure = run_macro("TestModule.ReturnOne").await;
    assert_eq!(failure.outcome, ExecutionOutcome::Failed);
    assert_eq!(failure.exit_code, Some(1));
    let diagnostic = failure.diagnostic.expect("diagnostic");
    assert_eq!(diagnostic.code, FailureCode::ExcelMacroReturnedFailure);
    assert_eq!(diagnostic.origin, FailureOrigin::ExcelMacro);
}

#[tokio::test]
#[ignore = "requires interactive Windows runner with licensed Excel and test workbook"]
async fn excel_vba_error_and_process_crash_are_distinguished() {
    let vba_error = run_macro("TestModule.RaiseVbaError").await;
    assert_eq!(vba_error.outcome, ExecutionOutcome::InfrastructureError);
    let diagnostic = vba_error.diagnostic.expect("VBA diagnostic");
    assert_eq!(diagnostic.code, FailureCode::ExcelMacroFailed);
    assert_eq!(diagnostic.origin, FailureOrigin::ExcelMacro);
    assert!(diagnostic.status.expect("COM status").hresult.is_some());

    let crash = run_macro("TestModule.CrashExcel").await;
    assert_eq!(crash.outcome, ExecutionOutcome::InfrastructureError);
    let diagnostic = crash.diagnostic.expect("crash diagnostic");
    assert_eq!(diagnostic.code, FailureCode::ExcelProcessCrashed);
    assert_eq!(diagnostic.origin, FailureOrigin::ExcelHostProcess);
}
