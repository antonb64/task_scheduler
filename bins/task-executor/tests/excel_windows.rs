#![cfg(windows)]

use std::{collections::BTreeMap, process::Stdio};

use scheduler_core::{
    ExcelMacroSpec, ExecutionAssignment, ExecutionOutcome, ExecutionPolicy, ExecutionSnapshot,
    ExecutorSpec,
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
    assert_eq!(
        run_macro("TestModule.ReturnZero").await.outcome,
        ExecutionOutcome::Succeeded
    );
    assert_eq!(
        run_macro("TestModule.ReturnOne").await.outcome,
        ExecutionOutcome::Failed
    );
}
