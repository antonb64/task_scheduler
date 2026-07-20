#![cfg(unix)]

use std::{collections::BTreeMap, process::Stdio};

use scheduler_core::{
    CommandSpec, ExecutionAssignment, ExecutionOutcome, ExecutionPolicy, ExecutionSnapshot,
    ExecutorSpec,
};
use tokio::{io::AsyncWriteExt, process::Command};
use uuid::Uuid;

fn assignment(program: &str, args: Vec<String>, lease_seconds: u64) -> ExecutionAssignment {
    ExecutionAssignment {
        schedule_id: Uuid::new_v4(),
        run_id: Uuid::new_v4(),
        attempt_id: Uuid::new_v4(),
        attempt_number: 1,
        lease_token: Uuid::new_v4().to_string(),
        lease_seconds,
        snapshot: ExecutionSnapshot {
            executor: ExecutorSpec::Command(CommandSpec {
                program: program.into(),
                args,
                env: BTreeMap::new(),
                working_directory: None,
            }),
            policy: ExecutionPolicy {
                timeout_seconds: 10,
                ..ExecutionPolicy::default()
            },
            required_labels: BTreeMap::new(),
            parameters_digest: "test".into(),
        },
    }
}

async fn execute(assignment: ExecutionAssignment) -> scheduler_core::ExecutionResult {
    let mut child = Command::new(env!("CARGO_BIN_EXE_task-executor"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    let mut stdin = child.stdin.take().expect("stdin");
    stdin
        .write_all(serde_json::to_string(&assignment).expect("json").as_bytes())
        .await
        .expect("write");
    stdin.write_all(b"\n").await.expect("newline");
    drop(stdin);
    let output = child.wait_with_output().await.expect("wait");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("result")
}

#[tokio::test]
async fn command_output_and_success_are_reported() {
    let result = execute(assignment("/bin/echo", vec!["hello".into()], 10)).await;
    assert_eq!(result.outcome, ExecutionOutcome::Succeeded);
    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.stdout, "hello\n");
}

#[tokio::test]
async fn completed_command_exits_while_agent_control_pipe_remains_open() {
    let assignment = assignment("/bin/echo", vec!["still-open".into()], 10);
    let mut child = Command::new(env!("CARGO_BIN_EXE_task-executor"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    let mut control_pipe = child.stdin.take().expect("stdin");
    control_pipe
        .write_all(serde_json::to_string(&assignment).expect("json").as_bytes())
        .await
        .expect("write");
    control_pipe.write_all(b"\n").await.expect("newline");

    let output = tokio::time::timeout(std::time::Duration::from_secs(3), child.wait_with_output())
        .await
        .expect("executor must not wait for the still-open control pipe")
        .expect("wait");
    drop(control_pipe);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: scheduler_core::ExecutionResult =
        serde_json::from_slice(&output.stdout).expect("result");
    assert_eq!(result.outcome, ExecutionOutcome::Succeeded);
}

#[tokio::test]
async fn missing_keepalive_expires_the_process_tree() {
    let result = execute(assignment("/bin/sleep", vec!["5".into()], 1)).await;
    assert_eq!(result.outcome, ExecutionOutcome::LeaseExpired);
}

#[tokio::test]
async fn nonzero_exit_is_a_task_failure() {
    let result = execute(assignment("false", Vec::new(), 10)).await;
    assert_eq!(result.outcome, ExecutionOutcome::Failed);
    assert_eq!(result.exit_code, Some(1));
}

#[tokio::test]
async fn configured_timeout_terminates_the_command() {
    let mut task = assignment("/bin/sleep", vec!["5".into()], 10);
    task.snapshot.policy.timeout_seconds = 1;
    let result = execute(task).await;
    assert_eq!(result.outcome, ExecutionOutcome::TimedOut);
}

#[tokio::test]
async fn cancellation_terminates_the_command() {
    let task = assignment("/bin/sleep", vec!["5".into()], 10);
    let mut child = Command::new(env!("CARGO_BIN_EXE_task-executor"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    let mut control_pipe = child.stdin.take().expect("stdin");
    control_pipe
        .write_all(serde_json::to_string(&task).expect("json").as_bytes())
        .await
        .expect("assignment");
    control_pipe
        .write_all(b"\n{\"cancel\":true}\n")
        .await
        .expect("cancel");
    let output = tokio::time::timeout(std::time::Duration::from_secs(3), child.wait_with_output())
        .await
        .expect("executor exits after cancellation")
        .expect("wait");
    let result: scheduler_core::ExecutionResult =
        serde_json::from_slice(&output.stdout).expect("result");
    assert_eq!(result.outcome, ExecutionOutcome::Cancelled);
}

#[tokio::test]
async fn noisy_output_is_bounded_and_marked_as_truncated() {
    let mut task = assignment("/usr/bin/yes", vec!["bounded".into()], 10);
    task.snapshot.policy.timeout_seconds = 1;
    let result = execute(task).await;
    assert_eq!(result.outcome, ExecutionOutcome::TimedOut);
    assert!(result.stdout.len() <= 1_048_576);
    assert!(
        result
            .error
            .as_deref()
            .is_some_and(|error| error.contains("truncated"))
    );
}
