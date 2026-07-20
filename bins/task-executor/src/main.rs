use std::{
    io::BufRead as _,
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
#[cfg(windows)]
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::Utc;
use scheduler_core::{
    CommandSpec, ExcelMacroSpec, ExecutionAssignment, ExecutionOutcome, ExecutionResult,
    ExecutorSpec, FailureCode, FailureDiagnostic, FailureOrigin, FailureStage, FailureStatus,
    OutputMetadata,
};
use serde::Deserialize;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    sync::watch,
};

const OUTPUT_LIMIT: usize = 1_048_576;

#[derive(Debug, Clone)]
struct ControlState {
    last_keepalive: Instant,
    cancelled: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    // Tokio implements stdin through a blocking background read. That read cannot
    // be cancelled, so a still-open agent control pipe would keep this short-lived
    // executor's runtime alive after the task had finished. Use a detached OS
    // thread instead: process exit reliably tears it down.
    let assignment_line = {
        let stdin = std::io::stdin();
        let mut stdin = stdin.lock();
        let mut line = String::new();
        let read = stdin
            .read_line(&mut line)
            .context("failed to read assignment JSON from stdin")?;
        if read == 0 {
            bail!("assignment JSON is required on stdin");
        }
        line
    };
    let assignment: ExecutionAssignment =
        serde_json::from_str(&assignment_line).context("invalid assignment JSON")?;
    let initial = ControlState {
        last_keepalive: Instant::now(),
        cancelled: false,
    };
    let (control_tx, control_rx) = watch::channel(initial);
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else {
                break;
            };
            let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };
            let mut state = control_tx.borrow().clone();
            if value.get("keepalive").and_then(|value| value.as_bool()) == Some(true) {
                state.last_keepalive = Instant::now();
            }
            if value.get("cancel").and_then(|value| value.as_bool()) == Some(true) {
                state.cancelled = true;
            }
            let _ = control_tx.send(state);
        }
    });

    let result = execute(&assignment, control_rx).await;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

async fn execute(
    assignment: &ExecutionAssignment,
    control: watch::Receiver<ControlState>,
) -> ExecutionResult {
    let started_at = Utc::now();
    let result = match &assignment.snapshot.executor {
        ExecutorSpec::Command(command) => run_command(assignment, command, control).await,
        ExecutorSpec::ExcelMacro(excel) => run_excel(assignment, excel, control).await,
    };
    match result {
        Ok(mut result) => {
            result.started_at = started_at;
            result.finished_at = Utc::now();
            result
        }
        Err(error) => ExecutionResult {
            outcome: ExecutionOutcome::InfrastructureError,
            exit_code: None,
            signal: None,
            stdout: String::new(),
            stderr: String::new(),
            started_at,
            finished_at: Utc::now(),
            error: Some(format!("{error:#}")),
            output: OutputMetadata::default(),
            diagnostic: Some(execution_error_diagnostic(
                &assignment.snapshot.executor,
                &error,
            )),
        },
    }
}

async fn run_command(
    assignment: &ExecutionAssignment,
    spec: &CommandSpec,
    control: watch::Receiver<ControlState>,
) -> Result<ExecutionResult> {
    let mut command = Command::new(&spec.program);
    command.args(&spec.args).envs(&spec.env);
    if let Some(directory) = &spec.working_directory {
        command.current_dir(directory);
    }
    command
        .env("TASK_RUN_ID", assignment.run_id.to_string())
        .env("TASK_ATTEMPT_ID", assignment.attempt_id.to_string());
    run_process(
        command,
        assignment.snapshot.policy.timeout_seconds,
        assignment.lease_seconds,
        control,
        false,
    )
    .await
}

#[cfg(windows)]
async fn run_excel(
    assignment: &ExecutionAssignment,
    spec: &ExcelMacroSpec,
    control: watch::Receiver<ControlState>,
) -> Result<ExecutionResult> {
    let payload = serde_json::json!({
        "workbook_path": spec.workbook_path,
        "macro_name": spec.macro_name,
        "args": spec.args,
        "read_only": spec.read_only,
        "save_changes": spec.save_changes,
        "visible": spec.visible,
        "run_id": assignment.run_id,
        "attempt_id": assignment.attempt_id,
    });
    let encoded_script = encode_powershell(EXCEL_SCRIPT);
    let mut command = Command::new("powershell.exe");
    command
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-EncodedCommand",
            &encoded_script,
        ])
        .env(
            "SCHEDULER_EXCEL_PAYLOAD_B64",
            STANDARD.encode(serde_json::to_vec(&payload)?),
        );
    run_process(
        command,
        assignment.snapshot.policy.timeout_seconds,
        assignment.lease_seconds,
        control,
        true,
    )
    .await
}

#[cfg(not(windows))]
async fn run_excel(
    _assignment: &ExecutionAssignment,
    _spec: &ExcelMacroSpec,
    _control: watch::Receiver<ControlState>,
) -> Result<ExecutionResult> {
    bail!("excel_macro executor is only available on Windows")
}

async fn run_process(
    mut command: Command,
    timeout_seconds: u64,
    lease_seconds: u64,
    mut control: watch::Receiver<ControlState>,
    excel_exit_codes: bool,
) -> Result<ExecutionResult> {
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    configure_process_group(&mut command)?;
    let mut child = command.spawn().context("failed to start task process")?;
    let process_id = child.id().context("task process has no PID")?;
    let guard = ProcessTree::attach(process_id)?;
    let stdout = child.stdout.take().context("stdout pipe unavailable")?;
    let stderr = child.stderr.take().context("stderr pipe unavailable")?;
    let stdout_task = tokio::spawn(read_bounded(stdout));
    let stderr_task = tokio::spawn(read_bounded(stderr));

    let timeout = tokio::time::sleep(Duration::from_secs(timeout_seconds));
    tokio::pin!(timeout);
    let lease = lease_watchdog(control.clone(), Duration::from_secs(lease_seconds));
    tokio::pin!(lease);
    let cancellation = async {
        loop {
            if control.borrow().cancelled {
                break;
            }
            if control.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    };
    tokio::pin!(cancellation);

    enum End {
        Exit(std::process::ExitStatus),
        Timeout,
        Lease,
        Cancel,
    }
    let end = tokio::select! {
        status = child.wait() => End::Exit(status?),
        _ = &mut timeout => End::Timeout,
        _ = &mut lease => End::Lease,
        _ = &mut cancellation => End::Cancel,
    };
    if !matches!(end, End::Exit(_)) {
        terminate(&mut child, &guard).await;
    }
    let (stdout, stdout_truncated) = stdout_task.await??;
    let (stderr, stderr_truncated) = stderr_task.await??;
    let output = OutputMetadata {
        stdout_bytes: stdout.len() as u64,
        stderr_bytes: stderr.len() as u64,
        stdout_truncated,
        stderr_truncated,
    };
    let mut error = None;
    if stdout_truncated || stderr_truncated {
        error = Some("task output was truncated at 1 MiB per stream".into());
    }
    let (outcome, exit_code, signal, diagnostic) = match end {
        End::Exit(status) => {
            let code = status.code();
            let signal = exit_signal(&status);
            let diagnostic = if code == Some(0) {
                None
            } else if excel_exit_codes {
                Some(excel_failure_diagnostic(
                    code,
                    signal.clone(),
                    process_id,
                    &stderr,
                ))
            } else {
                Some(command_failure_diagnostic(code, signal.clone(), process_id))
            };
            let outcome = match diagnostic.as_ref().map(|value| value.code) {
                None => ExecutionOutcome::Succeeded,
                Some(FailureCode::ProcessExitedNonZero)
                | Some(FailureCode::ExcelMacroReturnedFailure) => ExecutionOutcome::Failed,
                Some(_) => ExecutionOutcome::InfrastructureError,
            };
            (outcome, code, signal, diagnostic)
        }
        End::Timeout => (
            ExecutionOutcome::TimedOut,
            None,
            None,
            Some(
                FailureDiagnostic::new(
                    FailureCode::ProcessTimedOut,
                    process_origin(excel_exit_codes),
                    FailureStage::Execution,
                    "task exceeded its configured timeout",
                    true,
                )
                .with_status(process_status(process_id, None, None, None, None)),
            ),
        ),
        End::Lease => (
            ExecutionOutcome::LeaseExpired,
            None,
            None,
            Some(
                FailureDiagnostic::new(
                    FailureCode::AgentLeaseExpired,
                    FailureOrigin::Agent,
                    FailureStage::Lease,
                    "executor stopped because agent keepalives or its lease expired",
                    true,
                )
                .with_status(process_status(process_id, None, None, None, None)),
            ),
        ),
        End::Cancel => (
            ExecutionOutcome::Cancelled,
            None,
            None,
            Some(
                FailureDiagnostic::new(
                    FailureCode::Cancelled,
                    FailureOrigin::Coordinator,
                    FailureStage::Cancellation,
                    "task was cancelled by an administrator",
                    false,
                )
                .with_status(process_status(process_id, None, None, None, None)),
            ),
        ),
    };
    Ok(ExecutionResult {
        outcome,
        exit_code,
        signal,
        stdout,
        stderr,
        started_at: Utc::now(),
        finished_at: Utc::now(),
        error,
        output,
        diagnostic,
    })
}

fn execution_error_diagnostic(executor: &ExecutorSpec, error: &anyhow::Error) -> FailureDiagnostic {
    let detail = format!("{error:#}");
    let origin = match executor {
        ExecutorSpec::Command(_) => FailureOrigin::CommandProcess,
        ExecutorSpec::ExcelMacro(_) => FailureOrigin::ExcelHostProcess,
    };
    if detail.contains("only available on Windows") {
        FailureDiagnostic::new(
            FailureCode::ExcelUnsupported,
            FailureOrigin::ExcelAutomation,
            FailureStage::Validation,
            "Excel automation is unavailable on this operating system",
            false,
        )
    } else if detail.contains("failed to start task process") {
        FailureDiagnostic::new(
            FailureCode::ProcessSpawnFailed,
            origin,
            FailureStage::ProcessStart,
            "task process could not be started",
            true,
        )
    } else if detail.contains("process group")
        || detail.contains("Job Object")
        || detail.contains("OpenProcess")
    {
        FailureDiagnostic::new(
            FailureCode::ProcessIsolationFailed,
            FailureOrigin::TaskExecutor,
            FailureStage::Isolation,
            "task process isolation could not be established",
            true,
        )
    } else {
        FailureDiagnostic::new(
            FailureCode::InfrastructureError,
            FailureOrigin::TaskExecutor,
            FailureStage::Execution,
            "task executor encountered an internal infrastructure error",
            true,
        )
    }
}

fn command_failure_diagnostic(
    exit_code: Option<i32>,
    signal: Option<String>,
    process_id: u32,
) -> FailureDiagnostic {
    let crashed = signal.is_some() || exit_code.is_some_and(|code| cfg!(windows) && code < 0);
    let (code, summary, retryable) = if crashed {
        (
            FailureCode::ProcessCrashed,
            "command process crashed or was terminated by the operating system",
            true,
        )
    } else {
        (
            FailureCode::ProcessExitedNonZero,
            "command process returned a non-zero status code",
            false,
        )
    };
    FailureDiagnostic::new(
        code,
        FailureOrigin::CommandProcess,
        FailureStage::Execution,
        summary,
        retryable,
    )
    .with_status(process_status(process_id, exit_code, signal, None, None))
}

#[derive(Debug, Deserialize)]
struct ExcelDiagnosticMarker {
    code: String,
    stage: String,
    summary: String,
    hresult: Option<i64>,
    hresult_hex: Option<String>,
}

fn excel_failure_diagnostic(
    exit_code: Option<i32>,
    signal: Option<String>,
    process_id: u32,
    stderr: &str,
) -> FailureDiagnostic {
    if exit_code == Some(1) {
        return FailureDiagnostic::new(
            FailureCode::ExcelMacroReturnedFailure,
            FailureOrigin::ExcelMacro,
            FailureStage::MacroResult,
            "Excel macro returned 1",
            false,
        )
        .with_status(process_status(process_id, exit_code, signal, None, None));
    }
    if let Some(marker) = parse_excel_marker(stderr) {
        let code = match marker.code.as_str() {
            "excel_startup_failed" => FailureCode::ExcelStartupFailed,
            "excel_workbook_open_failed" => FailureCode::ExcelWorkbookOpenFailed,
            "excel_macro_failed" => FailureCode::ExcelMacroFailed,
            "excel_invalid_return" => FailureCode::ExcelInvalidReturn,
            "excel_process_crashed" => FailureCode::ExcelProcessCrashed,
            "excel_cleanup_failed" => FailureCode::ExcelCleanupFailed,
            _ => FailureCode::InfrastructureError,
        };
        let origin = match code {
            FailureCode::ExcelMacroFailed | FailureCode::ExcelInvalidReturn => {
                FailureOrigin::ExcelMacro
            }
            FailureCode::ExcelProcessCrashed => FailureOrigin::ExcelHostProcess,
            _ => FailureOrigin::ExcelAutomation,
        };
        let stage = match marker.stage.as_str() {
            "excel_startup" => FailureStage::ExcelStartup,
            "workbook_open" => FailureStage::WorkbookOpen,
            "macro_invoke" => FailureStage::MacroInvoke,
            "macro_result" => FailureStage::MacroResult,
            "cleanup" => FailureStage::Cleanup,
            _ => FailureStage::Execution,
        };
        return FailureDiagnostic::new(code, origin, stage, marker.summary, true).with_status(
            process_status(
                process_id,
                exit_code,
                signal,
                marker.hresult,
                marker.hresult_hex,
            ),
        );
    }

    FailureDiagnostic::new(
        FailureCode::ExecutorProcessCrashed,
        FailureOrigin::ExcelHostProcess,
        FailureStage::Execution,
        "Excel automation host exited without a structured diagnostic",
        true,
    )
    .with_status(process_status(process_id, exit_code, signal, None, None))
}

fn parse_excel_marker(stderr: &str) -> Option<ExcelDiagnosticMarker> {
    stderr.lines().find_map(|line| {
        line.strip_prefix("SCHEDULER_DIAGNOSTIC:")
            .and_then(|json| serde_json::from_str(json).ok())
    })
}

fn process_origin(excel: bool) -> FailureOrigin {
    if excel {
        FailureOrigin::ExcelHostProcess
    } else {
        FailureOrigin::CommandProcess
    }
}

fn process_status(
    process_id: u32,
    status_code: Option<i32>,
    signal: Option<String>,
    hresult: Option<i64>,
    hresult_hex: Option<String>,
) -> FailureStatus {
    FailureStatus {
        process_id: Some(process_id),
        status_code: status_code.map(i64::from),
        status_code_hex: status_code.map(|code| format!("0x{:08X}", code as u32)),
        signal,
        hresult,
        hresult_hex,
    }
}

async fn lease_watchdog(mut control: watch::Receiver<ControlState>, lease: Duration) {
    loop {
        let deadline = control.borrow().last_keepalive + lease;
        tokio::select! {
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => return,
            changed = control.changed() => if changed.is_err() {
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                return;
            },
        }
    }
}

async fn read_bounded(mut reader: impl AsyncRead + Unpin) -> Result<(String, bool)> {
    let mut retained = Vec::new();
    let mut buffer = [0_u8; 8192];
    let mut truncated = false;
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let available = OUTPUT_LIMIT.saturating_sub(retained.len());
        retained.extend_from_slice(&buffer[..read.min(available)]);
        truncated |= read > available;
    }
    Ok((String::from_utf8_lossy(&retained).into_owned(), truncated))
}

async fn terminate(child: &mut Child, guard: &ProcessTree) {
    guard.terminate_gracefully();
    if tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .is_err()
    {
        guard.terminate_forcefully();
        let _ = child.wait().await;
    }
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) -> Result<()> {
    use std::os::unix::process::CommandExt;
    unsafe {
        command.as_std_mut().pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            #[cfg(target_os = "linux")]
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}

#[cfg(windows)]
fn configure_process_group(command: &mut Command) -> Result<()> {
    use std::os::windows::process::CommandExt;
    command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NEW_PROCESS_GROUP);
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn configure_process_group(_command: &mut Command) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
struct ProcessTree {
    process_group: i32,
}

#[cfg(unix)]
impl ProcessTree {
    fn attach(pid: u32) -> Result<Self> {
        Ok(Self {
            process_group: pid as i32,
        })
    }
    fn terminate_gracefully(&self) {
        unsafe {
            libc::kill(-self.process_group, libc::SIGTERM);
        }
    }
    fn terminate_forcefully(&self) {
        unsafe {
            libc::kill(-self.process_group, libc::SIGKILL);
        }
    }
}

#[cfg(windows)]
struct ProcessTree {
    job: windows_sys::Win32::Foundation::HANDLE,
}

#[cfg(windows)]
impl ProcessTree {
    fn attach(pid: u32) -> Result<Self> {
        use std::{
            mem::{size_of, zeroed},
            ptr::{null, null_mut},
        };
        use windows_sys::Win32::{
            Foundation::CloseHandle,
            System::{JobObjects::*, Threading::*},
        };
        unsafe {
            let job = CreateJobObjectW(null(), null());
            if job.is_null() {
                bail!(
                    "CreateJobObjectW failed: {}",
                    std::io::Error::last_os_error()
                );
            }
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = zeroed();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const _,
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            ) == 0
            {
                CloseHandle(job);
                bail!(
                    "SetInformationJobObject failed: {}",
                    std::io::Error::last_os_error()
                );
            }
            let process = OpenProcess(PROCESS_SET_QUOTA | PROCESS_TERMINATE, 0, pid);
            if process.is_null() {
                CloseHandle(job);
                bail!("OpenProcess failed: {}", std::io::Error::last_os_error());
            }
            let assigned = AssignProcessToJobObject(job, process);
            CloseHandle(process);
            if assigned == 0 {
                CloseHandle(job);
                bail!(
                    "cannot isolate task in a Windows Job Object: {}",
                    std::io::Error::last_os_error()
                );
            }
            Ok(Self { job })
        }
        fn terminate_gracefully(&self) {
            unsafe {
                windows_sys::Win32::System::JobObjects::TerminateJobObject(self.job, 1);
            }
        }
        fn terminate_forcefully(&self) {
            self.terminate_gracefully();
        }
    }
}

#[cfg(windows)]
impl Drop for ProcessTree {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.job);
        }
    }
}

#[cfg(not(any(unix, windows)))]
struct ProcessTree;

#[cfg(not(any(unix, windows)))]
impl ProcessTree {
    fn attach(_pid: u32) -> Result<Self> {
        Ok(Self)
    }
    fn terminate_gracefully(&self) {}
    fn terminate_forcefully(&self) {}
}

#[cfg(unix)]
fn exit_signal(status: &std::process::ExitStatus) -> Option<String> {
    use std::os::unix::process::ExitStatusExt;
    status.signal().map(|signal| signal.to_string())
}

#[cfg(not(unix))]
fn exit_signal(_status: &std::process::ExitStatus) -> Option<String> {
    None
}

#[cfg(windows)]
fn encode_powershell(script: &str) -> String {
    let bytes = script
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    STANDARD.encode(bytes)
}

#[cfg(windows)]
const EXCEL_SCRIPT: &str = r#"
$ErrorActionPreference = 'Stop'
$payloadJson = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($env:SCHEDULER_EXCEL_PAYLOAD_B64))
$payload = $payloadJson | ConvertFrom-Json
$excel = $null
$workbook = $null
$stage = 'excel_startup'
$exitCode = 2
$diagnostic = $null
$exceptionDetail = $null
try {
  $excel = New-Object -ComObject Excel.Application
  $excel.Visible = [bool]$payload.visible
  $excel.DisplayAlerts = $false
  $excel.UserControl = $false
  $stage = 'workbook_open'
  $workbook = $excel.Workbooks.Open([string]$payload.workbook_path, 0, [bool]$payload.read_only)
  $macro = "'" + $workbook.Name + "'!" + [string]$payload.macro_name
  $invokeArgs = New-Object System.Collections.Generic.List[Object]
  $invokeArgs.Add($macro)
  foreach ($arg in $payload.args) { $invokeArgs.Add($arg) }
  $stage = 'macro_invoke'
  $result = $excel.GetType().InvokeMember('Run', [Reflection.BindingFlags]::InvokeMethod, $null, $excel, $invokeArgs.ToArray())
  $stage = 'macro_result'
  $integerTypes = @([sbyte], [byte], [int16], [uint16], [int32], [uint32], [int64], [uint64])
  if (-not ($integerTypes | Where-Object { $_.IsInstanceOfType($result) })) {
    throw "Macro returned a non-integer value of type $($result.GetType().FullName)"
  }
  $code = [Convert]::ToInt32($result)
  if ($code -ne 0 -and $code -ne 1) { throw "Macro returned unsupported value: $code" }
  $exitCode = $code
} catch {
  $exceptionDetail = $_.Exception.ToString()
  $hresult = [int64]$_.Exception.HResult
  $hresultHex = ('0x{0:X8}' -f ($hresult -band 0xffffffffL))
  $failureCode = switch ($stage) {
    'excel_startup' { 'excel_startup_failed' }
    'workbook_open' { 'excel_workbook_open_failed' }
    'macro_invoke' { 'excel_macro_failed' }
    'macro_result' { 'excel_invalid_return' }
    default { 'excel_automation_failed' }
  }
  $summary = switch ($stage) {
    'excel_startup' { 'private Excel application could not be started' }
    'workbook_open' { 'Excel could not open the configured workbook' }
    'macro_invoke' { 'Excel or VBA failed while invoking the macro' }
    'macro_result' { 'Excel macro returned an unsupported value' }
    default { 'Excel automation failed' }
  }
  if (@('0x800706BA', '0x80010108', '0x80010007') -contains $hresultHex) {
    $failureCode = 'excel_process_crashed'
    $summary = 'private Excel process crashed or disconnected during automation'
    $exitCode = 14
  } else {
    $exitCode = switch ($stage) {
      'excel_startup' { 10 }
      'workbook_open' { 11 }
      'macro_invoke' { 12 }
      'macro_result' { 13 }
      default { 2 }
    }
  }
  $diagnostic = @{ code = $failureCode; stage = $stage; summary = $summary; hresult = $hresult; hresult_hex = $hresultHex }
} finally {
  $cleanupFailed = $false
  if ($null -ne $workbook) { try { $workbook.Close([bool]$payload.save_changes) } catch { $cleanupFailed = $true } }
  if ($null -ne $excel) { try { $excel.Quit() } catch { $cleanupFailed = $true } }
  if ($null -ne $workbook) { try { [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($workbook) } catch { $cleanupFailed = $true } }
  if ($null -ne $excel) { try { [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($excel) } catch { $cleanupFailed = $true } }
  [GC]::Collect(); [GC]::WaitForPendingFinalizers()
  if ($cleanupFailed -and $null -eq $diagnostic) {
    $exitCode = 15
    $diagnostic = @{ code = 'excel_cleanup_failed'; stage = 'cleanup'; summary = 'Excel workbook or application cleanup failed'; hresult = $null; hresult_hex = $null }
  }
}
if ($null -ne $diagnostic) {
  [Console]::Error.WriteLine('SCHEDULER_DIAGNOSTIC:' + ($diagnostic | ConvertTo-Json -Compress))
}
if ($null -ne $exceptionDetail) { [Console]::Error.WriteLine($exceptionDetail) }
exit $exitCode
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn excel_return_one_is_a_macro_failure_with_status_code() {
        let diagnostic = excel_failure_diagnostic(Some(1), None, 42, "");
        assert_eq!(diagnostic.code, FailureCode::ExcelMacroReturnedFailure);
        assert_eq!(diagnostic.origin, FailureOrigin::ExcelMacro);
        assert_eq!(diagnostic.stage, FailureStage::MacroResult);
        assert_eq!(diagnostic.status.expect("status").status_code, Some(1));
    }

    #[test]
    fn excel_com_marker_preserves_stage_and_hresult() {
        let stderr = concat!(
            "SCHEDULER_DIAGNOSTIC:",
            r#"{"code":"excel_macro_failed","stage":"macro_invoke","summary":"Excel or VBA failed while invoking the macro","hresult":-2146827284,"hresult_hex":"0x800A03EC"}"#,
            "\nfull encrypted detail"
        );
        let diagnostic = excel_failure_diagnostic(Some(12), None, 42, stderr);
        assert_eq!(diagnostic.code, FailureCode::ExcelMacroFailed);
        assert_eq!(diagnostic.origin, FailureOrigin::ExcelMacro);
        assert_eq!(diagnostic.stage, FailureStage::MacroInvoke);
        let status = diagnostic.status.expect("status");
        assert_eq!(status.status_code, Some(12));
        assert_eq!(status.hresult_hex.as_deref(), Some("0x800A03EC"));
    }

    #[test]
    fn excel_disconnect_marker_is_identified_as_process_crash() {
        let stderr = concat!(
            "SCHEDULER_DIAGNOSTIC:",
            r#"{"code":"excel_process_crashed","stage":"macro_invoke","summary":"private Excel process crashed or disconnected during automation","hresult":-2147023174,"hresult_hex":"0x800706BA"}"#
        );
        let diagnostic = excel_failure_diagnostic(Some(14), None, 42, stderr);
        assert_eq!(diagnostic.code, FailureCode::ExcelProcessCrashed);
        assert_eq!(diagnostic.origin, FailureOrigin::ExcelHostProcess);
        assert_eq!(diagnostic.stage, FailureStage::MacroInvoke);
    }
}
