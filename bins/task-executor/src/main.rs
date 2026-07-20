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
    ExecutorSpec,
};
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
    let guard = ProcessTree::attach(child.id().context("task process has no PID")?)?;
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
    let mut error = None;
    if stdout_truncated || stderr_truncated {
        error = Some("task output was truncated at 1 MiB per stream".into());
    }
    let (outcome, exit_code, signal) = match end {
        End::Exit(status) => {
            let code = status.code();
            let outcome = if code == Some(0) {
                ExecutionOutcome::Succeeded
            } else if excel_exit_codes && code != Some(1) {
                ExecutionOutcome::InfrastructureError
            } else {
                ExecutionOutcome::Failed
            };
            (outcome, code, exit_signal(&status))
        }
        End::Timeout => (ExecutionOutcome::TimedOut, None, None),
        End::Lease => (ExecutionOutcome::LeaseExpired, None, None),
        End::Cancel => (ExecutionOutcome::Cancelled, None, None),
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
    })
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
try {
  $excel = New-Object -ComObject Excel.Application
  $excel.Visible = [bool]$payload.visible
  $excel.DisplayAlerts = $false
  $excel.UserControl = $false
  $workbook = $excel.Workbooks.Open([string]$payload.workbook_path, 0, [bool]$payload.read_only)
  $macro = "'" + $workbook.Name + "'!" + [string]$payload.macro_name
  $invokeArgs = New-Object System.Collections.Generic.List[Object]
  $invokeArgs.Add($macro)
  foreach ($arg in $payload.args) { $invokeArgs.Add($arg) }
  $result = $excel.GetType().InvokeMember('Run', [Reflection.BindingFlags]::InvokeMethod, $null, $excel, $invokeArgs.ToArray())
  $code = [Convert]::ToInt32($result)
  if ($code -ne 0 -and $code -ne 1) { throw "Macro returned unsupported value: $code" }
  exit $code
} catch {
  [Console]::Error.WriteLine($_.Exception.ToString())
  exit 2
} finally {
  if ($null -ne $workbook) { try { $workbook.Close([bool]$payload.save_changes) } catch {} }
  if ($null -ne $excel) { try { $excel.Quit() } catch {} }
  if ($null -ne $workbook) { [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($workbook) }
  if ($null -ne $excel) { [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($excel) }
  [GC]::Collect(); [GC]::WaitForPendingFinalizers()
}
"#;
