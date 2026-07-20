param(
  [Parameter(Mandatory=$true)][string]$InstallDirectory,
  [Parameter(Mandatory=$true)][string]$AgentId,
  [Parameter(Mandatory=$true)][string]$CoordinatorUrl
)

$agent = Join-Path $InstallDirectory "agent.exe"
if (-not (Test-Path $agent)) { throw "agent.exe not found at $agent" }

$action = New-ScheduledTaskAction -Execute $agent -Argument "--agent-id `"$AgentId`" --coordinator-url `"$CoordinatorUrl`" --executor-path `"$(Join-Path $InstallDirectory 'task-executor.exe')`""
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask -TaskName "RustTaskSchedulerAgent" -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force

Write-Host "Agent registered for the current interactive user. Excel automation must not be changed to a Windows-service logon type."
