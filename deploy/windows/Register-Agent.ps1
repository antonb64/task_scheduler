param(
  [Parameter(Mandatory=$true)][string]$InstallDirectory,
  [Parameter(Mandatory=$true)][string]$AgentId,
  [Parameter(Mandatory=$true)][string]$CoordinatorUrl,
  [string]$DataDirectory = (Join-Path $env:LOCALAPPDATA "RustTaskScheduler"),
  [string]$UiAddress = "127.0.0.1:8081",
  [ValidateRange(1, 1024)][int]$Capacity = 2,
  [string[]]$Labels = @(),
  [string]$TlsCa,
  [string]$TlsCert,
  [string]$TlsKey,
  [string]$TlsDomain,
  [string]$OtlpEndpoint
)

$agent = Join-Path $InstallDirectory "agent.exe"
if (-not (Test-Path $agent)) { throw "agent.exe not found at $agent" }
$executor = Join-Path $InstallDirectory "task-executor.exe"
if (-not (Test-Path $executor)) { throw "task-executor.exe not found at $executor" }
if ($AgentId -notmatch '^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,62}[A-Za-z0-9])?$') {
  throw "AgentId must be 1-64 URL-safe ASCII characters and start/end with a letter or digit"
}

$tlsValues = @($TlsCa, $TlsCert, $TlsKey) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
if ($tlsValues.Count -ne 0 -and $tlsValues.Count -ne 3) {
  throw "TlsCa, TlsCert, and TlsKey must be supplied together"
}

New-Item -ItemType Directory -Path $DataDirectory -Force | Out-Null
$databasePath = Join-Path $DataDirectory "agent.db"
$databaseUrl = "sqlite://" + ($databasePath -replace '\\', '/')

function Quote-TaskArgument([string]$Value) {
  if ($Value.Contains('"')) { throw "Scheduled-task arguments cannot contain a double quote" }
  return '"' + $Value + '"'
}

$arguments = [System.Collections.Generic.List[string]]::new()
$arguments.Add("--agent-id")
$arguments.Add((Quote-TaskArgument $AgentId))
$arguments.Add("--coordinator-url")
$arguments.Add((Quote-TaskArgument $CoordinatorUrl))
$arguments.Add("--database-url")
$arguments.Add((Quote-TaskArgument $databaseUrl))
$arguments.Add("--ui-addr")
$arguments.Add((Quote-TaskArgument $UiAddress))
$arguments.Add("--executor-path")
$arguments.Add((Quote-TaskArgument $executor))
$arguments.Add("--capacity")
$arguments.Add($Capacity.ToString([System.Globalization.CultureInfo]::InvariantCulture))
foreach ($label in $Labels) {
  if ($label -notmatch '^[^=\s]+=[^\s]+$') { throw "Labels must use key=value syntax without whitespace" }
  $arguments.Add("--label")
  $arguments.Add((Quote-TaskArgument $label))
}
if ($tlsValues.Count -eq 3) {
  foreach ($path in @($TlsCa, $TlsCert, $TlsKey)) {
    if (-not (Test-Path $path -PathType Leaf)) { throw "TLS file not found: $path" }
  }
  $arguments.Add("--tls-ca")
  $arguments.Add((Quote-TaskArgument $TlsCa))
  $arguments.Add("--tls-cert")
  $arguments.Add((Quote-TaskArgument $TlsCert))
  $arguments.Add("--tls-key")
  $arguments.Add((Quote-TaskArgument $TlsKey))
}
if (-not [string]::IsNullOrWhiteSpace($TlsDomain)) {
  $arguments.Add("--tls-domain")
  $arguments.Add((Quote-TaskArgument $TlsDomain))
}
if (-not [string]::IsNullOrWhiteSpace($OtlpEndpoint)) {
  $arguments.Add("--otlp-endpoint")
  $arguments.Add((Quote-TaskArgument $OtlpEndpoint))
}

$action = New-ScheduledTaskAction -Execute $agent -Argument ($arguments -join ' ') -WorkingDirectory $InstallDirectory
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask -TaskName "RustTaskSchedulerAgent" -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force

Write-Host "Agent registered for the current interactive user. Ledger: $databasePath"
Write-Host "Excel automation must not be changed to a Windows-service logon type."
