param(
  [Parameter(Mandatory=$true)][string]$Distribution,
  [Parameter(Mandatory=$true)][string]$RepositoryPath,
  [Parameter(Mandatory=$true)][string]$EnvironmentFile,
  [Parameter(Mandatory=$true)][string]$NodePath,
  [string]$TaskName = 'Transit daily processing'
)
$ErrorActionPreference = 'Stop'
# Arguments go directly to wsl.exe/env/node, never through an interpolated shell.
function Quote-Argument([string]$Value) {
  if ($Value.Contains('"') -or $Value.Contains("`r") -or $Value.Contains("`n")) { throw 'Arguments must not contain quotes or line breaks' }
  return '"' + $Value + '"'
}
$TaskArguments = @('--distribution', (Quote-Argument $Distribution), '--cd', (Quote-Argument $RepositoryPath),
  '--exec', 'env', (Quote-Argument ('DOTENV_CONFIG_PATH=' + $EnvironmentFile)),
  (Quote-Argument $NodePath), '--import', 'tsx', 'scripts/process-daily.ts') -join ' '
$TaskAction = New-ScheduledTaskAction -Execute 'wsl.exe' -Argument $TaskArguments
$TaskTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 15)
$TaskSettings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 24)
Register-ScheduledTask -TaskName $TaskName -Action $TaskAction -Trigger $TaskTrigger -Settings $TaskSettings -Description 'Either workstation may claim the daily server job; collection stays on the server.'
