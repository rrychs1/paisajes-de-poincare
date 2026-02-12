param(
    [string]$Source = "C:\Users\rrych\Documents\Tercer API Binance",
    [string]$Repo = "C:\Users\rrych\Documents\paisajes-de-poincare",
    [string]$CommitMessage = "Sync bot updates"
)

$excludeDirs = @(
    ".venv",
    ".idea",
    "__pycache__",
    ".git"
)

$excludeFiles = @(
    ".env",
    "bot_state.db",
    "bot_state.db-wal",
    "bot_state.db-shm",
    "bot.log"
)

$robocopyArgs = @($Source, $Repo, "/E", "/XD") + $excludeDirs + @("/XF") + $excludeFiles
robocopy @robocopyArgs | Out-Host
if ($LASTEXITCODE -ge 8) {
    throw "Robocopy failed with exit code $LASTEXITCODE"
}

Push-Location $Repo
try {
    git add -A
    git diff --cached --quiet
    if ($LASTEXITCODE -eq 0) {
        Write-Host "No changes to commit."
        exit 0
    }
    git commit -m $CommitMessage
    git pull --rebase
    git push
} finally {
    Pop-Location
}
