# Build lại nội dung rồi đồng bộ lên bucket S3 static website, một lệnh, chạy trên Windows.
#   .\scripts\deploy-s3.ps1            # build + sync (xoá trên S3 file đã xoá ở repo)
#   .\scripts\deploy-s3.ps1 -DryRun    # chỉ in ra sẽ upload/xoá gì
#   .\scripts\deploy-s3.ps1 -NoBuild   # bỏ bước build
# Nếu PowerShell chặn script: powershell -ExecutionPolicy Bypass -File .\scripts\deploy-s3.ps1
# Cấu hình (không chứa secret) ở deploy\s3.env, do scripts/wizard-s3-portal.sh tạo.
# Credential ở %USERPROFILE%\.aws (profile trong S3_PROFILE), không nằm trong repo.
param([switch]$DryRun, [switch]$NoBuild)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)

if (-not (Test-Path 'deploy\s3.env')) { throw 'Thiếu deploy\s3.env: chạy wizard trước (Git Bash: bash scripts/wizard-s3-portal.sh).' }
$cfg = @{}
foreach ($line in Get-Content 'deploy\s3.env') {
  if ($line -match "^\s*([A-Z0-9_]+)='(.*)'\s*$") { $cfg[$Matches[1]] = $Matches[2] -replace "'\\''", "'" }
}
foreach ($k in 'S3_BUCKET', 'S3_REGION', 'S3_PROFILE') { if (-not $cfg[$k]) { throw "Thiếu $k trong deploy\s3.env" } }
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
  $msi = 'C:\Program Files\Amazon\AWSCLIV2'
  if (Test-Path "$msi\aws.exe") { $env:PATH += ";$msi" } else { throw 'Chưa có AWS CLI: winget install -e --id Amazon.AWSCLI' }
}

if (-not $NoBuild) {
  $env:PYTHONIOENCODING = 'utf-8'
  python scripts/html2md.py | Out-Null
  if ($LASTEXITCODE) { throw 'html2md.py lỗi' }
  node scripts/build_portal.mjs
  if ($LASTEXITCODE) { throw 'build_portal.mjs lỗi' }
}

$syncArgs = @('s3', 'sync', '.', "s3://$($cfg.S3_BUCKET)",
  '--profile', $cfg.S3_PROFILE, '--region', $cfg.S3_REGION, '--delete',
  '--exclude', '.git/*', '--exclude', '.claude/*', '--exclude', '.agents/*', '--exclude', '.vscode/*',
  '--exclude', 'dist/*', '--exclude', 'deploy/*', '--exclude', '*/target/*', '--exclude', '*.local.json',
  '--cache-control', 'max-age=300')
if ($DryRun) { $syncArgs += '--dryrun' }
& aws @syncArgs
if ($LASTEXITCODE) { throw "aws s3 sync lỗi (mã $LASTEXITCODE)" }

# Endpoint website: một số region cũ dùng dấu gạch, còn lại dấu chấm (AWS General Reference, bảng S3 website endpoints)
$dash = 'us-east-1', 'us-west-1', 'us-west-2', 'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1', 'eu-west-1', 'sa-east-1', 'us-gov-west-1'
if ($dash -contains $cfg.S3_REGION) { $sep = '-' } else { $sep = '.' }
Write-Host ""
Write-Host "Website: http://$($cfg.S3_BUCKET).s3-website$sep$($cfg.S3_REGION).amazonaws.com/"
