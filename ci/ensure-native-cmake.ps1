param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("arm64", "x64")]
  [string]$Architecture,
  [Parameter(Mandatory = $true)]
  [string]$Destination
)

$ErrorActionPreference = "Stop"
$version = "3.31.8"
$pins = @{
  arm64 = @{
    asset = "cmake-3.31.8-windows-arm64.zip"
    sha256 = "d37ca89ec60aece080664ee936f7aaffdfdbde0a201b165c45ecfaf68a4c1fa9"
    machine = 0xaa64
    operatingSystemArchitecture = "Arm64"
  }
  x64 = @{
    asset = "cmake-3.31.8-windows-x86_64.zip"
    sha256 = "81aa9964dbabd71fe02e7ec50472fd3ad56138c49944515ece9001efbff8d719"
    machine = 0x8664
    operatingSystemArchitecture = "X64"
  }
}
$pin = $pins[$Architecture]
if (-not $IsWindows -or [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString() -ne $pin.operatingSystemArchitecture) {
  throw "Pinned CMake architecture differs from the native Windows host."
}
$parent = [System.IO.Path]::GetFullPath($env:RUNNER_TEMP)
$root = [System.IO.Path]::GetFullPath($Destination)
if ([System.IO.Path]::GetDirectoryName($root) -ne $parent) {
  throw "CMake extraction must be a direct child of the declared runner temp directory."
}
$parentItem = Get-Item -LiteralPath $parent -Force
if ($parentItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
  throw "CMake extraction parent is a reparse point."
}
if ($null -ne (Get-Item -LiteralPath $root -Force -ErrorAction SilentlyContinue)) {
  throw "Occupied CMake extraction destination."
}
New-Item -ItemType Directory -Path $root -ErrorAction Stop | Out-Null
$rootItem = Get-Item -LiteralPath $root -Force
$rootCreation = $rootItem.CreationTimeUtc
$archive = Join-Path $root "source.zip"
$url = "https://github.com/Kitware/CMake/releases/download/v$version/$($pin.asset)"
Invoke-WebRequest -Uri $url -OutFile $archive
if ((Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant() -ne $pin.sha256) {
  throw "Pinned CMake archive checksum differs."
}
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [System.IO.Compression.ZipFile]::OpenRead($archive)
$packageName = $pin.asset.Substring(0, $pin.asset.Length - 4)
$rootPrefix = $root + [System.IO.Path]::DirectorySeparatorChar
$seen = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
$expandedBytes = 0L
try {
  foreach ($entry in $zip.Entries) {
    $name = $entry.FullName.Replace("\", "/")
    if (-not $name.StartsWith("$packageName/", [System.StringComparison]::Ordinal) -or $name.Split("/") -contains ".." -or $name -match "[:\x00]") {
      throw "Foreign CMake archive member path."
    }
    $target = [System.IO.Path]::GetFullPath((Join-Path $root $name))
    if (-not $target.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase) -or -not $seen.Add($target.TrimEnd([System.IO.Path]::DirectorySeparatorChar))) {
      throw "Aliased CMake archive member path."
    }
    $kind = ($entry.ExternalAttributes -shr 16) -band 0xf000
    if ($kind -notin @(0, 0x8000, 0x4000)) {
      throw "Nonregular CMake archive member."
    }
    $expandedBytes += $entry.Length
    if ($expandedBytes -gt 512MB) {
      throw "CMake archive exceeds its finite extraction allowance."
    }
  }
} finally {
  $zip.Dispose()
}
Expand-Archive -LiteralPath $archive -DestinationPath $root
$after = Get-Item -LiteralPath $root -Force
if ($after.CreationTimeUtc -ne $rootCreation -or $after.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
  throw "Owned CMake extraction directory changed."
}
$executable = Join-Path $root "$packageName\bin\cmake.exe"
$item = Get-Item -LiteralPath $executable -Force
if ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
  throw "Pinned CMake executable is a reparse point."
}
$bytes = [System.IO.File]::ReadAllBytes($executable)
if ($bytes.Length -lt 64 -or $bytes[0] -ne 0x4d -or $bytes[1] -ne 0x5a) {
  throw "Pinned CMake executable has no DOS header."
}
$offset = [System.BitConverter]::ToUInt32($bytes, 0x3c)
if ($offset -gt $bytes.Length - 24 -or [System.BitConverter]::ToUInt32($bytes, $offset) -ne 0x00004550 -or [System.BitConverter]::ToUInt16($bytes, $offset + 4) -ne $pin.machine) {
  throw "Pinned CMake executable machine differs."
}
$output = @(& $executable --version)
if ($LASTEXITCODE -ne 0 -or $output[0] -ne "cmake version $version") {
  throw "Pinned CMake executable version differs."
}
$bin = [System.IO.Path]::GetDirectoryName($executable)
Add-Content -LiteralPath $env:GITHUB_PATH -Value $bin
Write-Host "Pinned native CMake $version selected; archive SHA256 $($pin.sha256)."
