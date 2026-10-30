# Real MSVC/vccexe file-argument control. No compiler or filesystem is mocked.
# A short absolute C file must remain usable from a valid deep caller directory.
param(
    [Parameter(Mandatory = $true)][string]$EvidenceRoot,
    [Parameter(Mandatory = $true)][string]$TargetTriple
)
$ErrorActionPreference = 'Stop'
# Use the actual target already selected by the native CRT driver, including
# an ARM-labelled runner whose installed Rust toolchain is in fact x64.
$platform = switch -Regex ($TargetTriple) {
    '^x86_64-.*-windows-msvc$' { 'amd64'; break }
    '^aarch64-.*-windows-msvc$' { 'x64_arm64'; break }
    '^i[3-6]86-.*-windows-msvc$' { 'x86'; break }
    default { throw "No existing vccexe platform mapping for selected target $TargetTriple" }
}
$evidence = Join-Path $EvidenceRoot 'source-paths'
if ([IO.Directory]::Exists($evidence)) { throw 'source-path evidence already exists' }
[IO.Directory]::CreateDirectory($evidence) | Out-Null
$identity = [Guid]::NewGuid().ToString('N')
$scratch = Join-Path ([IO.Path]::GetTempPath()) "vcc-path-$identity"
if ([IO.Directory]::Exists($scratch)) { throw 'source-path scratch already exists' }
[IO.Directory]::CreateDirectory($scratch) | Out-Null
$receipt = @{ complete = $false; target = $TargetTriple; compiler_platform = $platform; cases = @() }
try {
    $wrapper = (Get-Command vccexe.exe -CommandType Application -ErrorAction Stop | Select-Object -First 1).Source
    $nim = (Get-Command nim.exe -CommandType Application -ErrorAction Stop | Select-Object -First 1).Source
    $receipt.wrapper = @{ path = $wrapper; sha256 = (Get-FileHash -LiteralPath $wrapper -Algorithm SHA256).Hash }
    $receipt.nim = @{ path = $nim; sha256 = (Get-FileHash -LiteralPath $nim -Algorithm SHA256).Hash }
    $source = Join-Path $scratch '@mcodetracer_trace_writer@sstreaming_value_encoder.nim.c'
    [IO.File]::WriteAllText($source, 'int path_control(void) { return 37; }', [Text.UTF8Encoding]::new($false))
    $sourceHash = (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash
    $caller = Join-Path $scratch 'caller'
    while ($caller.Length -lt 210) { $caller = Join-Path $caller 'deep-caller' }
    if ($source.Length -ge 240 -or $caller.Length -ge 240) { throw 'fixture itself exceeds ordinary Windows path limits' }
    [IO.Directory]::CreateDirectory($caller) | Out-Null
    [IO.File]::WriteAllText((Join-Path $caller 'cwd-marker'), $identity)
    [IO.File]::WriteAllText((Join-Path $caller 'relative.c'), 'int relative_control(void) { return 41; }')
    $relative = [IO.Path]::GetRelativePath($caller, $source)
    if (($caller.Length + 1 + $relative.Length) -lt 260) { throw 'deep relative spelling did not cross the intended boundary' }
    $receipt.source = @{ path = $source; exists_before = [IO.File]::Exists($source); sha256 = $sourceHash }
    $receipt.caller = $caller
    $receipt.relative = $relative
    $receipt.units = @{ absolute = $source.Length; relative = $relative.Length; caller = $caller.Length; unreduced = $caller.Length + 1 + $relative.Length }

    function Invoke-PathControl([string]$Name, [string[]]$Arguments) {
        $start = [Diagnostics.ProcessStartInfo]::new($wrapper)
        $start.WorkingDirectory = $caller
        $start.UseShellExecute = $false
        $start.RedirectStandardOutput = $true
        $start.RedirectStandardError = $true
        foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
        $start.Environment['CT_PATH_SOURCE'] = $source
        $start.Environment['CT_PATH_OBJECT'] = Join-Path $scratch "$Name.obj"
        $start.Environment['CT_PATH_OBSERVATION'] = Join-Path $evidence 'direct-observation.json'
        $start.Environment['CT_PATH_MARKER'] = $identity
        $process = [Diagnostics.Process]::new()
        $process.StartInfo = $start
        if (-not $process.Start()) { throw 'vccexe did not start' }
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        $process.WaitForExit()
        [IO.File]::WriteAllText((Join-Path $evidence "$Name.stdout"), $stdout.GetAwaiter().GetResult())
        [IO.File]::WriteAllText((Join-Path $evidence "$Name.stderr"), $stderr.GetAwaiter().GetResult())
        return @{ name = $Name; exit = $process.ExitCode; object = [IO.File]::Exists((Join-Path $scratch "$Name.obj")) }
    }

    # The wrapper's documented --command interface supplies the SAME MSVC
    # environment. Pass the source through this child's environment so vccexe
    # cannot rewrite it before the real cl.exe absolute-path control sees it.
    # This is fixture observation, never a production compilation workaround.
    $direct = @'
$ErrorActionPreference = 'Stop'
$compiler = (Get-Command cl.exe -CommandType Application -ErrorAction Stop | Select-Object -First 1).Source
$cwd = [IO.Directory]::GetCurrentDirectory()
$marker = [IO.File]::ReadAllText((Join-Path $cwd 'cwd-marker'))
$observation = @{
    cwd = $cwd
    marker_matches = ($marker -ceq $env:CT_PATH_MARKER)
    source_exists = [IO.File]::Exists($env:CT_PATH_SOURCE)
    compiler = $compiler
    compiler_sha256 = (Get-FileHash -LiteralPath $compiler -Algorithm SHA256).Hash
}
$observation | ConvertTo-Json | Set-Content -LiteralPath $env:CT_PATH_OBSERVATION -Encoding utf8
if (-not $observation.marker_matches -or -not $observation.source_exists) { throw 'actual child cwd/source control failed' }
$arguments = @('/nologo', '/c', ('/Fo' + $env:CT_PATH_OBJECT), $env:CT_PATH_SOURCE)
& $compiler @arguments
exit $LASTEXITCODE
'@
    $shellImage = [Environment]::ProcessPath
    $arguments = @("--platform:$platform", "--command:$shellImage", '-NoLogo', '-NoProfile', '-NonInteractive', '-Command', $direct)
    $directResult = Invoke-PathControl 'direct' $arguments
    $receipt.cases += $directResult

    $arguments = @("--platform:$platform", '/nologo', '/c', ('/Fo' + (Join-Path $scratch 'absolute.obj')), $source)
    $absoluteResult = Invoke-PathControl 'absolute' $arguments
    $receipt.cases += $absoluteResult
    $arguments = @("--platform:$platform", '/nologo', '/c', ('/Fo' + (Join-Path $scratch 'relative.obj')), 'relative.c')
    $relativeResult = Invoke-PathControl 'relative' $arguments
    $receipt.cases += $relativeResult
    $arguments = @("--platform:$platform", '/nologo', '/c', ('/Fo' + (Join-Path $scratch 'missing.obj')), (Join-Path $scratch 'missing.c'))
    $missingResult = Invoke-PathControl 'missing' $arguments
    $receipt.cases += $missingResult
    $receipt.source.exists_after = [IO.File]::Exists($source)
    $receipt.source.hash_unchanged = ((Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash -ceq $sourceHash)
    if ($directResult.exit -ne 0 -or -not $directResult.object) { throw 'same-MSVC absolute compile control failed' }
    if ($relativeResult.exit -ne 0 -or -not $relativeResult.object) { throw 'ordinary relative input failed' }
    if ($missingResult.exit -eq 0 -or $missingResult.object) { throw 'missing-file negative control falsely succeeded' }
    if (-not $receipt.source.exists_after -or -not $receipt.source.hash_unchanged) { throw 'source changed during compilation' }
    if ($absoluteResult.exit -ne 0 -or -not $absoluteResult.object) { throw 'vccexe rejected the existing short absolute source from a valid deep cwd' }
    $receipt.complete = $true
    Write-Host 'PASS real MSVC absolute/deep-cwd, relative and missing-file controls'
} finally {
    $receipt | ConvertTo-Json -Depth 7 | Set-Content -LiteralPath (Join-Path $evidence 'receipt.json') -Encoding utf8
    # Only files created by this invocation; all evidence is retained above.
    try { Remove-Item -LiteralPath $scratch -Recurse -Force -ErrorAction Stop }
    catch { Write-Warning "Could not remove owned source-path scratch $scratch; evidence remains at $evidence" }
}
