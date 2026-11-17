# Real Cargo/Nim/MSVC archives, linked and executed with both host CRT modes.
# No compiler/provider is mocked and no SDK or source bootstrap is performed.
# Existing RUSTFLAGS are preserved; only each child's CRT feature is selected.
$ErrorActionPreference = 'Stop'
function Add-CrtRuntimeFeature([Diagnostics.ProcessStartInfo]$child, [string]$feature) {
    $argument = "-Ctarget-feature=$feature"
    if ($child.Environment.ContainsKey('CARGO_ENCODED_RUSTFLAGS')) {
        $previous = $child.Environment['CARGO_ENCODED_RUSTFLAGS']
        $child.Environment['CARGO_ENCODED_RUSTFLAGS'] = if ([string]::IsNullOrEmpty($previous)) {
            $argument
        } else { $previous + ([string][char]31) + $argument }
    } else {
        $child.Environment['RUSTFLAGS'] = "$($child.Environment['RUSTFLAGS']) $argument"
    }
}
$crtRepo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$rustDetails = @(& rustc -vV)
if ($LASTEXITCODE -ne 0) { throw 'rustc identity query failed' }
$rustHostTriple = ($rustDetails | Where-Object { $_ -like 'host: *' }) -replace '^host: ', ''
if ($rustHostTriple -notmatch '-windows-msvc$') { throw "MSVC native host required, observed $rustHostTriple" }
if ($env:CODETRACER_TRACE_WRITER_NIM_PREBUILT) { throw 'This check requires newly source-built archives, not a prebuilt prefix' }
$crtBase = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [IO.Path]::GetTempPath() }
$crtId = [Guid]::NewGuid().ToString('N')
$crtEvidence = Join-Path $crtBase "ctnim-crt-evidence-$crtId"
$crtScratch = Join-Path $crtBase "ctnim-crt-$crtId"
if ([IO.Directory]::Exists($crtEvidence) -or [IO.Directory]::Exists($crtScratch)) { throw 'fresh test directory already exists' }
[IO.Directory]::CreateDirectory($crtEvidence) | Out-Null
[IO.Directory]::CreateDirectory($crtScratch) | Out-Null
if ($env:GITHUB_OUTPUT) { [IO.File]::AppendAllText($env:GITHUB_OUTPUT, "evidence_dir=$crtEvidence`n") }
$crtReceipt = @{ rust = $rustDetails; source = @(& git -C $crtRepo rev-parse HEAD); modes = @(); complete = $false }
try {
    foreach ($crtCase in @(
        @{ name = 'dll'; feature = '-crt-static'; argument = '--passC:/MD' },
        @{ name = 'static'; feature = '+crt-static'; argument = '--passC:/MT' }
    )) {
        $crtStart = [Diagnostics.ProcessStartInfo]::new('cargo')
        $crtStart.WorkingDirectory = $crtRepo
        $crtStart.UseShellExecute = $false
        $crtStart.RedirectStandardOutput = $true
        $crtStart.RedirectStandardError = $true
        foreach ($crtArg in @('test', '--locked', '-p', 'codetracer_trace_writer_nim',
            '--test', 'the_archive_is_the_librarys_own_build', '--test', 'msvc_runtime',
            '--target', $rustHostTriple, '--target-dir', (Join-Path $crtScratch $crtCase.name),
            '--message-format=json', '--', '--nocapture', '--color', 'never')) {
            $crtStart.ArgumentList.Add($crtArg)
        }
        # Cargo gives the encoded form precedence. Append to whichever form
        # the caller actually uses without logging or replacing its other flags.
        Add-CrtRuntimeFeature $crtStart $crtCase.feature
        $crtProcess = [Diagnostics.Process]::new()
        $crtProcess.StartInfo = $crtStart
        if (-not $crtProcess.Start()) { throw 'Cargo did not start' }
        $crtStdoutTask = $crtProcess.StandardOutput.ReadToEndAsync()
        $crtStderrTask = $crtProcess.StandardError.ReadToEndAsync()
        $crtProcess.WaitForExit()
        $crtStdout = $crtStdoutTask.GetAwaiter().GetResult()
        $crtStderr = $crtStderrTask.GetAwaiter().GetResult()
        [IO.File]::WriteAllText((Join-Path $crtEvidence "$($crtCase.name).stdout"), $crtStdout)
        [IO.File]::WriteAllText((Join-Path $crtEvidence "$($crtCase.name).stderr"), $crtStderr)
        if ($crtProcess.ExitCode -ne 0) { throw "$($crtCase.name) real Cargo link/execution failed ($($crtProcess.ExitCode)); retained $crtEvidence" }
        # Require every intended test, not merely a successful empty harness.
        foreach ($crtTest in @(
            'the_linked_archive_reports_the_library_build_configuration',
            'the_archive_links_with_the_hosts_dictionary_builder',
            'default_msvc_context_uses_the_dll_runtime',
            'static_runtime_is_an_exact_feature_token',
            'other_abis_do_not_receive_msvc_flags',
            'runtime_modes_do_not_share_native_cache_identity'
        )) {
            if ($crtStdout -notmatch ('(?m)^test ' + [regex]::Escape($crtTest) + ' \.\.\. ok\r?$')) {
                throw "required native test did not pass: $crtTest"
            }
        }
        $crtArchives = @()
        foreach ($crtLine in ($crtStdout -split "`n")) {
            if (-not $crtLine.StartsWith('{')) { continue }
            $crtMessage = ConvertFrom-Json -InputObject $crtLine
            if ($crtMessage.reason -ne 'build-script-executed') { continue }
            $crtArchive = Join-Path $crtMessage.out_dir 'codetracer_trace_writer.lib'
            if ([IO.File]::Exists($crtArchive)) { $crtArchives += $crtArchive }
        }
        $crtArchives = @($crtArchives | Select-Object -Unique)
        if ($crtArchives.Count -ne 1) { throw "expected exactly one real Nim archive, observed $($crtArchives.Count)" }
        $crtArchive = $crtArchives[0]
        $crtOutput = [IO.File]::ReadAllText((Join-Path (Split-Path (Split-Path $crtArchive)) 'output'))
        if (-not $crtOutput.Contains($crtCase.argument)) { throw 'actual producer invocation omitted the host runtime argument' }
        $crtCacheMatch = [regex]::Match($crtOutput, '(?:"--nimcache:(?<cache>[^"]+)"|''--nimcache:(?<cache>[^'']+)''|--nimcache:(?<cache>\S+))')
        if (-not $crtCacheMatch.Success) { throw 'actual Nim command has no cache path' }
        $crtCache = $crtCacheMatch.Groups['cache'].Value
        $crtObjects = @([IO.Directory]::EnumerateFiles($crtCache, '*.obj'))
        if ($crtObjects.Count -eq 0) { throw 'no real MSVC objects were produced' }
        $crtReceipt.modes += @{
            name = $crtCase.name; argument = $crtCase.argument; archive = $crtArchive
            sha256 = (Get-FileHash -LiteralPath $crtArchive -Algorithm SHA256).Hash.ToLowerInvariant()
            cache = $crtCache; object_count = $crtObjects.Count
        }
        $crtReceipt | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $crtEvidence 'receipt.json') -Encoding utf8
        Write-Host "PASS $($crtCase.name): real Nim archive, dictionary-builder link/roundtrip and context controls"
    }
    if ($crtReceipt.modes[0].cache -eq $crtReceipt.modes[1].cache) { throw 'different CRT objects shared one native cache' }
    $crtReceipt.complete = $true
    Write-Host "PASS both MSVC CRT modes; evidence $crtEvidence"
} finally {
    $crtReceipt | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $crtEvidence 'receipt.json') -Encoding utf8
    # Only this invocation's fresh private Cargo output, never a shared cache.
    try { Remove-Item -LiteralPath $crtScratch -Recurse -Force -ErrorAction Stop }
    catch { Write-Warning "Could not remove owned test scratch $crtScratch; evidence remains at $crtEvidence" }
}
