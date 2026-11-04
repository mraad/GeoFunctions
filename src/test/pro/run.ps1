# Run the ArcGIS Pro tests on a machine with Pro and a spark_esri conda env. Nothing is installed:
# the release zip is unpacked into a temp folder, and that folder is removed at the end.
#
#   powershell -ExecutionPolicy Bypass -File <repo>\src\test\pro\run.ps1 -Version 0.40 [-Mode pro] [-Shp <gps.shp folder>]
#
# -Mode pip uses the env's pip PySpark. -Mode pro uses the Spark that ships with Pro.
# Run ./gf.sh first: this reads release\geofunctions-<Version>.zip.
# Exits 1 unless both scripts finish with no failed test.
param([Parameter(Mandatory)][string]$Version, [string]$Mode = 'pip', [string]$Shp = '', [string]$Jar = '')
$repo = (Resolve-Path "$PSScriptRoot\..\..\..").ProviderPath
if (-not $Jar) { $Jar = "geofunctions-$Version-2.13.jar" }
$t = Join-Path $env:TEMP "gf-pro-$PID"
$failed = $false
New-Item -ItemType Directory $t | Out-Null
try {
  Copy-Item "$repo\release\geofunctions-$Version.zip" "$t\bundle.zip"
  Expand-Archive "$t\bundle.zip" "$t\bundle"
  Copy-Item "$t\bundle\geofunctions-$Version-py3-none-any.whl" "$t\wheel.zip"
  Expand-Archive "$t\wheel.zip" "$t\wheel"
  Copy-Item -Recurse "$repo\src\test\resources\Shapes.gdb" "$t\bundle\Shapes.gdb"
  Copy-Item "$repo\src\test\resources\make_shapes_gdb.py","$repo\src\test\pro\pro_smoke.py","$repo\src\test\pro\pro_arcpy.py" $t
  if ($Shp) { Copy-Item "$Shp\gps.*" "$t\bundle" }

  $py = Join-Path $env:LOCALAPPDATA 'ESRI\conda\envs\spark_esri\python.exe'
  $env:SPARK_ESRI_NO_WARN = '1'
  $env:PYTHONPATH = "$t\wheel"
  if ($Mode -eq 'pro') {
    $env:SPARK_HOME = 'C:\Program Files\ArcGIS\Pro\Java\runtime\spark'
    $env:PYTHONPATH = "$t\wheel;" + ((Get-ChildItem "$env:SPARK_HOME\python\lib\*.zip").FullName -join ';')
  }
  foreach ($test in 'pro_smoke', 'pro_arcpy') {
    "=== $test ($Mode)"
    $log = "$t\$test.log"
    & $py "$t\$test.py" "$t\bundle" $Jar "$t\work-$test" 2>&1 | ForEach-Object { "$_" } | Set-Content $log
    Select-String -Path $log -Pattern '^(FAIL|NOTE|SKIP|SUMMARY|spark |arcpy )' | ForEach-Object { $_.Line }
    "PASS lines: " + @(Select-String -Path $log -Pattern '^PASS').Count
    # The scripts exit 0 even with failed tests, and a crash never reaches SUMMARY.
    if (-not (Select-String -Path $log -Pattern '^SUMMARY failed=0 ' -Quiet)) {
      "FAILED ${test}: end of its log:"
      Get-Content $log -Tail 20
      $failed = $true
    }
  }
} finally {
  Remove-Item -Recurse -Force $t -ErrorAction SilentlyContinue
}
if ($failed) { exit 1 }
