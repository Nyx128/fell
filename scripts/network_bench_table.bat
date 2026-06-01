@echo off
setlocal enabledelayedexpansion

set BENCH=C:\dev\fell\build-release\bench\sys\bench-network.exe
set OPS=10000
set PAYLOAD=256
set KEY=benchmark-key

echo.
echo ============================================
echo Machine Information
echo ============================================

wmic cpu get Name
wmic cpu get NumberOfCores,NumberOfLogicalProcessors
wmic computersystem get TotalPhysicalMemory

echo.
echo L1/L2/L3 Cache:
wmic cpu get L2CacheSize,L3CacheSize

echo ============================================
echo.

echo Pipeline sweep V1 (1 thread)
for %%P in (1 4 8 16 32 64 128) do (
    set /p "=pipeline=%%P  " <nul
    for /f "tokens=2" %%T in ('!BENCH! --ops %OPS% --threads 1 --payload-size %PAYLOAD% --pipeline %%P ^| findstr "Throughput"') do (
        echo %%T ops/sec
    )
)

echo.
echo Pipeline sweep V2 (1 thread, keyed)
for %%P in (1 4 8 16 32 64 128) do (
    set /p "=pipeline=%%P  " <nul
    for /f "tokens=2" %%T in ('!BENCH! --ops %OPS% --threads 1 --payload-size %PAYLOAD% --pipeline %%P --key %KEY% ^| findstr "Throughput"') do (
        echo %%T ops/sec
    )
)

echo.
echo Thread sweep V1 (pipeline=32)
for %%T in (1 2 4 8 16) do (
    set /p "=threads=%%T  " <nul
    for /f "tokens=2" %%R in ('!BENCH! --ops %OPS% --threads %%T --payload-size %PAYLOAD% --pipeline 32 ^| findstr "Throughput"') do (
        echo %%R ops/sec
    )
)

echo.
echo Thread sweep V2 (pipeline=32, keyed)
for %%T in (1 2 4 8 16) do (
    set /p "=threads=%%T  " <nul
    for /f "tokens=2" %%R in ('!BENCH! --ops %OPS% --threads %%T --payload-size %PAYLOAD% --pipeline 32 --key %KEY% ^| findstr "Throughput"') do (
        echo %%R ops/sec
    )
)

echo.
echo Payload sweep V1 (pipeline=32, threads=4)
for %%S in (64 256 1024 8192 65536) do (
    set /p "=payload=%%S  " <nul
    for /f "tokens=2" %%R in ('!BENCH! --ops %OPS% --threads 4 --payload-size %%S --pipeline 32 ^| findstr "Throughput"') do (
        echo %%R ops/sec
    )
)

echo.
echo Payload sweep V2 (pipeline=32, threads=4, keyed)
for %%S in (64 256 1024 8192 65536) do (
    set /p "=payload=%%S  " <nul
    for /f "tokens=2" %%R in ('!BENCH! --ops %OPS% --threads 4 --payload-size %%S --pipeline 32 --key %KEY% ^| findstr "Throughput"') do (
        echo %%R ops/sec
    )
)

endlocal