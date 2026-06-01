@echo off
setlocal

REM ============================================
REM Configure Debug build
REM ============================================

echo Configuring Debug build...

cmake -B build-debug -G Ninja ^
    -DCMAKE_BUILD_TYPE=Debug ^
    -DCMAKE_CXX_COMPILER=clang++ ^
    -DCMAKE_C_COMPILER=clang

if %errorlevel% neq 0 (
    echo Debug configuration failed.
    exit /b %errorlevel%
)

REM ============================================
REM Configure Release build
REM ============================================

echo.
echo Configuring Release build...

cmake -B build-release -G Ninja ^
    -DCMAKE_BUILD_TYPE=Release ^
    -DCMAKE_CXX_COMPILER=clang++ ^
    -DCMAKE_C_COMPILER=clang

if %errorlevel% neq 0 (
    echo Release configuration failed.
    exit /b %errorlevel%
)

echo.
echo Done.