@echo off
:: ==============================================================================
:: PROYECTO: Encuesta Percepcion - Automatizacion T-1
:: OBJETIVO: Calcular mes anterior y ejecutar motor Python con Log Externo
:: SERVIDOR: SRVCBQABI (10.10.10.98)
:: ==============================================================================

setlocal

:: 1. Calculo dinamico de Periodo T-1 usando PowerShell
:: Retorna el año y mes del mes calendario anterior (ej. si es marzo, retorna febrero)
for /f "tokens=1,2" %%A in ('powershell -command "((Get-Date).AddMonths(-1)).ToString('yyyy MM')"') do (
    set ANIO_PROC=%%A
    set MES_PROC=%%B
)

:: 2. Definicion de Rutas Institucionales
set PYTHON_EXE="C:\ProgramData\Anaconda3\envs\base\python.exe"
set SCRIPT_PY="F:\ETL_DITIC\DWHencuestaPercepcion\main.py"
set LOG_FILE="F:\ETL_DITIC\DWHencuestaPercepcion\Logs\Encuesta_Percepcion_Job_Output.txt"

echo [%DATE% %TIME%] >>> INICIANDO: Encuesta Percepcion para %MES_PROC%/%ANIO_PROC% >> %LOG_FILE%

:: 3. Ejecucion del Motor Python para todas las areas
:: Se redirige la salida estandar (1) y los errores (2) al archivo de Log
%PYTHON_EXE% %SCRIPT_PY% --anio %ANIO_PROC% --mes %MES_PROC% >> %LOG_FILE% 2>&1

echo [%DATE% %TIME%] >>> FINALIZADO: Ciclo de ejecucion completado. >> %LOG_FILE%
echo -------------------------------------------------------------------------- >> %LOG_FILE%

endlocal