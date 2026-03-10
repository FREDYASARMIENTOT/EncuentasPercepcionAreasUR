@echo off
SETLOCAL EnableDelayedExpansion
TITLE Orquestador Encuesta Percepcion - DITIC

:: ==========================================================================
:: 🛡️ MAGIA ANTI-ERRORES DE EMOJIS (Fuerza a Windows y Python a usar UTF-8)
:: ==========================================================================
chcp 65001 > nul
set PYTHONIOENCODING=utf-8

:: 1. REPARAR EL PATH
set "PATH=%SystemRoot%\system32;%SystemRoot%;%SystemRoot%\System32\Wbem;%SystemRoot%\System32\WindowsPowerShell\v1.0\;%PATH%"

:: 2. GENERAR TIMESTAMP
set "TEMP_DATE=%DATE:/=-%"
set "TEMP_TIME=%TIME: =0%"
set "TEMP_TIME=%TEMP_TIME::=-%"
set "TEMP_TIME=%TEMP_TIME:.=-%"
set "TIMESTAMP=%TEMP_DATE%_%TEMP_TIME:~0,5%"

:: 3. DEFINICIÓN DE RUTAS
set "RUTA_PROYECTO=%~dp0"
set "RUTA_ESCRITORIO=C:\Users\admin.bisql02\Desktop"
set "ARCHIVO_LOG=%RUTA_ESCRITORIO%\LogLanzador_Encuestas_%TIMESTAMP%.txt"

echo Verificando acceso a log...
echo. > "%ARCHIVO_LOG%" 2>nul
if %ERRORLEVEL% NEQ 0 (
    set "ARCHIVO_LOG=%RUTA_PROYECTO%LogLanzador_Encuestas_%TIMESTAMP%.txt"
)

:: 4. INICIO DE ESCRITURA DEL LOG
(
echo ===================================================
echo   REGISTRO DE EJECUCION DITIC - ENCUESTAS
echo   Inicio: %DATE% %TIME%
echo   Usuario: admin.bisql02
echo   Proyecto: %RUTA_PROYECTO%
echo ===================================================
) > "%ARCHIVO_LOG%"

echo ===================================================
echo   INICIANDO ORQUESTADOR DE ENCUESTAS DITIC
echo ===================================================

:: 5. ACTIVACIÓN DE CONDA
echo [1/3] Activando ambiente Conda... >> "%ARCHIVO_LOG%"
if exist "F:\Anaconda3\Scripts\activate.bat" (
    call "F:\Anaconda3\Scripts\activate.bat" RespuestaEncuestaAreas >> "%ARCHIVO_LOG%" 2>&1
) else (
    echo ❌ ERROR: No se encontro el activador de Conda en F:\ >> "%ARCHIVO_LOG%"
)

:: 6. ENTRAR A LA CARPETA
echo [2/3] Entrando a la carpeta... >> "%ARCHIVO_LOG%"
cd /d "%RUTA_PROYECTO%" >> "%ARCHIVO_LOG%" 2>&1

:: 7. EJECUCIÓN (Ya no fallara por emojis)
echo [3/3] Iniciando proceso de Python... >> "%ARCHIVO_LOG%"

if "%~1"=="" (
    echo ⚙️ MODO AUTOMATICO: Ejecutando TODAS las areas...
    python -u orquestador_principal_flujo_encuestas.py --auto_date >> "%ARCHIVO_LOG%" 2>&1
) else (
    echo ⚙️ MODO MANUAL: Parametros recibidos: %*
    python -u orquestador_principal_flujo_encuestas.py %* >> "%ARCHIVO_LOG%" 2>&1
)

:: 8. CIERRE DE LOG
(
echo.
echo ===================================================
echo ✅ Fin de ejecucion: %TIME%
echo ===================================================
) >> "%ARCHIVO_LOG%"

echo.
echo ✅ Ejecucion terminada. Revisa el log en:
echo "%ARCHIVO_LOG%"

ENDLOCAL
pause