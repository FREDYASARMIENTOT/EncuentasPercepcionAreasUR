--Consulta estado ejecucion job Encuesta_Percepcion_Areas_mes_anterior
USE msdb; -- Contexto para tablas del Agente
GO

-- 1. ESTADO DEL JOB EN EL AGENTE
SELECT 
    j.name AS Proyecto,
    ja.start_execution_date AS [Inicio_Ejecucion],
    ISNULL(CAST(ja.stop_execution_date AS VARCHAR), '>>> EN PROCESO...') AS [Fin_Ejecucion],
    DATEDIFF(SECOND, ja.start_execution_date, GETDATE()) AS [Segundos_Activo]
FROM dbo.sysjobs j
INNER JOIN dbo.sysjobactivity ja ON j.job_id = ja.job_id
WHERE j.name = N'Encuesta_Percepcion_Areas_mes_anterior'
AND ja.session_id = (SELECT MAX(session_id) FROM dbo.syssessions); -- FIX: syssessions
GO

-- 2. LOGICA DE NEGOCIO (Tabla en BA_MODELS)
SELECT TOP 1
    Area,
    Anio AS [Año_Reporte], -- Columnas del Parche V82.9.22
    Mes AS [Mes_Reporte],
    TotalRegistros AS [Muestra_SQL],
    Estado,
    SharePointUpload AS [Sincronizado_Nube],
    Mensaje AS [Hito_Final]
FROM [BA_MODELS].[chatbot].[Log_ExportacionEncuestas] -- FIX: Referencia completa a BA_MODELS
WHERE Area = 'CRAI'
ORDER BY FechaEjecucion DESC;
GO
---para  el job

--USE msdb;
--GO

---- Detener el proceso principal
--EXEC dbo.sp_stop_job @job_name = N'Encuesta_Percepcion_Areas_mes_anterior';
--GO

-- Si lanzaste el job temporal de CRAI, usa este:
-- EXEC dbo.sp_stop_job @job_name = N'Temp_CRAI_Manual_Fix';
-- GO