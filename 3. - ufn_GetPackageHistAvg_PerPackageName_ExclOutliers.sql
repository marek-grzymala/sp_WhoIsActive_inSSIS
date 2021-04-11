USE [SSISDB]
GO

IF OBJECT_ID (N'ufn_GetPackageHistAvg_PerPackageName_ExclOutliers', N'IF') IS NOT NULL
  DROP FUNCTION dbo.ufn_GetPackageHistAvg_PerPackageName_ExclOutliers
GO

CREATE FUNCTION ufn_GetPackageHistAvg_PerPackageName_ExclOutliers (@ProjectName NVARCHAR(256), @PackageName NVARCHAR(256), @DurationPercentileLower FLOAT, @DurationPercentileUpper FLOAT, @DaysBack INT, @EndDate DATETIME)
RETURNS TABLE
AS
RETURN
(
SELECT       --TOP 1 

             @ProjectName AS [ProjectName]
            ,@PackageName AS [PackageName]
            ,AVG(DATEDIFF(MINUTE, ei.start_time, ei.end_time)) AS [DurationAvgMinutes]
            --,CASE runs_daily.[RunsOnceDaily] WHEN 1 THEN CONVERT(TIME(0), DATEADD(SECOND, AVG(DATEDIFF(SECOND, 0, CAST(ei.start_time AS TIME(0)))), 0)) ELSE NULL END AS [StartTimeAvg] -- for performance reason we do not want to GROUP BY runs_daily.[RunsOnceDaily] 
            ,CONVERT(TIME(0), DATEADD(SECOND, AVG(DATEDIFF(SECOND, 0, CAST(ei.start_time AS TIME(0)))), 0)) AS [StartTimeAvg] -- so let's calculate the StartTime AVG no matter if the package runs once daily or not

FROM        [SSISDB].internal.execution_info ei WITH (NOLOCK)
CROSS APPLY (
                SELECT
                        [ProjectName],
                        [PackageName],
                        [RunsOnceDaily]
                FROM    dbo.ufn_DoesPackageRunOnceDaily(ei.project_name, ei.package_name, @DaysBack, GETDATE()) 
            )   AS      runs_daily
WHERE       1 = 1
AND         ei.project_name = @ProjectName
AND         ei.package_name = @PackageName
AND         ei.project_name = runs_daily.ProjectName
AND         ei.package_name = runs_daily.PackageName
AND         ei.status = 7 -- Success (we want to calculate stats on successfull runs only)  
AND         ei.start_time >= DATEADD(DAY, @DaysBack, CAST(CAST(@EndDate+1 AS DATE) AS DATETIME)) -- cast the date to midnight next day
AND         ei.end_time < CAST(CAST(@EndDate+1 AS DATE) AS DATETIME) -- cast the date to midnight next day
AND         DATEDIFF(MINUTE, ei.start_time, ei.end_time) >= @DurationPercentileLower
AND         DATEDIFF(MINUTE, ei.start_time, ei.end_time) <= @DurationPercentileUpper
)
GO