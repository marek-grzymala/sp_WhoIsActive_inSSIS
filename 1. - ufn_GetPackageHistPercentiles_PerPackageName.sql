USE [SSISDB]
GO

IF OBJECT_ID (N'ufn_GetPackageHistPercentiles_PerPackageName', N'IF') IS NOT NULL
  DROP FUNCTION dbo.ufn_GetPackageHistPercentiles_PerPackageName
GO
CREATE FUNCTION ufn_GetPackageHistPercentiles_PerPackageName (@ProjectName NVARCHAR(256), @PackageName NVARCHAR(256), @PercentileLower FLOAT, @PercentileUpper FLOAT, @DaysBack INT, @EndDate DATETIME)
RETURNS TABLE
AS
RETURN
(
SELECT   TOP 1 

         @ProjectName AS [ProjectName]
        ,@PackageName AS [PackageName]

        ,CAST(DATEADD(SECOND, PERCENTILE_CONT(@PercentileLower) WITHIN GROUP (ORDER BY DATEDIFF(SECOND, CAST(ei.start_time AS DATE), ei.start_time)) OVER (PARTITION BY @PackageName), 0) AS TIME(0)) AS [TimeStart_PercLower]
        ,CAST(DATEADD(SECOND, PERCENTILE_CONT(@PercentileUpper) WITHIN GROUP (ORDER BY DATEDIFF(SECOND, CAST(ei.start_time AS DATE), ei.start_time)) OVER (PARTITION BY @PackageName), 0) AS TIME(0)) AS [TimeStart_PercUpper]
        
        ,CAST(DATEADD(SECOND, PERCENTILE_CONT(@PercentileLower) WITHIN GROUP (ORDER BY DATEDIFF(SECOND, CAST(ei.end_time AS DATE), ei.end_time)) OVER (PARTITION BY @PackageName), 0) AS TIME(0)) AS [TimeEnd_PercLower]
        ,CAST(DATEADD(SECOND, PERCENTILE_CONT(@PercentileUpper) WITHIN GROUP (ORDER BY DATEDIFF(SECOND, CAST(ei.end_time AS DATE), ei.end_time)) OVER (PARTITION BY @PackageName), 0) AS TIME(0)) AS [TimeEnd_PercUpper]

        ,PERCENTILE_CONT(@PercentileLower) WITHIN GROUP (ORDER BY DATEDIFF(MINUTE, ei.start_time, ei.end_time)) OVER (PARTITION BY @PackageName) AS [Duration_PercLower]
        ,PERCENTILE_CONT(@PercentileUpper) WITHIN GROUP (ORDER BY DATEDIFF(MINUTE, ei.start_time, ei.end_time)) OVER (PARTITION BY @PackageName) AS [Duration_PercUpper]

FROM    [SSISDB].internal.execution_info ei WITH (NOLOCK)           
WHERE   1 = 1
AND     ei.project_name = @ProjectName
AND     ei.package_name = @PackageName
AND     ei.status = 7 -- Success (we want to calculate stats on successfull runs only)  
AND     ei.start_time >= DATEADD(DAY, @DaysBack, CAST(CAST(@EndDate+1 AS DATE) AS DATETIME)) -- cast the date to midnight next day
AND     ei.end_time < CAST(CAST(@EndDate+1 AS DATE) AS DATETIME) -- cast the date to midnight next day
)
GO