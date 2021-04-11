/*********************************************************************************************
sp_WhoIsActive_inSSIS v0.01 (2021-04-08)
(C) 2021, Marek Grzymala

Feedback: https://www.linkedin.com/in/marek-grzymala-sql/

License: 
	sp_WhoIsActive_inSSIS is free to download and use for personal, educational, and internal 
	corporate purposes, provided that this header is preserved. Redistribution or sale 
	of sp_WhoIsActive_inSSIS, in whole or in part, is prohibited without the author's express 
	written consent.
Running:
    EXEC [dbo].[sp_WhoIsActive_inSSIS]
*********************************************************************************************/


USE [SSISDB]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER PROCEDURE [dbo].[sp_WhoIsActive_inSSIS]
       @PercentileLower FLOAT = 0.2, /* percentile BELOW WHICH we are excluding the outliers from the stats calculation */
       @PercentileUpper FLOAT = 0.8, /* percentile ABOVE WHICH we are excluding the outliers from the stats calculation */
       @DaysBack INT = -30, /* the range (in days) of how far back we are calculating the duration/start-time statistics */
       @DaysMargin INT = 10 /* if a package execution is missing some days within the range of @DaysBack 
                               but never runs MORE THAN ONCE DAILY this is the max. number of skipped days 
                               still allowed to calculate the StartTime Avg, Lower/Upper Percentiles etc. for that package */
AS
BEGIN

DROP TABLE IF EXISTS #CurrentlyRunningPckgs
CREATE TABLE #CurrentlyRunningPckgs
(
    [execution_id] BIGINT PRIMARY KEY NOT NULL,
    [project_name] NVARCHAR(128) NOT NULL,
    [package_name] NVARCHAR(260) NOT NULL,
    [executed_as_name] NVARCHAR(128) NOT NULL,
    [StartTime] DATETIME2(0) NOT NULL,
    [Duration_Current(minutes)] INT NOT NULL,
    --[Duration_CompToAvg] INT,
    [Duration_PercLower] FLOAT(8) NULL,
    [Duration_PercUpper] FLOAT(8) NULL,
    [TimeStart_PercLower] TIME(0) NULL,
    [TimeStart_PercUpper] TIME(0) NULL,
    [TimeEnd_PercLower] TIME(0) NULL,
    [TimeEnd_PercUpper] TIME(0) NULL,
    [RunsOnceDaily] BIT NOT NULL,
    [RanMoreThanOnceDaily] BIT NOT NULL,
    [HowManyTimesDidItRun] INT NOT NULL
)

INSERT INTO #CurrentlyRunningPckgs
(
    execution_id,
    project_name,
    package_name,
    executed_as_name,
    StartTime,
    [Duration_Current(minutes)],
    --Duration_CompToAvg,
    Duration_PercLower,
    Duration_PercUpper,
    TimeStart_PercLower,
    TimeStart_PercUpper,
    TimeEnd_PercLower,
    TimeEnd_PercUpper,
    RunsOnceDaily,
    RanMoreThanOnceDaily,
    HowManyTimesDidItRun
)

SELECT
             ei.execution_id
            ,ei.project_name
            ,ei.package_name
            ,ei.executed_as_name
            ,CAST(ei.start_time AS DATETIME2(0))                               AS [StartTime]
            ,DATEDIFF(MINUTE, CAST(ei.start_time AS DATETIME2(0)), GETDATE())  AS [Duration_Current(minutes)]
            ,prcnt.[Duration_PercLower]                                        AS [Duration_PercLower]
            ,prcnt.[Duration_PercUpper]                                        AS [Duration_PercUpper]
            ,CASE WHEN 
                  (rd.[RunsOnceDaily] = 1) OR (rd.[RunsOnceDaily] = 0 AND rd.[RanMoreThanOnceDaily] = 0 AND rd.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                  THEN prcnt.[TimeStart_PercLower] ELSE NULL 
             END AS [TimeStart_PercLower]
            ,CASE WHEN 
                  (rd.[RunsOnceDaily] = 1) OR (rd.[RunsOnceDaily] = 0 AND rd.[RanMoreThanOnceDaily] = 0 AND rd.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                  THEN prcnt.[TimeStart_PercUpper] ELSE NULL 
             END AS [TimeStart_PercUpper]
            ,CASE WHEN 
                  (rd.[RunsOnceDaily] = 1) OR (rd.[RunsOnceDaily] = 0 AND rd.[RanMoreThanOnceDaily] = 0 AND rd.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                  THEN prcnt.[TimeEnd_PercLower]   ELSE NULL 
             END AS [TimeEnd_PercLower]  
            ,CASE WHEN (rd.[RunsOnceDaily] = 1) OR (rd.[RunsOnceDaily] = 0 AND rd.[RanMoreThanOnceDaily] = 0 AND rd.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                  THEN prcnt.[TimeEnd_PercUpper]   ELSE NULL 
             END AS [TimeEnd_PercUpper]
            ,rd.[RunsOnceDaily]
            ,rd.[RanMoreThanOnceDaily]
            ,rd.[HowManyTimesDidItRun]

FROM        [SSISDB].internal.execution_info ei WITH (NOLOCK)
CROSS APPLY (
                SELECT 
                        [ProjectName],
                        [PackageName],
                        [TimeStart_PercLower],
                        [TimeStart_PercUpper],
                        [TimeEnd_PercLower],
                        [TimeEnd_PercUpper],
                        [Duration_PercLower],
                        [Duration_PercUpper] 
                FROM    dbo.ufn_GetPackageHistPercentiles_PerPackageName(ei.project_name, ei.package_name, @PercentileLower, @PercentileUpper, @DaysBack, GETDATE())
            )   AS      prcnt
CROSS APPLY (
                SELECT
                        [ProjectName],
                        [PackageName],
                        [RunsOnceDaily],
                        [RanMoreThanOnceDaily],
                        [HowManyTimesDidItRun]
                FROM    dbo.ufn_DoesPackageRunOnceDaily(ei.project_name, ei.package_name, @DaysBack, GETDATE()) 
            )   AS      rd

WHERE      ei.status = 2 -- Currently Running
AND        ei.package_name NOT IN ('Orchestration.dtsx', 'ParallelStream.dtsx')
AND        ei.project_name = prcnt.ProjectName
AND        ei.package_name = prcnt.PackageName
AND        ei.project_name = rd.ProjectName
AND        ei.package_name = rd.PackageName
ORDER BY   ei.execution_id DESC 

SELECT 
            crp.[execution_id],
            crp.[project_name],
            crp.[package_name],
            crp.[executed_as_name],
            crp.[StartTime],
            
            /* Start-Time Results: */
            CASE WHEN 
                (crp.[RunsOnceDaily] = 1) OR (crp.[RunsOnceDaily] = 0 AND crp.[RanMoreThanOnceDaily] = 0 AND crp.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                THEN hstavg.[StartTimeAvg]   ELSE NULL
            END AS [StartTimeAvg],  
            CASE WHEN 
                (crp.[RunsOnceDaily] = 1) OR (crp.[RunsOnceDaily] = 0 AND crp.[RanMoreThanOnceDaily] = 0 AND crp.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                THEN DATEDIFF(MINUTE, CAST(crp.[StartTime] AS TIME(0)), hstavg.[StartTimeAvg])   
                ELSE NULL
            END AS [StartTime Behind/AheadOf Avg],
            crp.[TimeStart_PercLower],
            crp.[TimeStart_PercUpper],
            crp.[TimeEnd_PercLower],
            crp.[TimeEnd_PercUpper],
            CAST(DATEADD(MINUTE, hstavg.[DurationAvgMinutes], crp.[StartTime]) AS TIME(0)) AS [ExpctEndTime],
            
            /* Duration Results: */
            crp.[Duration_Current(minutes)],
            (hstavg.[DurationAvgMinutes] - crp.[Duration_Current(minutes)]) AS [Currently Behind/AheadOf Avg.Duration],
            crp.[Duration_PercLower],
            crp.[Duration_PercUpper],
            
            /* Additional Stats: */
            crp.[RunsOnceDaily],
            crp.[RanMoreThanOnceDaily],
            crp.[HowManyTimesDidItRun]
            
FROM 
            #CurrentlyRunningPckgs crp
CROSS APPLY (
                SELECT 
                         [ProjectName]
                        ,[PackageName]
                        ,[DurationAvgMinutes]
                        ,[StartTimeAvg]
                FROM    dbo.ufn_GetPackageHistAvg_PerPackageName_ExclOutliers(crp.project_name, crp.package_name, crp.Duration_PercLower, crp.Duration_PercUpper, @DaysBack, GETDATE())
            )   AS      hstavg
WHERE       1 = 1
AND        crp.project_name = hstavg.ProjectName
AND        crp.package_name = hstavg.PackageName
ORDER BY   crp.execution_id DESC
END
GO
