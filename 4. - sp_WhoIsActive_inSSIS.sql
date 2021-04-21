/*********************************************************************************************
sp_WhoIsActive_inSSIS v0.02 (2021-04-21)
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

SELECT
             ei.execution_id
            ,ei.project_name
            ,ei.package_name
            ,ei.executed_as_name
            ,CAST(ei.start_time AS DATETIME2(0))                               AS [StartTime]

            /* Start-Time Results: */
            ,CASE WHEN 
                 (rd.[RunsOnceDaily] = 1) OR (rd.[RunsOnceDaily] = 0 AND rd.[RanMoreThanOnceDaily] = 0 AND rd.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                 THEN hstavg.[StartTimeAvg]   ELSE NULL
             END AS [StartTimeAvg]

            ,CASE WHEN 
                 (rd.[RunsOnceDaily] = 1) OR (rd.[RunsOnceDaily] = 0 AND rd.[RanMoreThanOnceDaily] = 0 AND rd.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                 THEN DATEDIFF(MINUTE, CAST(ei.start_time AS TIME(0)), hstavg.[StartTimeAvg])   
                 ELSE NULL
             END AS [StartTime Behind/AheadOf Avg]
            ,prcnt.[TimeStart_PercLower]
            ,prcnt.[TimeStart_PercUpper]
            ,prcnt.[TimeEnd_PercLower]
            ,prcnt.[TimeEnd_PercUpper]
            ,CAST(DATEADD(MINUTE, hstavg.[DurationAvgMinutes], ei.start_time) AS TIME(0)) AS [ExpctEndTime]

            /* Duration Results: */
            ,DATEDIFF(MINUTE, CAST(ei.start_time AS DATETIME2(0)), GETDATE())  AS [Duration_Current(minutes)]
            ,(hstavg.[DurationAvgMinutes] - (DATEDIFF(MINUTE, CAST(ei.start_time AS DATETIME2(0)), GETDATE()))) AS [Currently Behind/AheadOf Avg.Duration]
            ,prcnt.[Duration_PercLower]
            ,prcnt.[Duration_PercUpper]

            /* Additional Stats: */
            ,rd.[RunsOnceDaily]
            ,rd.[RanMoreThanOnceDaily]
            ,rd.[HowManyTimesDidItRun]

FROM        [SSISDB].internal.execution_info ei
OUTER APPLY (
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
OUTER APPLY (
                SELECT
                        [ProjectName],
                        [PackageName],
                        [RunsOnceDaily],
                        [RanMoreThanOnceDaily],
                        [HowManyTimesDidItRun]
                FROM    dbo.ufn_DoesPackageRunOnceDaily(ei.project_name, ei.package_name, @DaysBack, GETDATE()) 
            )   AS      rd
OUTER APPLY (
                SELECT 
                         [ProjectName]
                        ,[PackageName]
                        ,[DurationAvgMinutes]
                        ,[StartTimeAvg]
                FROM    dbo.ufn_GetPackageHistAvg_PerPackageName_ExclOutliers(ei.project_name, ei.package_name, prcnt.Duration_PercLower, prcnt.Duration_PercUpper, @DaysBack, GETDATE())
            )   AS      hstavg

WHERE      ei.status = 2 -- Currently Running
AND        ei.package_name NOT IN ('Orchestration.dtsx', 'ParallelStream.dtsx')
ORDER BY   ei.execution_id DESC 
END
GO
