/*********************************************************************************************
sp_WhoIsActive_inSSIS v0.04 (2021-06-02)
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
       
        @PercentileLower FLOAT = 0.2         /* percentile BELOW WHICH we are excluding the outliers from the stats calculation */
       ,@PercentileUpper FLOAT = 0.8         /* percentile ABOVE WHICH we are excluding the outliers from the stats calculation */
       ,@DaysBack INT = -30                  /* the range (in days) of how far back we are calculating the duration/start-time statistics */
       ,@DaysMargin INT = 10                 /* if a package execution is missing some days within the range of @DaysBack 
                                               but never runs MORE THAN ONCE DAILY this is the max. number of skipped days 
                                               still allowed to calculate the StartTime Avg, Lower/Upper Percentiles for that package */
       ,@ProjectsToIgnore NVARCHAR(MAX) = 'Dyn_Dyn_Orchestration'                   /* comma-separated names of projects you want to ignore in the list */
       ,@PackagesToIgnore NVARCHAR(MAX) = 'Orchestration.dtsx, ParallelStream.dtsx' /* comma-separated names of packages you want to ignore in the list */
AS
BEGIN

DECLARE @EndDate DATETIME = GETDATE() /* change if you want to limit the stats calculation to an earlier date in the past */
SELECT
             ei.execution_id
            ,ei.project_name
            ,ei.package_name
            ,ei.executed_as_name
            ,CAST(ei.start_time AS DATETIME2(0))                               AS [StartTime]

            /* Start-Time Results: */
            /* if a package execution is missing @DaysMargin number of days within the range of @DaysBack 
               but never runs MORE THAN ONCE DAILY calculate the StartTime Avg, Lower/Upper Percentiles for that package */
            ,CASE WHEN 
                 (rd.[RunsOnceDaily] = 1) OR (rd.[RunsOnceDaily] = 0 AND rd.[RanMoreThanOnceDaily] = 0 AND rd.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                 THEN hstavg_start_time.[StartTimeAvg]   ELSE NULL
             END                                                                           AS [StartTimeAvg]
            ,CASE WHEN 
                 (rd.[RunsOnceDaily] = 1) OR (rd.[RunsOnceDaily] = 0 AND rd.[RanMoreThanOnceDaily] = 0 AND rd.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                 THEN DATEDIFF(MINUTE, CAST(ei.start_time AS TIME(0)), hstavg_start_time.[StartTimeAvg])   
                 ELSE NULL
             END                                                                          AS [StartTime Behind/AheadOf Avg]
            ,CASE WHEN 
                  (rd.[RunsOnceDaily] = 1) OR (rd.[RunsOnceDaily] = 0 AND rd.[RanMoreThanOnceDaily] = 0 AND rd.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                  THEN prcnt.[TimeStart_PercLower] ELSE NULL 
             END                                                                          AS [TimeStart_PercLower]
            ,CASE WHEN 
                  (rd.[RunsOnceDaily] = 1) OR (rd.[RunsOnceDaily] = 0 AND rd.[RanMoreThanOnceDaily] = 0 AND rd.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                  THEN prcnt.[TimeStart_PercUpper] ELSE NULL 
             END                                                                          AS [TimeStart_PercUpper]
            ,CASE WHEN 
                  (rd.[RunsOnceDaily] = 1) OR (rd.[RunsOnceDaily] = 0 AND rd.[RanMoreThanOnceDaily] = 0 AND rd.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                  THEN prcnt.[TimeEnd_PercLower]   ELSE NULL 
             END                                                                          AS [TimeEnd_PercLower]  
            ,CASE WHEN (rd.[RunsOnceDaily] = 1) OR (rd.[RunsOnceDaily] = 0 AND rd.[RanMoreThanOnceDaily] = 0 AND rd.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
                  THEN prcnt.[TimeEnd_PercUpper]   ELSE NULL 
             END                                                                          AS [TimeEnd_PercUpper]
            ,CAST(DATEADD(MINUTE, hstavg_duration.[DurationAvgMinutes], ei.start_time) AS TIME(0)) AS [ExpctEndTime]

            /* Duration Results: */
            ,DATEDIFF(MINUTE, CAST(ei.start_time AS DATETIME2(0)), GETDATE())  AS [Duration_Current(minutes)]
            ,(hstavg_duration.[DurationAvgMinutes] - (DATEDIFF(MINUTE, CAST(ei.start_time AS DATETIME2(0)), GETDATE()))) AS [Currently Behind/AheadOf Avg.Duration]
            ,prcnt.[Duration_PercLower]
            ,prcnt.[Duration_PercUpper]

            /* Additional Stats: */
            ,rd.[RunsOnceDaily]
            ,rd.[RanMoreThanOnceDaily]
            ,rd.[HowManyTimesDidItRun]

FROM        [SSISDB].internal.execution_info ei
/* ----------------------------- Function to GetPackageHistPercentiles_PerPackageName: ----------------------------- */
OUTER APPLY (
            SELECT  TOP 1 
            
                     ei_prcnt.project_name AS [ProjectName]
                    ,ei_prcnt.package_name AS [PackageName]
            
                    ,CAST(DATEADD(SECOND, PERCENTILE_CONT(@PercentileLower) WITHIN GROUP (ORDER BY DATEDIFF(SECOND, CAST(CAST(ei_prcnt.start_time AS DATE) AS DATETIME), CAST(ei_prcnt.start_time AS DATETIME))) OVER (PARTITION BY ei_prcnt.package_name), 0) AS TIME(0)) AS [TimeStart_PercLower]
                    ,CAST(DATEADD(SECOND, PERCENTILE_CONT(@PercentileUpper) WITHIN GROUP (ORDER BY DATEDIFF(SECOND, CAST(CAST(ei_prcnt.start_time AS DATE) AS DATETIME), CAST(ei_prcnt.start_time AS DATETIME))) OVER (PARTITION BY ei_prcnt.package_name), 0) AS TIME(0)) AS [TimeStart_PercUpper]
                    
                    ,CAST(DATEADD(SECOND, PERCENTILE_CONT(@PercentileLower) WITHIN GROUP (ORDER BY DATEDIFF(SECOND, CAST(CAST(ei_prcnt.end_time AS DATE) AS DATETIME), CAST(ei_prcnt.end_time AS DATETIME))) OVER (PARTITION BY ei_prcnt.package_name), 0) AS TIME(0)) AS [TimeEnd_PercLower]
                    ,CAST(DATEADD(SECOND, PERCENTILE_CONT(@PercentileUpper) WITHIN GROUP (ORDER BY DATEDIFF(SECOND, CAST(CAST(ei_prcnt.end_time AS DATE) AS DATETIME), CAST(ei_prcnt.end_time AS DATETIME))) OVER (PARTITION BY ei_prcnt.package_name), 0) AS TIME(0)) AS [TimeEnd_PercUpper]
            
                    ,PERCENTILE_CONT(@PercentileLower) WITHIN GROUP (ORDER BY DATEDIFF(MINUTE, ei_prcnt.start_time, ei_prcnt.end_time)) OVER (PARTITION BY ei_prcnt.package_name) AS [Duration_PercLower]
                    ,PERCENTILE_CONT(@PercentileUpper) WITHIN GROUP (ORDER BY DATEDIFF(MINUTE, ei_prcnt.start_time, ei_prcnt.end_time)) OVER (PARTITION BY ei_prcnt.package_name) AS [Duration_PercUpper]
            
            FROM    [SSISDB].internal.execution_info ei_prcnt WITH (NOLOCK)           
            WHERE   1 = 1
            AND     ei_prcnt.project_name = ei.project_name
            AND     ei_prcnt.package_name = ei.package_name
            AND     ei_prcnt.status = 7 /* Success (we want to calculate stats on successfull runs only)  */ 
            AND     ei_prcnt.start_time >= DATEADD(DAY, @DaysBack, CAST(CAST(@EndDate+1 AS DATE) AS DATETIME)) /* cast the date to midnight next day minus @DaysBack */ 
            AND     ei_prcnt.end_time < CAST(CAST(@EndDate+1 AS DATE) AS DATETIME) /* cast the date to midnight next day */ 
            ) AS    prcnt
/* ----------------------------- Function to find if the PackageRunsOnceDaily: ----------------------------- */
OUTER APPLY (
            SELECT 
                      DistCnt.project_name AS [ProjectName],
                      DistCnt.package_name AS [PackageName],
                      CASE 
                          WHEN ((SUM(DistCnt.DistinctCountOfExecIdsPerDay) = @DaysBack  * (-1)) AND MAX(DistCnt.DistinctCountOfExecIdsPerDay) = 1) THEN 1
                          ELSE 0
                      END AS [RunsOnceDaily],
                      CASE 
                          WHEN (MAX(DistCnt.DistinctCountOfExecIdsPerDay) > 1) THEN 1
                          ELSE 0
                      END AS [RanMoreThanOnceDaily],
                      SUM(DistCnt.DistinctCountOfExecIdsPerDay) AS [HowManyTimesDidItRun]
            FROM      (
                          SELECT  
                                    CAST(ei_rd.start_time AS DATE) AS [Date]
                                   ,COUNT(DISTINCT ei_rd.execution_id) AS [DistinctCountOfExecIdsPerDay]
                                   ,ei_rd.project_name
                                   ,ei_rd.package_name
                          FROM     [SSISDB].internal.execution_info ei_rd WITH (NOLOCK)           
                          WHERE    1 = 1
                          AND      ei_rd.project_name = ei.project_name
                          AND      ei_rd.package_name = ei.package_name
                          AND      ei_rd.start_time >= DATEADD(DAY, @DaysBack , GETDATE())
                          AND      ei_rd.end_time <= @EndDate
                          AND      ei_rd.status = 7 -- Success (we want to calculate stats on successfull runs only)  
                          GROUP BY CAST(ei_rd.start_time AS DATE), ei_rd.project_name, ei_rd.package_name
                      )   AS DistCnt
            GROUP BY               
                      DistCnt.project_name,
                      DistCnt.package_name
            ) AS      rd
/* ----------------------------- Function to GetPackageHistAvg_Duration_PerPackageName_ExclOutliers: ----------------------------- */
OUTER APPLY (
            SELECT       
                         ei.project_name AS [ProjectName]
                        ,ei.package_name AS [PackageName]
                        ,AVG(DATEDIFF(MINUTE, ei_hst_av_dur.start_time, ei_hst_av_dur.end_time)) AS [DurationAvgMinutes]
            FROM        [SSISDB].internal.execution_info ei_hst_av_dur WITH (NOLOCK)
            
            WHERE       1 = 1
            AND         ei_hst_av_dur.project_name = ei.project_name
            AND         ei_hst_av_dur.package_name = ei.package_name
            AND         ei_hst_av_dur.status = 7 /* Success (we want to calculate stats on successfull runs only)   */ 
            AND         ei_hst_av_dur.start_time >= DATEADD(DAY, @DaysBack, CAST(CAST(@EndDate+1 AS DATE) AS DATETIME)) /* Cast the date to midnight next day minus @DaysBack */ 
            AND         ei_hst_av_dur.end_time < CAST(CAST(@EndDate+1 AS DATE) AS DATETIME) /* CAST the date to midnight next day */
            /* Here we are excluding duration outliers: */
            AND         DATEDIFF(MINUTE, ei_hst_av_dur.start_time, ei_hst_av_dur.end_time) >= prcnt.Duration_PercLower
            AND         DATEDIFF(MINUTE, ei_hst_av_dur.start_time, ei_hst_av_dur.end_time) <= prcnt.Duration_PercUpper
)   AS      hstavg_duration
/* ----------------------------- Function to GetPackageHistAvg_StartTime_PerPackageName_ExclOutliers: ----------------------------- */
OUTER APPLY (
            SELECT       --TOP 1 
                         ei.project_name AS [ProjectName]
                        ,ei.package_name AS [PackageName]
                        ,CONVERT(TIME(0), DATEADD(SECOND, AVG(DATEDIFF(SECOND, 0, CAST(ei_hst_av_strt.start_time AS TIME(0)))), 0)) AS [StartTimeAvg] -- so let's calculate the StartTime AVG no matter if the package runs once daily or not
            
            FROM        [SSISDB].internal.execution_info ei_hst_av_strt WITH (NOLOCK)
            WHERE       1 = 1
            AND         ei_hst_av_strt.project_name = ei.project_name
            AND         ei_hst_av_strt.package_name = ei.package_name
            AND         ei_hst_av_strt.status = 7 /* Success (we want to calculate stats on successfull runs only)   */ 
            AND         ei_hst_av_strt.start_time >= DATEADD(DAY, @DaysBack, CAST(CAST(@EndDate+1 AS DATE) AS DATETIME)) /* Cast the date to midnight next day minus @DaysBack */ 
            AND         ei_hst_av_strt.end_time < CAST(CAST(@EndDate+1 AS DATE) AS DATETIME) /* Cast the date to midnight next day */
            /* Here we are excluding start-time outliers: */
            AND         CAST(ei_hst_av_strt.start_time AS TIME(0)) >= prcnt.TimeStart_PercLower
            AND         CAST(ei_hst_av_strt.start_time AS TIME(0)) <= prcnt.TimeStart_PercUpper
            )   AS      hstavg_start_time

WHERE      ei.status = 2 -- Currently Running
AND        ei.project_name NOT IN (SELECT LTRIM([value]) FROM STRING_SPLIT(@ProjectsToIgnore, ','))
AND        ei.package_name NOT IN (SELECT LTRIM([value]) FROM STRING_SPLIT(@PackagesToIgnore, ','))

ORDER BY   ei.execution_id DESC 
END
GO
