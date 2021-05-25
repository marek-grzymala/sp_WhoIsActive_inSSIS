USE [SSISDB]
GO


DECLARE
  @ProjectName NVARCHAR(256) = ''
, @PackageName NVARCHAR(256) = '.dtsx'
, @PercentileLower FLOAT = 0.2
, @PercentileUpper FLOAT = 0.8
, @DaysBack INT = -30
, @EndDate DATETIME = GETDATE()
, @DaysMargin INT = 10  


SELECT    --TOP 1 

          @ProjectName AS [ProjectName]
         ,@PackageName AS [PackageName]
         ,CAST(ei.start_time AS DATETIME2(0)) AS [StartTime]
         ,CAST(ei.end_time AS DATETIME2(0)) AS [EndTime]
         ,CAST(ei.start_time AS TIME(0)) AS [StartTime_Time]
         --,CAST(CAST(ei.start_time AS DATE) AS DATETIME2(0)) AS [StartTime_Time2]
         --,DATEDIFF(SECOND, CAST(ei.start_time AS DATE), ei.start_time) AS [NumOfSecondsFromMidnight_WRONG]
         --,DATEDIFF(SECOND, CAST(CAST(ei.start_time AS DATE) AS DATETIME2(0)), CAST(ei.start_time AS DATETIME2(0))) AS [NumOfSecondsFromMidnight_CORRECT]
         --,CONVERT(TIME(0), DATEADD(SECOND, DATEDIFF(SECOND, CAST(ei.start_time AS DATE), ei.start_time), 0)) AS [StartTime_Time_BasedOnSecondsFromMidnight_WRONG]
         --,CONVERT(TIME(0), DATEADD(SECOND, DATEDIFF(SECOND, CAST(CAST(ei.start_time AS DATE) AS DATETIME), CAST(ei.start_time AS DATETIME)), 0)) AS [StartTime_Time_BasedOnSecondsFromMidnight_CORRECT]
         

         ,hstavg_duration.[DurationAvgMinutes]      AS [DurationAvgMinutes]
         ,hstavg_start_time.[StartTimeAvg]          AS [StartTimeAvg_NEW]

         /* [TimeStart_PercLower] and [TimeStart_PercUpper] are wrong by one hour: */
         /*-------------------------------------------------------------------------------------------------------------------------------------------------------*/
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
          /*-------------------------------------------------------------------------------------------------------------------------------------------------------*/
         
         ,CASE WHEN 
              (rd.[RunsOnceDaily] = 1) OR (rd.[RunsOnceDaily] = 0 AND rd.[RanMoreThanOnceDaily] = 0 AND rd.[HowManyTimesDidItRun] >= (@DaysBack*(-1))-@DaysMargin) 
              THEN DATEDIFF(MINUTE, CAST(ei.start_time AS TIME(0)), hstavg_start_time.[StartTimeAvg])   
              ELSE NULL
          END                                                                          AS [StartTime Behind/AheadOf Avg]

FROM     [SSISDB].internal.execution_info ei WITH (NOLOCK)    
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
                FROM    dbo.ufn_GetPackageHistAvg_Duration_PerPackageName_ExclOutliers(ei.project_name, ei.package_name, prcnt.Duration_PercLower, prcnt.Duration_PercUpper, @DaysBack, GETDATE())
            )   AS      hstavg_duration
OUTER APPLY (
                SELECT 
                         [ProjectName]
                        ,[PackageName]
                        ,[StartTimeAvg]
                FROM    dbo.ufn_GetPackageHistAvg_StartTime_PerPackageName_ExclOutliers(ei.project_name, ei.package_name, prcnt.TimeStart_PercLower, prcnt.TimeStart_PercUpper, @DaysBack, GETDATE())
            )   AS      hstavg_start_time
WHERE    1 = 1
AND      ei.project_name = @ProjectName
AND      ei.package_name = @PackageName
AND      ei.status = 7 -- Success (we want to calculate stats on successfull runs only)  
AND      ei.start_time >= DATEADD(DAY, @DaysBack, CAST(CAST(@EndDate+1 AS DATE) AS DATETIME)) -- cast the date to midnight next day
AND      ei.end_time < CAST(CAST(@EndDate+1 AS DATE) AS DATETIME) -- cast the date to midnight next day

ORDER BY [StartTime] DESC --, [StartTime_SecFromMidnight]
