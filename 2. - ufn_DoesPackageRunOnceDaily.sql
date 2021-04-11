USE [SSISDB]
GO

IF OBJECT_ID (N'ufn_DoesPackageRunOnceDaily', N'IF') IS NOT NULL
  DROP FUNCTION dbo.ufn_DoesPackageRunOnceDaily
GO
CREATE FUNCTION ufn_DoesPackageRunOnceDaily (@ProjectName NVARCHAR(256), @PackageName NVARCHAR(256), @DaysBack INT, @EndDate DATETIME)
RETURNS TABLE
AS
RETURN
(
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
                        CAST(ei.start_time AS DATE) AS [Date]
                       ,COUNT(DISTINCT ei.execution_id) AS [DistinctCountOfExecIdsPerDay]
                       ,ei.project_name
                       ,ei.package_name
              FROM     [SSISDB].internal.execution_info ei WITH (NOLOCK)           
              WHERE    1 = 1
              AND      ei.project_name = @ProjectName
              AND      ei.package_name = @PackageName
              AND      ei.start_time >= DATEADD(DAY, @DaysBack , GETDATE())
              AND      ei.end_time <= @EndDate
              AND      ei.status = 7 -- Success (we want to calculate stats on successfull runs only)  
              GROUP BY CAST(ei.start_time AS DATE), ei.project_name, ei.package_name
          )   AS DistCnt
GROUP BY               
          DistCnt.project_name,
          DistCnt.package_name
)
GO