USE [SSISDB]
GO


DROP INDEX IF EXISTS [IX_Internal_Operations_status_start_end_time_INCL_operation_id] ON [internal].[operations] 
CREATE NONCLUSTERED INDEX [IX_Internal_Operations_status_start_end_time_INCL_operation_id]
ON [internal].[operations] ([status],[start_time],[end_time])
INCLUDE ([operation_id])
GO

DROP INDEX IF EXISTS [IX_Internal_Executions_project_name_package_name] ON [internal].[executions]
CREATE NONCLUSTERED INDEX [IX_Internal_Executions_project_name_package_name]
ON [internal].[executions] ([project_name],[package_name])
GO
