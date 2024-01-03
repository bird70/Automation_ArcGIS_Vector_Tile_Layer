REM This process is used to convert NetCDF to ArcGIS WebServices for TURBID example
REM IMPORTANT: the Python environment needs to have PYODBC installed (we use ArcGIS Pro conda env)

SET LOG_LOCATION="F:\Conversion_from_NetCDF.log"

echo %Date%:%Time% - Converting new NetCDF files (A) in F:\TURBID_modelData_Convert... >>%LOG_LOCATION%


REM ============== Starting Python script ==============
%PYTHONBIN% F:\Scripted_DB_and_VTL_Update_ArcGIS.py


echo %Date%:%Time% - Done Converting new NetCDF files  in F:\Topnet_modelData_Convert... >>%LOG_LOCATION%