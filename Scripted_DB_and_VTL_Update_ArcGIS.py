# -*- coding: utf-8 -*-
"""

Modified on 4 Jan 2024 to use Example Data 

## TURBID VTL Update Model for Example Application ##

This has been modified so it updates a layer in the SDE (SQLServer) Geodatabase 
and uses a stored map file to update a Vector Tile Package that is uploaded and
published as a Hosted Feature Layer.

--> (the expected input file is NetCDF in our case - we receive that regularly and convert it.
 In order to pick up the correct file for conversion while keeping older files, a NOTIFICATION text file 
 is used to contain the name.) <--

The process which updates the SQLServer stored layer uses Pandas and PyODBC to
create a (Spatial) Data Frame and use fast/bulk writing to the GISDB DB

Make sure you use your own details for the ArcGIS Portal/Online connection at the top of the script
If you use SDE, also change the credentials used for the SDE connection


"""
import arcpy
from arcpy import da as arcpyDA
from arcgis.gis import GIS
import os
import pandas as pd
import pyodbc
import shutil
from sys import argv

gis = GIS("https://gisportal.com/portal", "USERNAME", "PASSWRD")
sdeConn = "F:\\TURBIDUpdates\\GISDBx@GISDB@SDE.sde"

indir = r"F:\TURBID_modelData_Download"
outdir = r"F:\TURBID_modelData_Convert"

# Open and read content from the downloaded Notification File
notification_file = open(os.path.join(indir, "NOTIFICATION_TURBID.txt"), "r")
content = notification_file.read()

# The File Name for the most current NetCDF file is found in the notification file:
infileName = os.path.join(indir, content.split("\n")[0])
outfileName = "allregions_CONVERT.nc"


infilePath = os.path.join(indir, infileName)
outFilePath = os.path.join(outdir, outfileName)


# Creating a SQLServer connection function for PyODBC
def connectSQL(server, database, uid, pwd, auth="Private"):
    try:
        driverList = pyodbc.drivers()
        if auth == "Private":
            cnxn = pyodbc.connect(
                "Driver={0};Server={1};Database={2};UID={3};PWD={4}".format(
                    driverList[-1], server, database, uid, pwd
                )
            )
        else:
            cnxn = pyodbc.connect(
                "Driver={0};Server={1};Database={2};Trusted_Connection=yes;".format(
                    driverList[-1], server, database
                )
            )
    except:
        print("connectSQL: Cannot connect to database")
        raise
    else:
        print("Connected to the database successfully")
        return cnxn


def truncateTable(cnxn, tbl):
    try:
        curs = cnxn.cursor()
        curs.execute("TRUNCATE TABLE {0}".format(tbl))
    except pyodbc.DatabaseError as err:
        cnxn.rollback()
        print("truncateTable: Unable to truncate table - pyodbc.DatabaseError")
        raise err
    except Exception as e:
        cnxn.rollback()
        print("truncateTable: Unable to truncate table - unknown reason")
        raise e
    else:
        cnxn.commit()
        print("truncateTable successful")


def insertManyRecords(cnxn, dataColumns, dataInput, tableName):
    varHeaders = ",".join(dataColumns)  # Creating a comma delimited list
    valsListVars = ",".join(["?"] * len(dataColumns))  # Creates a placeholder

    print("In function insertManyRecords now")

    try:
        cursor = cnxn.cursor()

        cnxn.autocommit = False
        cursor.fast_executemany = True

        sql_statement = "INSERT INTO {0}({1}) VALUES ({2})".format(
            tableName, varHeaders, valsListVars
        )
        print("Running INSERT cursor: {0}".format(sql_statement))
        cursor.executemany(sql_statement, dataInput)

    except pyodbc.DatabaseError as err:
        cnxn.rollback()
        raise err
    except:
        cnxn.rollback()
        raise
    else:
        cnxn.commit()

    # new
    # Now we need to switch the view which is used to query the hydrograph to use our new table:
    try:
        cursor = cnxn.cursor()

        cnxn.autocommit = True
        cursor.fast_executemany = False

        sql_statement2 = """    ALTER VIEW [GISDBx].[TURBID_Rivers_Flow_All_NEW_TEST]
                                AS
                                SELECT        GISDBx.TURBID_LAYER_TBL.OBJECTID, GISDBx.TURBID_LAYER_TBL.rchid, 
                                                        GISDBx.TURBID_LAYER_TBL.streamorder, GISDBx.TURBID_LAYER_TBL.time, 
                                                        GISDBx.TURBID_LAYER_TBL.nrch, GISDBx.TURBID_LAYER_TBL.absoluteValues, 
                                                        GISDBx.TURBID_LAYER_TBL.absoluteValues5thPercentile, GISDBx.TURBID_LAYER_TBL.absoluteValues25thPercentile, 
                                                        GISDBx.TURBID_LAYER_TBL.absoluteValuesMedian, GISDBx.TURBID_LAYER_TBL.absoluteValues75thPercentile, 
                                                        GISDBx.TURBID_LAYER_TBL.absoluteValues95thPercentile, GISDBx.TURBID_LAYER_TBL.relativeValues, 
                                                        GISDBx.TURBID_LAYER_TBL.relativeValues5thPercentile, GISDBx.TURBID_LAYER_TBL.relativeValues25thPercentile, 
                                                        GISDBx.TURBID_LAYER_TBL.relativeValuesMedian, GISDBx.TURBID_LAYER_TBL.relativeValues75thPercentile, 
                                                        GISDBx.TURBID_LAYER_TBL.relativeValues95thPercentile, GISDB.GISDBx.LINES.Shape
                                FROM          GISDBx.TURBID_LAYER_TBL INNER JOIN
                                GISDB.GISDBx.LINES ON
                                                        
                                                        GISDBx.TURBID_LAYER_TBL.rchid =
                                                        GISDB.GISDBx.LINES.Top_reach"""  # .format(tableName, varHeaders, valsListVars)
        print("Running ALTER VIEW cursor: {0}".format(sql_statement2))
        cursor.execute(sql_statement2)

    except pyodbc.DatabaseError as err:
        cnxn.rollback()
        raise err
    except:
        cnxn.rollback()
        raise
    else:
        cnxn.commit()
    print("ALTER VIEW has run")

    # and also alter the other view which is used for the Vector Tile Layer compilation
    try:
        cursor = cnxn.cursor()

        cnxn.autocommit = True
        cursor.fast_executemany = False

        sql_statement3 = """    ALTER VIEW [GISDBx].[TURBID_Max_RelativeFlow_TEST]
                                    AS
                                    SELECT rchid,MAX(relativeValues) as relativeValues,streamorder from [GISDBx].[TURBID_LAYER_TBL] 
                                    WHERE relativeValues >=0
                                    GROUP BY rchid,streamorder
        """  # .format(tableName, varHeaders, valsListVars)
        print("Running ALTER VIEW cursor: {0}".format(sql_statement3))
        cursor.execute(sql_statement3)

    except pyodbc.DatabaseError as err:
        cnxn.rollback()
        raise err
    except:
        cnxn.rollback()
        raise
    else:
        cnxn.commit()
    print("ALTER VIEW for Vector Tile Layer has run")


def TURBIDVTLupdateModel(
    GISDBx_GISDB_welwmssql_sde=sdeConn,
    streamq_allregions_webmap_nc=outFilePath,
    TURBIDRelativeFlowsVTL_vtpk="F:\\TURBIDUpdates\\TURBIDRelativeFlowsVTL_TEST.vtpk",
):  # TURBID_VTL_Update_Model
    # The grunt of the work happens here in this function

    # To allow overwriting outputs change overwriteOutput option to True.
    arcpy.env.overwriteOutput = True

    print("Copying NetCDF file from Download location to Conversion Folder")
    shutil.copy(infilePath, outFilePath)

    # Model Environment settings
    with arcpy.EnvManager(
        scratchWorkspace=r"F:\TURBIDUpdates\TURBIDUpdates_TEST.gdb",
        workspace=r"F:\TURBIDUpdates\TURBIDUpdates_TEST.gdb",
    ):
        # This Map File stores the symbology etc. used for the Vector Tile Layer and is needed during every update.
        Input_Map = r"F:\TURBIDUpdates\TURBID_Rivers_VTL1_TEST.mapx"

        # Process: Make NetCDF Feature Layer (Make NetCDF Feature Layer) (md)
        ModelValues_Layer_TURBID = "ModelValues_Layer_TURBID"
        with arcpy.EnvManager(
            scratchWorkspace=r"F:\TURBIDUpdates\TURBIDUpdates_TEST.gdb",
            workspace=r"F:\TURBIDUpdates\TURBIDUpdates_TEST.gdb",
        ):
            print("Making the Temp Table View.")
            tempLayer2 = arcpy.md.MakeNetCDFTableView(
                in_netCDF_file=streamq_allregions_webmap_nc,
                variable=[
                    "absoluteValues",
                    "absoluteValues5thPercentile",
                    "absoluteValues25thPercentile",
                    "absoluteValuesMedian",
                    "absoluteValues75thPercentile",
                    "absoluteValues95thPercentile",
                    "relativeValues",
                    "relativeValues5thPercentile",
                    "relativeValues25thPercentile",
                    "relativeValuesMedian",
                    "relativeValues75thPercentile",
                    "relativeValues95thPercentile",
                    "streamorder",
                    "rchid",
                ],
                out_table_view=ModelValues_Layer_TURBID,
                row_dimension=["nrch", "time"],
                dimension_values=[],
                value_selection_method="BY_VALUE",
            )

            print("Conversion to Data Frame is up next.")
            ##################### CONVERT TO SPATIAL DATAFRAME ###############################
            nsdftab = arcpyDA.TableToNumPyArray(in_table=tempLayer2, field_names="*")
            print("Non-spatial NumpyArray created")
            nsdf = pd.DataFrame(nsdftab)

            #################### CONVERT TO LIST OF TUPLES | GET COLUMN NAME LIST##############################

            # Then to convert that sdf to a list of tuples:
            listTuplesFromSDF = list(nsdf.itertuples(index=False, name=None))

            # To get the list of column names it's:
            dataColumns = list(nsdf.columns)
            dataColumns.pop(0)
            dataColumns.insert(0, "OBJECTID")

            try:
                print("Column names: {0}".format(dataColumns))
            except:
                print("not sure how to display column names in SDF.")

            print("Let's try to use Execute SQL to insert the dataColumns")

            try:
                # Variables
                tbl = "GISDB.GISDBx.TURBID_LAYER_TBL"

                # Needs to be a list of tuples [(),(),()] that represents a row entry (let me know if you need help to convert the feature class to this format)
                # There must be a OBJECTID column and the SHAPE columns must be a geometry sql type
                # dataInput = [("1","Row1","Rowl2","Row3","geometry"),( "2", "Row1","Rowl2","Row3"," geometry"),( "3", "Row1","Rowl2","Row3"," geometry"),( "4", "Row1","Rowl2","Row3"," geometry"),( "5", "Row1","Rowl2","Row3"," geometry")]
                dataInput = listTuplesFromSDF

                # Connect to SQL
                cnxn = connectSQL(
                    "GISDBDB.niwa.local", "GISDB", "GISDBx", "AdminPasswordXXX"
                )  # (INSTANCE,DATABASE,USER,PASSWORD)
                # Truncate Table
                truncateTable(cnxn, tbl)
                # Insert records into SQL
                GISDB_GISDBx_ModelValues_Layer_TURBID_NO_thresholds_TEST = (
                    insertManyRecords(cnxn, dataColumns, dataInput, tbl)
                )

            except Exception as e:
                print("Insert function failed")
                raise e
            else:
                print("Main function successful")
            finally:
                cnxn.close()
                pass

        print("The Layer has been written into the DB.")

        # Process: Create Vector Tile Package (Create Vector Tile Package) (management)
        print("GIS: {0}".format(gis))

        try:
            with arcpy.EnvManager(
                scratchWorkspace=r"F:\TURBIDUpdates\TURBIDUpdates_TEST.gdb",
                workspace=r"F:\TURBIDUpdates\TURBIDUpdates_TEST.gdb",
            ):
                arcpy.management.CreateVectorTilePackage(
                    in_map=Input_Map,
                    output_file=TURBIDRelativeFlowsVTL_vtpk,
                    service_type="ONLINE",
                    tiling_scheme="",
                    tile_structure="INDEXED",
                    min_cached_scale=295828763.7957775,
                    max_cached_scale=564.248588,
                    index_polygons="",
                    summary="Layer showing TURBID Relative Flow for a forecast period",
                    tags="TURBID",
                )
                print(
                    "Creating VT Package successful. I'll need to delete the existing Vector Tile Layer next (before publishing a new version)."
                )
                search_result = gis.content.search(
                    "title:TURBIDRelativeFlowsVTL_TEST", item_type=None
                )

                try:
                    search_result[0].delete()

                    print("old VTL title:TURBIDRelativeFlowsVTL_TEST deleted")
                except:
                    print("nothing to delete")
                    pass

        except:
            print("unable to create VT Package")
            arcpy.GetMessages()

            # Process: Share Package (Share Package to Portal and publish as Vector Tile Layer)

            print("Logged into {0}".format(gis))

            vtpk_item = gis.content.add(
                {}, data=TURBIDRelativeFlowsVTL_vtpk, folder="TURBID"
            )
            print(
                "Added package successfully as {0}. Now publishing & sharing it publicly.".format(
                    vtpk_item
                )
            )
            vtile_layer = vtpk_item.publish()
            print("Published package as Layer successfully.")
            try:
                vtile_layer.share(everyone=True)
            except:
                print("couldn't share the layer publicly")

            print("Published as tiled service successfully as {0}".format(vtile_layer))


if __name__ == "__main__":
    TURBIDVTLupdateModel(*argv[1:])
    print("All done.  {0}  ".format(arcpy.GetMessages()))
