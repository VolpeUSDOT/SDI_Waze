
# ==================================================================================================
#
# Name:        STEP_2b_Prepare_BellevueCrash_Data.py
#
# Purpose:     Prepare Waze data for snapping to Road Network. 
#
# Author:      Michelle Gilmore 
#
# Created:     03/18/2019
#
# Version:     1.0
#
# ==================================================================================================
# IMPORT REQUIRED MODULES
# ==================================================================================================

import arcpy
import datetime
import os
import sys
import time 

# ==================================================================================================
# CONFIG SECTION
# ==================================================================================================

OUTPUT_DIR  = 'D:\Tasks\OST_waze_phase2\Bellevue_Snapping'

OUTPUT_FGDB = 'Bellevue_Snapping' + '.gdb'


# ==================================================================================================
# METHODS 
# ==================================================================================================


def prepare_bellevue_data(fp_to_bellevue_export_csv):

    # Import CSV of Bellevue Crash export from Dan into fGDB 
    print('\nCreate Bellevue Crash full fc')
    arcpy.TableToTable_conversion(fp_to_bellevue_export_csv, fp_to_data_fgdb, 'Crash_Reports_full_tbl')
    
    # Create feature class of Bellevue Crash export 
    crash_reports_full = 'Crash_Reports_full'
    fp_to_crash_reports_full = os.path.join(fp_to_data_fgdb, crash_reports_full)
    
    xy_sr = arcpy.SpatialReference(6599)
    
    arcpy.management.XYTableToPoint('Crash_Reports_full_tbl', fp_to_crash_reports_full, 
        'WA_STATE_PLANE_SOUTH___X', 'WA_STATE_PLANE_SOUTH___Y', "", xy_sr)
        
        
    # Project into projection of Bellevue Road network 
    print('\nProject Bellevue Crash full fc into projection of Bellevue Road Network')
    prj_sr = arcpy.SpatialReference(6597)
    
    arcpy.Project_management(fp_to_crash_reports_full, fp_to_crash_reports_prj, prj_sr)
    
    

def crashes_in_aoi(fp_to_crash_reports_prj):

    # Clip Waze reports by city boundary
    print('\nClip Bellevue Crash reports by City Boundary')
    
    fp_to_city_boundary_fc = os.path.join(fp_to_data_fgdb, 'CityBoundary')
    
    crashes_clip_city = 'Crashes_withinCityBoundary'
    fp_to_crashes_clip_city = os.path.join(fp_to_data_fgdb, crashes_clip_city) 
    
    arcpy.Clip_analysis(fp_to_crash_reports_prj, fp_to_city_boundary_fc, fp_to_crashes_clip_city)
    
    
    # subset to Bellevue Crash reports which are under the jurisdiction of the City of Bellevue 
    print('\nSubset Bellevue Crash reports to those under jurisdiction')
    arcpy.MakeFeatureLayer_management(fp_to_crashes_clip_city, 'crashes_temp')
    
    # Only select Bellevue Crash reports which occurred on a City Street
    arcpy.SelectLayerByAttribute_management('crashes_temp', "NEW_SELECTION", "JURISDICTION = 'City Street'", "")

    arcpy.CopyFeatures_management('crashes_temp', fp_to_crashes_jurisdiction)
    
    
    # CLEAN UP 
    # ----------  
    arcpy.Delete_management('crashes_temp')
    
    
    
def generate_near_tbl(fp_to_crashes_jurisdiction):

    # Find closest road segments within 50ft of each Bellevue Crash report 
    print('\nGenerate Near table of Bellevue Crash reports within 50ft of Road segment')
    
    road_network_jurisdiction = 'RoadNetwork_Jurisdiction'
    fp_to_road_network_jurisdiction = os.path.join(fp_to_data_fgdb, road_network_jurisdiction) 
    
    crashes_near_tbl = 'Crashes_NearTable'

    # additional parameters 
    search_radius = '50 Feet'
    
    arcpy.GenerateNearTable_analysis(
        fp_to_crashes_jurisdiction, fp_to_road_network_jurisdiction, crashes_near_tbl, 
        "50 Feet", "LOCATION", "NO_ANGLE", "ALL", "", "GEODESIC")
        
    
    # Join the Bellevue Crash reports report number, indexed primary trafficway, and primary trafficway fields to near table 
    arcpy.JoinField_management(crashes_near_tbl, "IN_FID", fp_to_crashes_jurisdiction, "OBJECTID", "REPORT_NUMBER")
    arcpy.JoinField_management(crashes_near_tbl, "IN_FID", fp_to_crashes_jurisdiction, "OBJECTID", "INDEXED_PRIMARY_TRAFFICWAY")
    arcpy.JoinField_management(crashes_near_tbl, "IN_FID", fp_to_crashes_jurisdiction, "OBJECTID", "PRIMARY_TRAFFICWAY")

    # Join the Road Network's RDSEG_ID and street name, and StreetSegm fields to the near table 
    arcpy.JoinField_management(crashes_near_tbl, "NEAR_FID", fp_to_road_network_jurisdiction, "OBJECTID_1", "RDSEG_ID")
    arcpy.JoinField_management(crashes_near_tbl, "NEAR_FID", fp_to_road_network_jurisdiction, "OBJECTID_1", "OfficialSt")
    arcpy.JoinField_management(crashes_near_tbl, "NEAR_FID", fp_to_road_network_jurisdiction, "OBJECTID_1", "StreetSegm")


    # Export Near Table to .TXT to then upload into SQL Server 
    fp_to_near_tbl_txt = os.path.join(OUTPUT_DIR, 'Crashes_NearTable.txt')
    
    arcpy.CopyRows_management(crashes_near_tbl, fp_to_near_tbl_txt)



# ==================================================================================================
# MAIN
# ==================================================================================================

if __name__ == "__main__":

    start_process_time = datetime.datetime.now()
    print('\nStart time = {}'.format(start_process_time))
    

    fp_to_data_fgdb = os.path.join(OUTPUT_DIR, OUTPUT_FGDB)

    arcpy.env.workspace = fp_to_data_fgdb
    
    # Field names will NOT include table name 
    arcpy.env.qualifiedFieldNames = False


    fp_to_bellevue_export_csv = os.path.join(OUTPUT_DIR, '20190314_All_roads_Bellevue.csv')
    fp_to_crash_reports_prj = os.path.join(fp_to_data_fgdb, 'Crash_Reports_full_prj')
    fp_to_crashes_jurisdiction = os.path.join(fp_to_data_fgdb, 'Crashes_Jurisdiction')
    
    
    # CORE 
    # -----

    prepare_bellevue_data(fp_to_bellevue_export_csv)
    
    crashes_in_aoi(fp_to_crash_reports_prj)
    
    generate_near_tbl(fp_to_crashes_jurisdiction)
    
    
    
    end_process_time = datetime.datetime.now()
    total_process_time = end_process_time - start_process_time
    print('\nEnd time = {}'.format(end_process_time))
    print('Total run time = {}'.format(total_process_time))