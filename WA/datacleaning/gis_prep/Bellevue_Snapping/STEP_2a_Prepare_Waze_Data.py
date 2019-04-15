
# ==================================================================================================
#
# Name:        STEP_2_Prepare_Waze_Data.py
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


def prepare_waze_data(fp_to_waze_export_csv):

    # Import CSV of Waze export from Dan into fGDB 
    print('\nCreate Waze full fc')
    arcpy.TableToTable_conversion(fp_to_waze_export_csv, fp_to_data_fgdb, 'Waze_Reports_full_tbl')
    
    # Create feature class of Waze events export 
    waze_reports_full = 'Waze_Reports_full'
    fp_to_waze_reports_full = os.path.join(fp_to_data_fgdb, waze_reports_full)
    
    xy_sr = arcpy.SpatialReference(4326)
    
    arcpy.management.XYTableToPoint('Waze_Reports_full_tbl', fp_to_waze_reports_full, 'lon', 'lat', "", xy_sr)
    
    
    # Project into projection of Bellevue Road network 
    print('\nProject Waze full fc into projection of Bellevue Road Network')
    prj_sr = arcpy.SpatialReference(6597)
    
    arcpy.Project_management(fp_to_waze_reports_full, fp_to_waze_reports_prj, prj_sr)
    
    

def waze_in_aoi(fp_to_waze_reports_prj):
    
    # Clip Waze reports by city boundary
    print('\nClip Waze reports by City Boundary')
    
    fp_to_city_boundary_fc = os.path.join(fp_to_data_fgdb, 'CityBoundary')

    waze_clip_city = 'Waze_withinCityBoundary'
    fp_to_waze_clip_city = os.path.join(fp_to_data_fgdb, waze_clip_city) 
    
    arcpy.Clip_analysis(fp_to_waze_reports_prj, fp_to_city_boundary_fc, fp_to_waze_clip_city)
    
    
    # subset to Waze reports which are under the jurisdiction of the City of Bellevue 
    print('\nSubset Waze reports to those under jurisdiction')
    arcpy.MakeFeatureLayer_management(fp_to_waze_clip_city, 'waze_temp')
    
    # Only select the roads which do not have I-405, I-90, SR-520, Exit, and 'to ' (with a space after to)
    arcpy.SelectLayerByAttribute_management(
        'waze_temp', "NEW_SELECTION", 
        "street LIKE '%I-405%' Or street LIKE '%I-90%' Or street LIKE '%SR-520%' Or street LIKE '%Exit%' Or street LIKE '%to %'", 
        "INVERT")
    
    arcpy.CopyFeatures_management('waze_temp', fp_to_waze_jurisdiction)
    
    
    # CLEAN UP 
    # ----------  
    arcpy.Delete_management('waze_temp')
    
    
    
def generate_near_tbl(fp_to_waze_jurisdiction): 

    # Find closest road segments within 50ft of each Waze report 
    print('\nGenerate Near table of Waze reports within 50ft of Road segment')
    
    road_network_jurisdiction = 'RoadNetwork_Jurisdiction'
    fp_to_road_network_jurisdiction = os.path.join(fp_to_data_fgdb, road_network_jurisdiction) 
    
    Waze_near_tbl = 'Waze_NearTable'
    
    # additional parameters 
    search_radius = '50 Feet'
    
    arcpy.GenerateNearTable_analysis(
        fp_to_waze_jurisdiction, fp_to_road_network_jurisdiction, Waze_near_tbl, 
        "50 Feet", "LOCATION", "NO_ANGLE", "ALL", "", "GEODESIC")
    
    
    # Join the Waze events' SDC_ID, street name, and alert type fields to near table 
    arcpy.JoinField_management(Waze_near_tbl, "IN_FID", fp_to_waze_jurisdiction, "OBJECTID", "SDC_uuid")
    arcpy.JoinField_management(Waze_near_tbl, "IN_FID", fp_to_waze_jurisdiction, "OBJECTID", "street")
    arcpy.JoinField_management(Waze_near_tbl, "IN_FID", fp_to_waze_jurisdiction, "OBJECTID", "alert_type")
    
    
    # Join the Road Network's RDSEG_ID and street name fields to the near table 
    arcpy.JoinField_management(Waze_near_tbl, "NEAR_FID", fp_to_road_network_jurisdiction, "OBJECTID_1", "RDSEG_ID")
    arcpy.JoinField_management(Waze_near_tbl, "NEAR_FID", fp_to_road_network_jurisdiction, "OBJECTID_1", "OfficialSt")
    arcpy.JoinField_management(Waze_near_tbl, "NEAR_FID", fp_to_road_network_jurisdiction, "OBJECTID_1", "StreetSegm")
    

    # Export Near Table to .TXT to then upload into SQL Server 
    fp_to_near_tbl_txt = os.path.join(OUTPUT_DIR, 'Waze_NearTable.txt')
    
    arcpy.CopyRows_management(Waze_near_tbl, fp_to_near_tbl_txt)



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
    
    
    fp_to_waze_export_csv = os.path.join(OUTPUT_DIR, 'WA_Bellevue_Prep_Export.csv')
    fp_to_waze_reports_prj = os.path.join(fp_to_data_fgdb, 'Waze_Reports_full_prj')
    fp_to_waze_jurisdiction = os.path.join(fp_to_data_fgdb, 'Waze_Jurisdiction')

    
    
    # CORE 
    # -----
    
    prepare_waze_data(fp_to_waze_export_csv)
    
    waze_in_aoi(fp_to_waze_reports_prj)
    
    generate_near_tbl(fp_to_waze_jurisdiction)
        
    
    
    end_process_time = datetime.datetime.now()
    total_process_time = end_process_time - start_process_time
    print('\nEnd time = {}'.format(end_process_time))
    print('Total run time = {}'.format(total_process_time))