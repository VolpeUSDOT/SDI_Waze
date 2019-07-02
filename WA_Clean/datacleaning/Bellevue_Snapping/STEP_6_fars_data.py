
# ==================================================================================================
#
# Name:        fars_data.py
#
# Purpose:     Cross FARS data with grid cells. 
#
# Author:      Michelle Gilmore 
#
# Created:     03/26/2019
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

OUTPUT_DIR  = #Replace with path to Bellevue_Snapping folder

OUTPUT_FGDB = 'Bellevue_Snapping' + '.gdb'


## PRE-STEPS: MERGE INDIVIDUAL FARS TABLES INTO ONE TABLE IN fGDB AND CHANGE FUNCSYSTEM FIELD NAME. 


# ==================================================================================================
# METHODS 
# ==================================================================================================


def prepare_fars_data(fp_to_fars_tbl): 
    
    # Create FARS fc 
    print('\nCreate points layer from table')
    
    xy_sr = arcpy.SpatialReference(4326)
    
    arcpy.MakeXYEventLayer_management(
        fp_to_fars_tbl, "longitude", "latitude", "tmp_fars_lyr_full", xy_sr, "")
           
    
    prj_sr = arcpy.SpatialReference(6597)
    
    arcpy.Project_management("tmp_fars_lyr_full", fp_to_fars_full_fc, prj_sr)
    
    
    # CLEAN UP 
    # ----------  
    arcpy.Delete_management('tmp_fars_lyr_full')
    
    
    
def fars_in_aoi(fp_to_fars_full_fc):    

    # Clip FARS reports by city boundary
    print('\nClip FARS reports by City Boundary')

    fp_to_city_boundary_fc = os.path.join(fp_to_data_fgdb, 'CityBoundary')
    
    fars_clip_city = 'FARS_withinCityBoundary'
    fp_to_fars_clip_city = os.path.join(fp_to_data_fgdb, fars_clip_city) 
    
    arcpy.Clip_analysis(fp_to_fars_full_fc, fp_to_city_boundary_fc, fp_to_fars_clip_city)
    
    
    # subset to FARS reports which are under the jurisdiction of the City of Bellevue 
    print('\nSubset FARS reports to those under jurisdiction')
    arcpy.MakeFeatureLayer_management(fp_to_fars_clip_city, 'fars_temp')
    
    # Only select the roads which do not have I-405, I-90, and SR-520
    arcpy.SelectLayerByAttribute_management(
        'fars_temp', "NEW_SELECTION", 
        "trafid1 IN ('I-405', 'I-90', 'SR-520')", "INVERT")
        
    arcpy.CopyFeatures_management('fars_temp', fp_to_fars_jurisdiction)
    
    
    # CLEAN UP 
    # ----------  
    arcpy.Delete_management('fars_temp')
    
    

def generate_near_tbl(fp_to_fars_jurisdiction): 

    # Find closest road segments within 50ft of each FARS report 
    print('\nGenerate Near table of FARS reports within 50ft of Road segment')
    
    road_network_jurisdiction = 'RoadNetwork_Jurisdiction'
    fp_to_road_network_jurisdiction = os.path.join(fp_to_data_fgdb, road_network_jurisdiction) 
    
    fars_near_tbl = 'FARS_NearTable'
    
    # additional parameters 
    search_radius = '50 Feet'
    
    arcpy.GenerateNearTable_analysis(
        fp_to_fars_jurisdiction, fp_to_road_network_jurisdiction, fars_near_tbl, 
        "50 Feet", "LOCATION", "NO_ANGLE", "ALL", "", "GEODESIC")
        
    
    # Join the FARS reports' casenum, trafid1 fields to near table 
    arcpy.JoinField_management(fars_near_tbl, "IN_FID", fp_to_fars_jurisdiction, "OBJECTID", "casenum")
    arcpy.JoinField_management(fars_near_tbl, "IN_FID", fp_to_fars_jurisdiction, "OBJECTID", "trafid1")
    
   # Join the Road Network's RDSEG_ID and street name fields to the near table 
    arcpy.JoinField_management(fars_near_tbl, "NEAR_FID", fp_to_road_network_jurisdiction, "OBJECTID_1", "RDSEG_ID")
    arcpy.JoinField_management(fars_near_tbl, "NEAR_FID", fp_to_road_network_jurisdiction, "OBJECTID_1", "OfficialSt")
    arcpy.JoinField_management(fars_near_tbl, "NEAR_FID", fp_to_road_network_jurisdiction, "OBJECTID_1", "StreetSegm")


    # Join IN_FID to FARS fc 
    arcpy.JoinField_management(fp_to_fars_jurisdiction, "OBJECTID", fars_near_tbl, "IN_FID", "IN_FID")
    
    
    # Select records where IN_FID is not null and subset - these are the records which snapped and matched
    arcpy.MakeFeatureLayer_management(fp_to_fars_jurisdiction, 'fars_fc_temp')

    arcpy.SelectLayerByAttribute_management('fars_fc_temp', "NEW_SELECTION", "IN_FID IS NOT NULL", "")
    
    arcpy.CopyFeatures_management('fars_fc_temp', fp_to_fars_snapped_fc)
    
    
    # CLEAN UP 
    # ----------  
    arcpy.Delete_management('fars_fc_temp')

    
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
    
    
    fp_to_fars_tbl = os.path.join(fp_to_data_fgdb, 'FARS_2012_2017')
    fp_to_fars_full_fc = os.path.join(fp_to_data_fgdb, 'FARS_2012_2017_prj')
    fp_to_fars_jurisdiction = os.path.join(fp_to_data_fgdb, 'FARS_Jurisdiction')
    fp_to_fars_snapped_fc = os.path.join(fp_to_data_fgdb, 'FARS_Snapped50ft_MatchName')
    
    
    # CORE 
    # ----------
    prepare_fars_data(fp_to_fars_tbl)
    
    fars_in_aoi(fp_to_fars_full_fc)
    
    generate_near_tbl(fp_to_fars_jurisdiction)
    
    

    
    end_process_time = datetime.datetime.now()
    total_process_time = end_process_time - start_process_time
    print('\nEnd time = {}'.format(end_process_time))
    print('Total run time = {}'.format(total_process_time))