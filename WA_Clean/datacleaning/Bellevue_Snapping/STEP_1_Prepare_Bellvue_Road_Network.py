
# ==================================================================================================
#
# Name:        STEP_1_Prepare_Bellvue_Road_Network.py
#
# Purpose:     Prepare Bellevue Road Network for snapping. 
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


OUTPUT_DIR  = #Replace with path to Bellevue_Snapping folder

OUTPUT_FGDB = 'Bellevue_Snapping' + '.gdb'


# ==================================================================================================
# METHODS 
# ==================================================================================================


def prepare_road_network(fp_to_road_network_raw, fp_to_city_boundary_fc):
    

    # Clip road network by city boundary
    print('\nClip Road Network by City Boundary')
    
    arcpy.MakeFeatureLayer_management(fp_to_road_network_raw, 'road_full_fc_tmp')
    
    arcpy.management.SelectLayerByLocation('road_full_fc_tmp', "INTERSECT", fp_to_city_boundary_fc, "", "NEW_SELECTION", "")
    
    road_network_clip_city = 'RoadNetwork_withinCityBoundary'
    fp_to_road_network_clip_city = os.path.join(fp_to_data_fgdb, road_network_clip_city) 
    
    arcpy.CopyFeatures_management('road_full_fc_tmp', fp_to_road_network_clip_city)


    print('\nClip Road Network by to roads within jurisdiction')
    # subset to roads which are under the jurisdiction of the City of Bellevue 
    arcpy.MakeFeatureLayer_management(fp_to_road_network_clip_city, 'road_network_fc_tmp')
    
    # Only select the roads which do not have I-405, I-90, and SR-520
    arcpy.SelectLayerByAttribute_management('road_network_fc_tmp', "NEW_SELECTION", 
        "OfficialSt LIKE '%I-405%' Or OfficialSt LIKE '%I-90%' Or OfficialSt LIKE '%SR-520%'", "INVERT")
    
    road_network_jurisdiction = 'RoadNetwork_Jurisdiction'
    fp_to_road_network_jurisdiction = os.path.join(fp_to_data_fgdb, road_network_jurisdiction) 
    
    arcpy.CopyFeatures_management('road_network_fc_tmp', fp_to_road_network_jurisdiction)
    
    
    # CLEAN UP 
    # ----------  
    arcpy.Delete_management('road_network_fc_tmp')



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
    
    
    fp_to_road_network_raw = os.path.join(fp_to_data_fgdb, 'RoadNetwork')
    fp_to_city_boundary_fc = os.path.join(fp_to_data_fgdb, 'CityBoundary')
    
    
    
    # CORE 
    # -----
    prepare_road_network(fp_to_road_network_raw, fp_to_city_boundary_fc)
    
    
    
    end_process_time = datetime.datetime.now()
    total_process_time = end_process_time - start_process_time
    print('\nEnd time = {}'.format(end_process_time))
    print('Total run time = {}'.format(total_process_time))