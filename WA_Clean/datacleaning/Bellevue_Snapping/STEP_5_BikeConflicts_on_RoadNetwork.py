
# ==================================================================================================
#
# Name:        STEP_5_BikeConflicts_on_Network.py
#
# Purpose:     Assign bike conflict reports to a road network segment. 
#
# Author:      Michelle Gilmore 
#
# Created:     03/25/2019
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


def snap_bikes_to_road(fp_to_bike_proj_fc, fp_to_road_network_jurisdiction):

    # Project into projection of Bellevue Road network 
    print('\nProject Bike Conflict full fc into projection of Bellevue Road Network')
    prj_sr = arcpy.SpatialReference(6597)
    
    fp_to_bike_raw_fc = os.path.join(fp_to_data_fgdb, 'Wikimap_raw')
    
    arcpy.Project_management(fp_to_bike_raw_fc, fp_to_bike_proj_fc, prj_sr)
    
    
    # Snap bike conflict points to closest road segment within 50ft.   
    search_radius = '50 Feet'
    
    arcpy.GenerateNearTable_analysis(fp_to_bike_proj_fc, fp_to_road_network_jurisdiction, fp_to_bike_near_tbl, 
        search_radius, "LOCATION", "NO_ANGLE", "CLOSEST", "", "GEODESIC")
    
    
        
def join_parameters(fp_to_bike_near_tbl, fp_to_road_network_jurisdiction):

    # Join the ID from the Bike Conflict fc  
    arcpy.JoinField_management(fp_to_bike_near_tbl, "IN_FID", fp_to_bike_proj_fc, "OBJECTID", "id")
    
    # Join the Road Network's RDSEG_ID and street name fields to the near table 
    arcpy.JoinField_management(fp_to_bike_near_tbl, "NEAR_FID", fp_to_road_network_jurisdiction, "OBJECTID_1", "RDSEG_ID")
    arcpy.JoinField_management(fp_to_bike_near_tbl, "NEAR_FID", fp_to_road_network_jurisdiction, "OBJECTID_1", "OfficialSt")
    arcpy.JoinField_management(fp_to_bike_near_tbl, "NEAR_FID", fp_to_road_network_jurisdiction, "OBJECTID_1", "StreetSegm")



def create_snapped_bikes_fc(fp_to_bike_proj_fc):

    # Join IN_FID to full Bike Conflict fc 
    arcpy.JoinField_management(fp_to_bike_proj_fc, "OBJECTID", fp_to_bike_near_tbl, "IN_FID", "IN_FID")
    
    
    # Select records where IN_FID is not null and subset - these are the records which snapped and matched
    arcpy.MakeFeatureLayer_management(fp_to_bike_proj_fc, 'bikes_fc_temp')
    
    arcpy.SelectLayerByAttribute_management('bikes_fc_temp', "NEW_SELECTION", "IN_FID IS NOT NULL", "")
    
    arcpy.CopyFeatures_management('bikes_fc_temp', fp_to_bikes_snapped_fc)
    
    
    # Join road network fields to snapped Bellevue Crash reports fc 
    arcpy.JoinField_management(fp_to_bikes_snapped_fc, "IN_FID", fp_to_bike_near_tbl, "IN_FID", "RDSEG_ID")
    arcpy.JoinField_management(fp_to_bikes_snapped_fc, "IN_FID", fp_to_bike_near_tbl, "IN_FID", "OfficialSt")
    arcpy.JoinField_management(fp_to_bikes_snapped_fc, "IN_FID", fp_to_bike_near_tbl, "IN_FID", "StreetSegm")
    
    
    
def add_bikes_to_network(fp_to_road_network_all_data): 

    # Count the number of Bellevue Crash Reports on each road segment 
    print('\nUse summary statistics to count the number of Bike Conflicts reports on each road segement')
    
    arcpy.Statistics_analysis(
        fp_to_bike_near_tbl, 'Wikimap_NearTable_Stats', [["IN_FID", "COUNT"]], "RDSEG_ID")
        
    
    # add count of Bellevue Crash reports per road segment to the Road Network with data fc. 
    print('\nAdd count of Bellevue Crash Reports per road segment to Road Network fc')
    
    arcpy.JoinField_management(fp_to_road_network_all_data, "RDSEG_ID", 
        'Wikimap_NearTable_Stats', "RDSEG_ID", "COUNT_IN_FID")
        
        
    # Update field name 
    arcpy.AlterField_management(
        fp_to_road_network_all_data, "COUNT_IN_FID", 
        "nBikes_50ft", "nBikes_50ft", 
        "LONG", "", "NULLABLE", "") 



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
    
    
    
    fp_to_bike_proj_fc = os.path.join(fp_to_data_fgdb, 'Wikimap_raw_prj')
    fp_to_road_network_jurisdiction = os.path.join(fp_to_data_fgdb, 'RoadNetwork_Jurisdiction') 
    fp_to_bike_near_tbl = os.path.join(fp_to_data_fgdb, 'Wikimap_NearTable')
    fp_to_bikes_snapped_fc = os.path.join(fp_to_data_fgdb, 'Wikimap_Snapped50ft')
    fp_to_road_network_all_data = os.path.join(fp_to_data_fgdb, 'RoadNetwork_Jurisdiction_withData')
    
    
    # CORE
    # -----
    #snap_bikes_to_road(fp_to_bike_proj_fc, fp_to_road_network_jurisdiction)
    
    #join_parameters(fp_to_bike_near_tbl, fp_to_road_network_jurisdiction)
    
    #create_snapped_bikes_fc(fp_to_bike_proj_fc)
    
    add_bikes_to_network(fp_to_road_network_all_data)

    
    
    
    end_process_time = datetime.datetime.now()
    total_process_time = end_process_time - start_process_time
    print('\nEnd time = {}'.format(end_process_time))
    print('Total run time = {}'.format(total_process_time)) 