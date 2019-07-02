
# ==================================================================================================
#
# Name:        STEP_4a_Prepare_BellevueCrash_Data.py
#
# Purpose:     Prepare Bellevue Crash data for snapping to Road Network. 
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


def preapre_snapped_waze(fp_to_Waze_near_updated, fp_to_road_network_jurisdiction):

    # Use the NEAR_X and NEAR_Y fields to display the location of the Waze events snapped to the network. 
    # First bring the XY points in using the geographic coordinate system of the Waze layer (WKID: 6318).
    # Then project the layer into the same projection as the road network (WKID: 6597). 
    
    waze_snapped_temp = 'Waze_Reports_snapped_temp'
    
    xy_sr = arcpy.SpatialReference(6318)
    
    arcpy.management.XYTableToPoint(fp_to_Waze_near_updated, waze_snapped_temp, 'NEAR_X', 'NEAR_Y', "", xy_sr)
    
    # project into projection of Bellevue Road network 
    print('\nProject Waze full fc into projection of Bellevue Road Network')
    prj_sr = arcpy.SpatialReference(6597)
    
    arcpy.Project_management(waze_snapped_temp, fp_to_waze_near_tbl_fc, prj_sr)
    
           
    # CLEAN UP 
    # ----------  
    arcpy.Delete_management(waze_snapped_temp)
    
    
    
def create_snapped_waze_fc_full(fp_to_waze_near_tbl_fc, fp_to_waze_snapped_fc):

    print('\nCreate snapped Waze fc with all fields from original fc')
    # Create a copy of near tbl fc 
    arcpy.CopyFeatures_management(fp_to_waze_near_tbl_fc, fp_to_waze_snapped_fc)
    
    
    # Delete all fields except IN_FID 
    near_flds = arcpy.ListFields(fp_to_waze_snapped_fc)
    drop_near_flds = ['NEAR_FID', 'NEAR_DIST', 'NEAR_RANK', 'FROM_X', 'FROM_Y', 'NEAR_X', 'NEAR_Y', 'SDC_uuid', 'street', 'alert_type', 'RDSEG_ID', 'OfficialSt', 'StreetSegm', 'street_updated']
    arcpy.DeleteField_management(fp_to_waze_snapped_fc, [x.name for x in near_flds if x.name in drop_near_flds])
    
    
    # Join fields from Waze layer 
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_jurisdiction, "OBJECTID", "lat")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_jurisdiction, "OBJECTID", "lon")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_jurisdiction, "OBJECTID", "alert_type")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_jurisdiction, "OBJECTID", "time")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_jurisdiction, "OBJECTID", "roadclass")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_jurisdiction, "OBJECTID", "SDC_uuid")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_jurisdiction, "OBJECTID", "sub_type")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_jurisdiction, "OBJECTID", "city")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_jurisdiction, "OBJECTID", "street")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_jurisdiction, "OBJECTID", "magvar")
    
    
    # Join fields back in from Near Table updated fc 
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_near_tbl_fc, "IN_FID", "NEAR_X")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_near_tbl_fc, "IN_FID", "NEAR_Y")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_near_tbl_fc, "IN_FID", "RDSEG_ID")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_near_tbl_fc, "IN_FID", "StreetSegm")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_near_tbl_fc, "IN_FID", "OfficialSt")
    arcpy.JoinField_management(fp_to_waze_snapped_fc, "IN_FID", fp_to_waze_near_tbl_fc, "IN_FID", "street_updated")
    
    
    # Delete unnecessary fields
    waze_flds = arcpy.ListFields(fp_to_waze_snapped_fc)
    drop_waze_flds = ['IN_FID']
    arcpy.DeleteField_management(fp_to_waze_snapped_fc, [x.name for x in waze_flds if x.name in drop_waze_flds])
    
    
    # Calculate hour of day of event 
    arcpy.ConvertTimeField_management(fp_to_waze_snapped_fc, "time", "Not Used", "HourOfDay", "LONG", "HH")
   
    # Calculate minute of day of event 
    arcpy.ConvertTimeField_management(fp_to_waze_snapped_fc, "time", "Not Used", "MinOfDay", "LONG", "mm")    
   
   
   
def create_snapped_waze_fc_accidents(fp_to_waze_snapped_fc, fp_to_Waze_snapped_accidents):   
   
    # Create fc which is only snapped Waze accidents
    print('\nSubset snapped Waze fc to only Accidents')
    
    arcpy.MakeFeatureLayer_management(fp_to_waze_snapped_fc, 'waze_fc_temp')
    
    arcpy.SelectLayerByAttribute_management('waze_fc_temp', "NEW_SELECTION", "alert_type = 'ACCIDENT'", "")
  
    arcpy.CopyFeatures_management('waze_fc_temp', fp_to_Waze_snapped_accidents)
    
    
    # CLEAN UP 
    # ----------  
    arcpy.Delete_management('waze_fc_temp')

    

def waze_summary_full(fp_to_waze_near_tbl_fc, fp_to_road_network_all_data): 

    # Count the number of Waze events on each road segment 
    print('\nUse summary statistics to count the number of Waze events on each road segement')
    
    arcpy.Statistics_analysis(
        fp_to_waze_near_tbl_fc, 'Waze_NearTable_updated_Stats', [["SDC_uuid", "COUNT"]], "RDSEG_ID")
        
    
    # make a copy of the RoadNetwork_Jurisdiction fc to add the summary data fields too 
    arcpy.CopyFeatures_management(fp_to_road_network_jurisdiction, fp_to_road_network_all_data)
    
    
    # add count of Waze events per road segment to the Road Network with data fc. 
    print('\nAdd count of Waze events per road segment to Road Network fc')
    
    arcpy.JoinField_management(fp_to_road_network_all_data, "RDSEG_ID", 
        'Waze_NearTable_updated_Stats', "RDSEG_ID", "COUNT_SDC_uuid")
        
        
    # Update field name 
    arcpy.AlterField_management(
        fp_to_road_network_all_data, "COUNT_SDC_uuid", 
        "nWazeEvents_50ft", "nWazeEvents_50ft", 
        "LONG", "", "NULLABLE", "") 
        
        

def waze_summary_accidents(fp_to_waze_near_tbl_fc, fp_to_road_network_all_data):  
    
    # Count the Waze events which are only ACCIDENTS per road segment 
    print('\nUse summary statistics to count the number of Waze Accidents on each road segement')
    arcpy.MakeFeatureLayer_management(fp_to_waze_near_tbl_fc, 'waze_temp')
    
    arcpy.SelectLayerByAttribute_management(
        'waze_temp', "NEW_SELECTION", "alert_type = 'ACCIDENT'", "") 
    
    arcpy.Statistics_analysis(
        'waze_temp', 'Waze_NearTable_updated_Stats_Accidents', [["SDC_uuid", "COUNT"]], "RDSEG_ID")
        
    
    # add count of Waze Accidents per road segment to the Road Network with data fc. 
    print('\nAdd count of Waze Accidents per road segment to Road Network fc')
    
    arcpy.JoinField_management(fp_to_road_network_all_data, "RDSEG_ID", 
        'Waze_NearTable_updated_Stats_Accidents', "RDSEG_ID", "COUNT_SDC_uuid")
        
        
    # Update field name 
    arcpy.AlterField_management(
        fp_to_road_network_all_data, "COUNT_SDC_uuid", 
        "nWazeAccidents_50ft", "nWazeAccidents_50ft", 
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
    
    
    fp_to_Waze_near_updated = os.path.join(fp_to_data_fgdb, 'Waze_NearTable_updated')
    fp_to_road_network_jurisdiction = os.path.join(fp_to_data_fgdb, 'RoadNetwork_Jurisdiction') 
    fp_to_waze_near_tbl_fc = os.path.join(fp_to_data_fgdb, 'Waze_NearTable_updated_fc')
    fp_to_waze_jurisdiction = os.path.join(fp_to_data_fgdb, 'Waze_Jurisdiction')
    fp_to_waze_snapped_fc = os.path.join(fp_to_data_fgdb, 'Waze_Snapped50ft_MatchName')
    fp_to_Waze_snapped_accidents = os.path.join(fp_to_data_fgdb, 'Waze_Snapped50ft_MatchName_Accidents')
    fp_to_road_network_all_data = os.path.join(fp_to_data_fgdb, 'RoadNetwork_Jurisdiction_withData')
    
    
    
    # CORE
    # -----
    
    preapre_snapped_waze(fp_to_Waze_near_updated, fp_to_road_network_jurisdiction)
    
    create_snapped_waze_fc_full(fp_to_waze_near_tbl_fc, fp_to_waze_snapped_fc)
    
    create_snapped_waze_fc_accidents(fp_to_waze_snapped_fc, fp_to_Waze_snapped_accidents)
    
    waze_summary_full(fp_to_waze_near_tbl_fc, fp_to_road_network_all_data)
    
    waze_summary_accidents(fp_to_waze_near_tbl_fc, fp_to_road_network_all_data)


    end_process_time = datetime.datetime.now()
    total_process_time = end_process_time - start_process_time
    print('\nEnd time = {}'.format(end_process_time))
    print('Total run time = {}'.format(total_process_time))    