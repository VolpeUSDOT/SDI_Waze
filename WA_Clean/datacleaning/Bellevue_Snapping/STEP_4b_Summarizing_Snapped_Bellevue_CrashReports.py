
# ==================================================================================================
#
# Name:        STEP_4b_Prepare_BellevueCrash_Data.py
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

def create_snapped_crashes_fc(fp_to_crashes_near_updated, fp_to_crashes_snapped_fc):

    # Join IN_FID to full Bellevue crashes fc 
    fp_to_crashes_jurisdiction = os.path.join(fp_to_data_fgdb, 'Crashes_Jurisdiction')
    
    arcpy.JoinField_management(fp_to_crashes_jurisdiction, "OBJECTID", fp_to_crashes_near_updated, "IN_FID", "IN_FID")
    
    
    # Select records where IN_FID is not null and subset - these are the records which snapped and matched
    arcpy.MakeFeatureLayer_management(fp_to_crashes_jurisdiction, 'crashes_fc_temp')

    arcpy.SelectLayerByAttribute_management('crashes_fc_temp', "NEW_SELECTION", "IN_FID IS NOT NULL", "")
    
    arcpy.CopyFeatures_management('crashes_fc_temp', fp_to_crashes_snapped_fc)
    
    
    # Join road network fields to snapped Bellevue Crash reports fc 
    arcpy.JoinField_management(fp_to_crashes_snapped_fc, "IN_FID", fp_to_crashes_near_updated, "IN_FID", "RDSEG_ID")
    arcpy.JoinField_management(fp_to_crashes_snapped_fc, "IN_FID", fp_to_crashes_near_updated, "IN_FID", "StreetSegm")
    arcpy.JoinField_management(fp_to_crashes_snapped_fc, "IN_FID", fp_to_crashes_near_updated, "IN_FID", "OfficialSt")
    arcpy.JoinField_management(fp_to_crashes_snapped_fc, "IN_FID", fp_to_crashes_near_updated, "IN_FID", "INDEXED_PRIMARY_TRAFFICWAY_updated")
    
    
    # Calculate hour of day and minute of day of Bellevue Crash report 
    arcpy.AddField_management(fp_to_crashes_snapped_fc, "HourOfDay", "LONG", "", "")
    arcpy.AddField_management(fp_to_crashes_snapped_fc, "MinOfDay", "LONG", "", "")
    
    arcpy.CalculateField_management(fp_to_crashes_snapped_fc, "HourOfDay", '!F24_HR_TIME!.split(":")[0]', "PYTHON3", "")
    arcpy.CalculateField_management(fp_to_crashes_snapped_fc, "MinOfDay", '!F24_HR_TIME!.split(":")[1]', "PYTHON3", "") 
    
    
    # Delete unnecessary fields
    crash_flds = arcpy.ListFields(fp_to_crashes_jurisdiction)
    drop_crash_flds = ['IN_FID', 'IN_FID_1']
    arcpy.DeleteField_management(fp_to_crashes_jurisdiction, [x.name for x in crash_flds if x.name in drop_crash_flds])
    arcpy.DeleteField_management(fp_to_crashes_snapped_fc, [x.name for x in crash_flds if x.name in drop_crash_flds])
    
    
    # CLEAN UP 
    # ----------  
    arcpy.Delete_management('crashes_fc_temp')



def crash_summary(fp_to_road_network_all_data):
    
    # Count the number of Bellevue Crash Reports on each road segment 
    print('\nUse summary statistics to count the number of Bellevue Crash reports on each road segement')
    
    
    arcpy.Statistics_analysis(
        fp_to_crashes_near_updated, 'Crashes_NearTable_updated_Stats', [["IN_FID", "COUNT"]], "RDSEG_ID")
        
        
    # add count of Bellevue Crash reports per road segment to the Road Network with data fc. 
    print('\nAdd count of Bellevue Crash Reports per road segment to Road Network fc')
    
    arcpy.JoinField_management(fp_to_road_network_all_data, "RDSEG_ID", 
        'Crashes_NearTable_updated_Stats', "RDSEG_ID", "COUNT_IN_FID")
        
        
    # Update field name 
    arcpy.AlterField_management(
        fp_to_road_network_all_data, "COUNT_IN_FID", 
        "nCrashes_50ft", "nCrashes_50ft", 
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
    
    
    fp_to_crashes_near_updated = os.path.join(fp_to_data_fgdb, 'Crashes_NearTable_updated')
    fp_to_crashes_snapped_fc = os.path.join(fp_to_data_fgdb, 'Crashes_Snapped50ft_MatchName')
    fp_to_road_network_all_data = os.path.join(fp_to_data_fgdb, 'RoadNetwork_Jurisdiction_withData')
    
    
    
    # CORE
    # -----
    
    create_snapped_crashes_fc(fp_to_crashes_near_updated, fp_to_crashes_snapped_fc)
    
    crash_summary(fp_to_road_network_all_data)
    
    
    
    end_process_time = datetime.datetime.now()
    total_process_time = end_process_time - start_process_time
    print('\nEnd time = {}'.format(end_process_time))
    print('Total run time = {}'.format(total_process_time))  
