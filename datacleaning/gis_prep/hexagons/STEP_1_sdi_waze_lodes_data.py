
# ==================================================================================================
#
# Name:        sdi_waze_lodes_data.py
#
# Purpose:     Cross LEHD LODES data with grid cells. 
#
# Author:      Michelle Gilmore 
#
# Created:     09/01/2018
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

DATA_DIR = 'D:\Tasks\OST_waze_phase2\data\geodatabases'

SHP_DIR  = 'D:\Tasks\OST_waze_phase2\data\shapefiles'

UPPER_STATE = 'TN' 

LOWER_STATE = 'tn'

# HEXAGON_FC = UPPER_STATE + '_hexagons_1mi' # CT, MD, UT, VA 

# HEXAGON_FC = UPPER_STATE + '_1sqmile_hexagons' # TN hexagons 
HEXAGON_FC = UPPER_STATE + '_01dd_fishnet' # TN fishnet  

# BG_FC_NAME = 'cb_2015_09_bg_500k_prj.shp' # CT
# BG_FC_NAME = 'cb_2015_24_bg_500k_prj.shp' # MD
# BG_FC_NAME = 'cb_2015_49_bg_500k_prj.shp' # UT
# BG_FC_NAME = 'cb_2015_51_bg_500k_prj.shp' # VA


## PRE-STEPS: SUMMARIZE BLOCK-LEVEL DATA FROM LEHD-LODES WEBSITE INTO BLOCK GROUPS. 


# ==================================================================================================
# METHODS 
# ==================================================================================================


def get_lodes_rac(fp_to_rac_tbl): 
    
    # join block group fc and summarized RAC table 
    # print('\nCreating bg_rac_fc')
    
    # bg_fc = os.path.join(SHP_DIR, BG_FC_NAME)
    # temp_bg_fc = 'bg_tmp'

    # arcpy.MakeFeatureLayer_management(bg_fc, temp_bg_fc)
    
    # arcpy.AddJoin_management(temp_bg_fc, 'GEOID', fp_to_rac_tbl, 'bg_id') 
    
    fp_to_rac_fc = os.path.join(fp_to_data_fgdb, 'bg_lodes_rac')
    # arcpy.CopyFeatures_management(temp_bg_fc, fp_to_rac_fc)
    
    # arcpy.RemoveJoin_management(temp_bg_fc) 

    
    # # Calculate sq. meters of block group 
    # print('Calculating block group sq. meters')
    
    # arcpy.AddField_management(fp_to_rac_fc, 'SQ_METERS_BG', 'DOUBLE')
    
    # arcpy.CalculateField_management(
        # fp_to_rac_fc, "SQ_METERS_BG",  "!SHAPE.AREA@SQUAREMETERS!", "PYTHON")
        
    
    # intersect RAC-block group and hexagons 
    print('Intersecting block group fc and hexagons')
        
    fp_to_hexagons_fc = os.path.join(fp_to_data_fgdb, HEXAGON_FC)
    
    fp_to_rac_intersect_fc = os.path.join(fp_to_data_fgdb, HEXAGON_FC + '_bg_rac_intersect')
    
    arcpy.Intersect_analysis(
        [fp_to_hexagons_fc, fp_to_rac_fc], fp_to_rac_intersect_fc, 'ALL', "", 'INPUT') 
        
        
    # Calculate sq. meters of block group-hexagon intersected area 
    print('Calculating sq. meters of intersected area')
    
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'SQ_METERS_INTERSECT', 'DOUBLE')
    
    arcpy.CalculateField_management(
        fp_to_rac_intersect_fc, "SQ_METERS_INTERSECT",  "!SHAPE.AREA@SQUAREMETERS!", "PYTHON")
        
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'PRCT_BG_INTERSECT', 'DOUBLE')
    
    arcpy.CalculateField_management(
        fp_to_rac_intersect_fc, "PRCT_BG_INTERSECT",  "!SQ_METERS_INTERSECT! / !SQ_METERS_BG!", "PYTHON")
        
    
    # Calculate the proportion of each block group's RAC population after intersect 
    print('Calculating field values based on intersect proportion')
    
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'COUNT_C000', 'DOUBLE')
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'COUNT_CA01', 'DOUBLE')
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'COUNT_CA02', 'DOUBLE')
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'COUNT_CA03', 'DOUBLE')
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'COUNT_CE01', 'DOUBLE')
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'COUNT_CE02', 'DOUBLE')
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'COUNT_CE03', 'DOUBLE')
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'COUNT_CD01', 'DOUBLE')
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'COUNT_CD02', 'DOUBLE')
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'COUNT_CD03', 'DOUBLE')
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'COUNT_CD04', 'DOUBLE')
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'COUNT_CS01', 'DOUBLE')
    arcpy.AddField_management(fp_to_rac_intersect_fc, 'COUNT_CS02', 'DOUBLE')
    
    arcpy.CalculateField_management(fp_to_rac_intersect_fc, "COUNT_C000",  "!C000! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_rac_intersect_fc, "COUNT_CA01",  "!CA01! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_rac_intersect_fc, "COUNT_CA02",  "!CA02! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_rac_intersect_fc, "COUNT_CA03",  "!CA03! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_rac_intersect_fc, "COUNT_CE01",  "!CE01! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_rac_intersect_fc, "COUNT_CE02",  "!CE02! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_rac_intersect_fc, "COUNT_CE03",  "!CE03! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_rac_intersect_fc, "COUNT_CD01",  "!CD01! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_rac_intersect_fc, "COUNT_CD02",  "!CD02! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_rac_intersect_fc, "COUNT_CD03",  "!CD03! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_rac_intersect_fc, "COUNT_CD04",  "!CD04! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_rac_intersect_fc, "COUNT_CS01",  "!CS01! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_rac_intersect_fc, "COUNT_CS02",  "!CS02! * !PRCT_BG_INTERSECT!", "PYTHON")
    
    
    # Dissolve intersected block groups-hexagons by grid cell IDs 
    print('Dissolving based on hexagon grid IDs')
    
    fp_to_rac_dissolve_fc = os.path.join(fp_to_data_fgdb, HEXAGON_FC + '_bg_rac_dissolve')
    
    arcpy.Dissolve_management(
        fp_to_rac_intersect_fc, fp_to_rac_dissolve_fc, "GRID_ID",
        [
            ["COUNT_C000", "SUM"], 
            ["COUNT_CA01", "SUM"], 
            ["COUNT_CA02", "SUM"],
            ["COUNT_CA03", "SUM"], 
            ["COUNT_CE01", "SUM"], 
            ["COUNT_CE02", "SUM"], 
            ["COUNT_CE03", "SUM"], 
            ["COUNT_CD01", "SUM"],
            ["COUNT_CD02", "SUM"], 
            ["COUNT_CD03", "SUM"], 
            ["COUNT_CD04", "SUM"], 
            ["COUNT_CS01", "SUM"],
            ["COUNT_CS02", "SUM"]
        ], "", "")
        
    
    # Join dissolved data to full hexagons fc 
    print('Joining with full hexagon fc')
    
    temp_hex_fc = 'hex_temp'
    
    arcpy.MakeFeatureLayer_management(fp_to_hexagons_fc, temp_hex_fc)
    
    arcpy.AddJoin_management(temp_hex_fc, 'GRID_ID', fp_to_rac_dissolve_fc, 'GRID_ID')
     
    fp_to_rac_sum_fc = os.path.join(fp_to_data_fgdb, HEXAGON_FC + '_bg_rac_sum')
    
    arcpy.CopyFeatures_management(temp_hex_fc, fp_to_rac_sum_fc)
            
    arcpy.RemoveJoin_management(temp_hex_fc)
    
    
    # CLEAN UP 
    # ----------
    rac_fc_flds = arcpy.ListFields(fp_to_rac_sum_fc)
    drop_flds_rac = ['GRID_ID_1', 'OBJECTID_1']
    arcpy.DeleteField_management(
        fp_to_rac_sum_fc, [x.name for x in rac_fc_flds if x.name in drop_flds_rac])
    
    # arcpy.Delete_management(temp_bg_fc)
    arcpy.Delete_management(temp_hex_fc)
    
    
    
def get_lodes_wac(fp_to_wac_tbl): 
    
    # join block group fc and summarized wac table 
    # print('\nCreating bg_wac_fc')
    
    # bg_fc = os.path.join(SHP_DIR, BG_FC_NAME)
    # temp_bg_fc = 'bg_tmp'

    # arcpy.MakeFeatureLayer_management(bg_fc, temp_bg_fc)
    
    # arcpy.AddJoin_management(temp_bg_fc, 'GEOID', fp_to_wac_tbl, 'bg_id') 
    
    fp_to_wac_fc = os.path.join(fp_to_data_fgdb, 'bg_lodes_wac')
    # arcpy.CopyFeatures_management(temp_bg_fc, fp_to_wac_fc)
    
    # arcpy.RemoveJoin_management(temp_bg_fc) 

    
    # # Calculate sq. meters of block group 
    # print('Calculating block group sq. meters')
    
    # arcpy.AddField_management(fp_to_wac_fc, 'SQ_METERS_BG', 'DOUBLE')
    
    # arcpy.CalculateField_management(
        # fp_to_wac_fc, "SQ_METERS_BG",  "!SHAPE.AREA@SQUAREMETERS!", "PYTHON")
        
    
    # intersect wac-block group and hexagons 
    print('Intersecting block group fc and hexagons')
    
    fp_to_hexagons_fc = os.path.join(fp_to_data_fgdb, HEXAGON_FC)
    
    fp_to_wac_intersect_fc = os.path.join(fp_to_data_fgdb, HEXAGON_FC + '_bg_wac_intersect')
    
    arcpy.Intersect_analysis(
        [fp_to_hexagons_fc, fp_to_wac_fc], fp_to_wac_intersect_fc, 'ALL', "", 'INPUT') 
        
        
    # Calculate sq. meters of block group-hexagon intersected area 
    print('Calculating sq. meters of intersected area')
    
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'SQ_METERS_INTERSECT', 'DOUBLE')
    
    arcpy.CalculateField_management(
        fp_to_wac_intersect_fc, "SQ_METERS_INTERSECT",  "!SHAPE.AREA@SQUAREMETERS!", "PYTHON")
        
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'PRCT_BG_INTERSECT', 'DOUBLE')
    
    arcpy.CalculateField_management(
        fp_to_wac_intersect_fc, "PRCT_BG_INTERSECT",  "!SQ_METERS_INTERSECT! / !SQ_METERS_BG!", "PYTHON")
        
    
    # Calculate the proportion of each block group's wac population after intersect 
    print('Calculating field values based on intersect proportion')
    
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_C000', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CA01', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CA02', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CA03', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CE01', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CE02', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CE03', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CD01', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CD02', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CD03', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CD04', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CS01', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CS02', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CFA01', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CFA02', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CFA03', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CFA04', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CFA05', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CFS01', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CFS02', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CFS03', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CFS04', 'DOUBLE')
    arcpy.AddField_management(fp_to_wac_intersect_fc, 'COUNT_CFS05', 'DOUBLE')  
    
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_C000",  "!C000! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CA01",  "!CA01! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CA02",  "!CA02! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CA03",  "!CA03! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CE01",  "!CE01! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CE02",  "!CE02! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CE03",  "!CE03! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CD01",  "!CD01! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CD02",  "!CD02! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CD03",  "!CD03! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CD04",  "!CD04! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CS01",  "!CS01! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CS02",  "!CS02! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CFA01",  "!CFA01! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CFA02",  "!CFA02! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CFA03",  "!CFA03! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CFA04",  "!CFA04! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CFA05",  "!CFA05! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CFS01",  "!CFS01! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CFS02",  "!CFS02! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CFS03",  "!CFS03! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CFS04",  "!CFS04! * !PRCT_BG_INTERSECT!", "PYTHON")
    arcpy.CalculateField_management(fp_to_wac_intersect_fc, "COUNT_CFS05",  "!CFS05! * !PRCT_BG_INTERSECT!", "PYTHON")
    
    
    # Dissolve intersected block groups-hexagons by grid cell IDs 
    print('Dissolving based on hexagon grid IDs')
    
    fp_to_wac_dissolve_fc = os.path.join(fp_to_data_fgdb, HEXAGON_FC + '_bg_wac_dissolve')
    
    arcpy.Dissolve_management(
        fp_to_wac_intersect_fc, fp_to_wac_dissolve_fc, "GRID_ID",
        [
            ["COUNT_C000", "SUM"], 
            ["COUNT_CA01", "SUM"], 
            ["COUNT_CA02", "SUM"],
            ["COUNT_CA03", "SUM"], 
            ["COUNT_CE01", "SUM"], 
            ["COUNT_CE02", "SUM"], 
            ["COUNT_CE03", "SUM"], 
            ["COUNT_CD01", "SUM"],
            ["COUNT_CD02", "SUM"], 
            ["COUNT_CD03", "SUM"], 
            ["COUNT_CD04", "SUM"], 
            ["COUNT_CS01", "SUM"],
            ["COUNT_CS02", "SUM"], 
            ["COUNT_CFA01", "SUM"], 
            ["COUNT_CFA02", "SUM"], 
            ["COUNT_CFA03", "SUM"], 
            ["COUNT_CFA04", "SUM"], 
            ["COUNT_CFA05", "SUM"], 
            ["COUNT_CFS01", "SUM"], 
            ["COUNT_CFS02", "SUM"], 
            ["COUNT_CFS03", "SUM"], 
            ["COUNT_CFS04", "SUM"], 
            ["COUNT_CFS05", "SUM"]
        ], "", "")
        
    
    # Join dissolved data to full hexagons fc 
    print('Joining with full hexagon fc')
    
    temp_hex_fc = 'hex_temp'
    
    arcpy.MakeFeatureLayer_management(fp_to_hexagons_fc, temp_hex_fc)
    
    arcpy.AddJoin_management(temp_hex_fc, 'GRID_ID', fp_to_wac_dissolve_fc, 'GRID_ID')
    
    fp_to_wac_sum_fc = os.path.join(fp_to_data_fgdb, HEXAGON_FC + '_bg_wac_sum')
    
    arcpy.CopyFeatures_management(temp_hex_fc, fp_to_wac_sum_fc)
            
    arcpy.RemoveJoin_management(temp_hex_fc)
    
    
    # CLEAN UP 
    # ----------
    wac_fc_flds = arcpy.ListFields(fp_to_wac_sum_fc)
    drop_flds_wac = ['GRID_ID_1', 'OBJECTID_1']
    arcpy.DeleteField_management(
        fp_to_wac_sum_fc, [x.name for x in wac_fc_flds if x.name in drop_flds_wac])
    
    # arcpy.Delete_management(temp_bg_fc)
    arcpy.Delete_management(temp_hex_fc)
      
    
# ==================================================================================================
# MAIN
# ==================================================================================================

if __name__ == "__main__":

    start_process_time = datetime.datetime.now()
    print('\nStart time = {}'.format(start_process_time))


    data_fgdb = UPPER_STATE + '.gdb'
    fp_to_data_fgdb = os.path.join(DATA_DIR, data_fgdb)

    arcpy.env.workspace = fp_to_data_fgdb
    
    # Field names will NOT include table name 
    arcpy.env.qualifiedFieldNames = False
    
    
    # RAC 
    # -------
    rac_tbl = LOWER_STATE + '_rac_S000_JT00_2015_bg_sum'
    fp_to_rac_tbl = os.path.join(fp_to_data_fgdb, rac_tbl)
    
    get_lodes_rac(fp_to_rac_tbl)
    
    
    # WAC 
    # -------
    wac_tbl = LOWER_STATE + '_wac_S000_JT00_2015_bg_sum'
    fp_to_wac_tbl = os.path.join(fp_to_data_fgdb, wac_tbl)
    
    get_lodes_wac(fp_to_wac_tbl)
    

    
    end_process_time = datetime.datetime.now()
    total_process_time = end_process_time - start_process_time
    print('\nEnd time = {}'.format(end_process_time))
    print('Total run time = {}'.format(total_process_time))