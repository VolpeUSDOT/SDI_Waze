
# ==================================================================================================
#
# Name:        sdi_waze_fars_data.py
#
# Purpose:     Cross FARS data with grid cells. 
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

UPPER_STATE = 'VA' 


## PRE-STEPS: MERGE INDIVIDUAL FARS TABLES INTO ONE TABLE IN fGDB AND CHANGE FUNCSYSTEM FIELD NAME. 


# ==================================================================================================
# METHODS 
# ==================================================================================================


def get_fars_data(fp_to_fars_tbl): 
    
    # Create FARS fc 
    print('\nCreate points layer from table')
    
    xy_sr = arcpy.SpatialReference(4326)
    
    arcpy.MakeXYEventLayer_management(
        fp_to_fars_tbl, "longitude", "latitude", "tmp_fars_lyr_full", xy_sr, "")
        
    
    fars_fc = 'FARS_' + UPPER_STATE + '_2012_2016_prj'
    fp_to_fars_fc = os.path.join(fp_to_data_fgdb, fars_fc)
    
    prj_sr = arcpy.SpatialReference(102039)
    
    arcpy.Project_management("tmp_fars_lyr_full", fp_to_fars_fc, prj_sr)
    
    
    # Intersect FARS and hexagons 
    print('Intersecting FARS points fc and hexagons')
    
    hexagons_fc = UPPER_STATE + '_hexagons_1mi'
    fp_to_hexagons_fc = os.path.join(fp_to_data_fgdb, hexagons_fc)
    
    fp_to_intersect_fc = os.path.join(fp_to_data_fgdb, 'FARS_' + UPPER_STATE + '_2012_2016_intersect')
    
    arcpy.Intersect_analysis(
        [fp_to_hexagons_fc, fp_to_fars_fc], fp_to_intersect_fc, 'ALL', "", 'INPUT') 
        
        
    # Dissolve FARS per year by grid cell ID - ANNUAL SUM (2012 - 2016) 
    print('Dissolving FARS per year (annual sum)')
    
    fp_to_dissolve_fc_annual = os.path.join(fp_to_data_fgdb, 'FARS_' + UPPER_STATE + '_2012_2016_dissolve_annual')
    
    arcpy.Dissolve_management(
        fp_to_intersect_fc, fp_to_dissolve_fc_annual, ["GRID_ID", "caseyear"],
        [["casenum", "COUNT"], ["numfatal", "SUM"]], "", "")
        
        
    # Dissolve FARS per year, per functional class by grid cell - FCLASS SUM (2015 - 2016) 
    print('Dissolving FARS per functional class, per year (fclass sum)')
    
    arcpy.MakeFeatureLayer_management(fp_to_intersect_fc, 'tmp_fars_lyr_subset')
    
    arcpy.SelectLayerByAttribute_management(
        'tmp_fars_lyr_subset', "NEW_SELECTION", "caseyear in (2015, 2016)")
        
    
    fp_to_dissolve_fc_fclass = os.path.join(fp_to_data_fgdb, 'FARS_' + UPPER_STATE + '_2015_2016_dissolve_fclass')
            
    arcpy.Dissolve_management(
        'tmp_fars_lyr_subset', fp_to_dissolve_fc_fclass, ["GRID_ID", "caseyear", "funcsystem"],
        [["casenum", "COUNT"], ["numfatal", "SUM"]], "", "")
        
        
    # CLEAN UP 
    # ----------  
    arcpy.Delete_management('tmp_fars_lyr_full')
    arcpy.Delete_management('tmp_fars_lyr_subset')

    
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
    
    
    # CORE 
    # ----------
    fars_tbl = 'FARS_' + UPPER_STATE + '_2012_2016_tbl'
    fp_to_fars_tbl = os.path.join(fp_to_data_fgdb, fars_tbl)
    
    get_fars_data(fp_to_fars_tbl)

    
    end_process_time = datetime.datetime.now()
    total_process_time = end_process_time - start_process_time
    print('\nEnd time = {}'.format(end_process_time))
    print('Total run time = {}'.format(total_process_time))