
# ==================================================================================================
#
# Name:        sdi_waze_aadt_vmt_data.py
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

SHP_DIR  = 'D:\Tasks\OST_waze_phase2\data\shapefiles'

UPPER_STATE = 'TN' 

# HEXAGON_FC = UPPER_STATE + '_hexagons_1mi' # CT, MD, UT, VA 

# HEXAGON_FC = UPPER_STATE + '_1sqmile_hexagons' # TN hexagons 
HEXAGON_FC = UPPER_STATE + '_01dd_fishnet' # TN fishnet  


# ==================================================================================================
# GLOBAL VARIABLES
# ==================================================================================================

# Codeblock for calculating whether a grid cell is classified as UBRAN or RURAL -- CT, MD, UT, VA 
# urban_rural_codeblock = \
"""
def classify(prct_urban, group): 

    if prct_urban >= 50:
        group = 'URBAN'
        
    else: 
        group = 'RURAL'
        
    return group
"""


# Codeblock for calculating whether a grid cell is classified as UBRAN or RURAL -- TN 
urban_rural_codeblock = \
"""
def classify(prct_urban, group): 
       
    if prct_urban is None:
        group = 'RURAL'
        
    elif prct_urban >= 50:
        group = 'URBAN'
        
    else: 
        group = 'RURAL'
        
    return group
"""

# ==================================================================================================
# METHODS 
# ==================================================================================================


def urban_area_intersect(fp_to_hexagons_fc): 
    
    # Intersect urban areas and hexagons 
    print('\nIntersecting urban areas and hexagons')
    
    fp_to_urban_fc = os.path.join(SHP_DIR, 'Census2010_UrbanAreas_prj_dissolve.shp')
      
    arcpy.Intersect_analysis(
        [fp_to_hexagons_fc, fp_to_urban_fc], fp_to_urban_intersect_fc, 'ALL', "", 'INPUT') 
        
        
    # Calculate sq. meters of Census urban areas-hexagon intersected area 
    print('Calculating sq. meters of intersected area')
    
    arcpy.AddField_management(fp_to_urban_intersect_fc, 'sq_meters_intersect', 'DOUBLE')
    
    arcpy.CalculateField_management(
        fp_to_urban_intersect_fc, "sq_meters_intersect",  "!SHAPE.AREA@SQUAREMETERS!", "PYTHON")
        
        
    # Calculate percent of urban area coverage per grid cell 
    print('Calculating percent urban area coverage per hexagon')
    
    arcpy.AddField_management(fp_to_urban_intersect_fc, 'prct_urban_intersect', 'DOUBLE')
    
    arcpy.CalculateField_management(
        # fp_to_urban_intersect_fc, "prct_urban_intersect",  "(!sq_meters_intersect! / !sq_meters_hex!) * 100", "PYTHON")
        fp_to_urban_intersect_fc, "prct_urban_intersect",  "(!sq_meters_intersect! / !sq_meters_fishnet!) * 100", "PYTHON") # TN fishnet ONLY 
        
    
        
def hpms_routes_intersect(fp_to_hexagons_fc):

    # Intersect HPMS routes and hexagons 
    print('\nIntersecting HPMS routes and hexagons')
    
    fp_to_routes_fc = os.path.join(fp_to_data_fgdb, UPPER_STATE + '_ROUTE_SHAPES_LRS_prj')
       
    arcpy.Intersect_analysis(
        [fp_to_hexagons_fc, fp_to_routes_fc], fp_to_routes_intersect_fc, 'ALL', "", 'INPUT') 
        
        
    # Join percent intersected field from urban-hex intersected fc
    print('Joining percent urban intersect field to intersect routes fc')
    
    arcpy.JoinField_management(
        fp_to_routes_intersect_fc, "GRID_ID", fp_to_urban_intersect_fc, "GRID_ID", ["prct_urban_intersect"])
        
        
    # Specify whether the grid cell is URBAN or RURAL based on urban intersect coverage 
    print('Specifing if grid cell is either URBAN or RURAL')
    
    arcpy.AddField_management(fp_to_routes_intersect_fc, 'URBAN_RURAL', 'TEXT')

    urban_rural_exp = "classify( !prct_urban_intersect!, !URBAN_RURAL! )"

    arcpy.CalculateField_management(
        fp_to_routes_intersect_fc, "URBAN_RURAL", urban_rural_exp, "PYTHON", urban_rural_codeblock)   
        
        
    # Calculate the length of each intersected segment 
    print('Calculating intersected route segment length in miles')
    
    arcpy.AddField_management(fp_to_routes_intersect_fc, 'miles_intersected_len', 'DOUBLE')

    arcpy.CalculateField_management(
        fp_to_routes_intersect_fc, "miles_intersected_len", "!SHAPE.LENGTH@MILES!", "PYTHON") 
        
        
        
def calculate_max_aadt(fp_to_hexagons_fc):
   
    arcpy.MakeFeatureLayer_management(fp_to_routes_intersect_fc, 'tmp_routes_hex_lyr')
    
    # Only want records with FC 1-5 and AADT is not null 
    arcpy.SelectLayerByAttribute_management(
        'tmp_routes_hex_lyr', "NEW_SELECTION", "F_SYSTEM_VN in (1, 2, 3, 4, 5)")
        
    arcpy.SelectLayerByAttribute_management(
        'tmp_routes_hex_lyr', 'SUBSET_SELECTION', "AADT_VN > 0 ")
        
    arcpy.CopyFeatures_management('tmp_routes_hex_lyr', 'tmp_routes_hex_subset_lyr')
    
    
    # Calculate max AADT for each route ID, within each grid cell for each funtional class
    print('\nCalculating MAX AADT for each route ID, within each grid cell')
    
    max_by_grid_fc_route_tbl = os.path.join(DATA_DIR, data_fgdb, HEXAGON_FC + '_max_aadt_by_grid_fc_route')
    
    arcpy.Statistics_analysis('tmp_routes_hex_subset_lyr', max_by_grid_fc_route_tbl, 
        [["AADT_VN", "MAX"]], ["Route_ID", "GRID_ID", "F_SYSTEM_VN"])
        
    
    # Calculate max AADT for within each grid cell, for each functional calss 
    print('Calculating MAX AADT within each grid cell, for each functional class')
    
    max_by_grid_fc_tbl = os.path.join(DATA_DIR, data_fgdb, HEXAGON_FC + '_max_aadt_by_grid_fc')

    arcpy.Statistics_analysis(max_by_grid_fc_route_tbl, max_by_grid_fc_tbl, 
        [["MAX_AADT_VN", "SUM"]], ["GRID_ID", "F_SYSTEM_VN"])
        
        
    # Calculate max AADT for within each grid cell
    print('Calculating MAX AADT within each grid cell')
    
    max_by_grid_tbl = os.path.join(DATA_DIR, data_fgdb, HEXAGON_FC + '_max_aadt_by_grid')

    arcpy.Statistics_analysis(max_by_grid_fc_route_tbl, max_by_grid_tbl, 
        [["MAX_AADT_VN", "SUM"]], ["GRID_ID"])
        
        
    # Calculate max AADT for each route ID, within each grid cell for each funtional class and urban area
    print('Calculating MAX AADT for each route ID, within each grid cell and urban area')
    
    max_by_grid_fc_route_urban_tbl = os.path.join(DATA_DIR, data_fgdb, HEXAGON_FC + '_max_aadt_by_grid_fc_route_urban')
    
    arcpy.Statistics_analysis('tmp_routes_hex_subset_lyr', max_by_grid_fc_route_urban_tbl, 
        [["AADT_VN", "MAX"]], ["Route_ID", "GRID_ID", "F_SYSTEM_VN", "URBAN_RURAL"])
        
        
    # Calculate max AADT for within each grid cell, for each functional calss and urban area 
    print('Calculating MAX AADT within each grid cell, for each functional class and urban area')
    
    max_by_grid_fc_urban_tbl = os.path.join(DATA_DIR, data_fgdb, HEXAGON_FC + '_max_aadt_by_grid_fc_urban')

    arcpy.Statistics_analysis(max_by_grid_fc_route_urban_tbl, max_by_grid_fc_urban_tbl, 
        [["MAX_AADT_VN", "SUM"]], ["GRID_ID", "F_SYSTEM_VN", "URBAN_RURAL"])

    
    # CLEAN UP 
    # ----------  
    arcpy.Delete_management('tmp_routes_hex_lyr')
    arcpy.Delete_management('tmp_routes_hex_subset_lyr')
    
    
    
def calculate_aadt_for_vmt(fp_to_hexagons_fc):

    arcpy.MakeFeatureLayer_management(fp_to_routes_intersect_fc, 'tmp_routes_hex_lyr')
    
    # Only want records with FC 1-5 and AADT is not null 
    arcpy.SelectLayerByAttribute_management(
        'tmp_routes_hex_lyr', "NEW_SELECTION", "F_SYSTEM_VN in (1, 2, 3, 4, 5)")
        
    arcpy.SelectLayerByAttribute_management(
        'tmp_routes_hex_lyr', 'SUBSET_SELECTION', "AADT_VN > 0 ")
        
    arcpy.SelectLayerByAttribute_management(
        'tmp_routes_hex_lyr', 'SUBSET_SELECTION', "FACILITY_TYPE_VN <> 6 ")
        
    arcpy.CopyFeatures_management('tmp_routes_hex_lyr', 'tmp_routes_hex_subset_lyr')
    
    
    # Calculate total AADT for each route ID, within each grid cell for each funtional class and urban area 
    print('\nCalculating TOTAL AADT for each route ID, within each grid cell and urban area')
    
    total_by_grid_fc_route_urban_tbl = os.path.join(DATA_DIR, data_fgdb, HEXAGON_FC + '_total_aadt_by_grid_fc_route_urban')
    
    arcpy.Statistics_analysis(
        'tmp_routes_hex_subset_lyr', total_by_grid_fc_route_urban_tbl, 
        [["AADT_VN", "SUM"], ["miles_intersected_len", "SUM"]], 
        ["Route_ID", "GRID_ID", "F_SYSTEM_VN", "URBAN_RURAL"])
        
    
    # Calculate proportional AADT based on mileage in grid cell 
    print('\nCalculating proportional TOTAL AADT for each route ID, within each grid cell and urban area')
    
    arcpy.AddField_management(total_by_grid_fc_route_urban_tbl, 'proportional_aadt', 'DOUBLE')  
    
    arcpy.CalculateField_management(
        total_by_grid_fc_route_urban_tbl, "proportional_aadt",  "(!SUM_AADT_VN! * !SUM_miles_intersected_len!)", "PYTHON")
    
        
    # Calculate total AADT for within each grid cell, for each functional calss and urban area 
    print('Calculating proportional TOTAL AADT within each grid cell, for each functional class and urban area')
    
    total_by_grid_fc_urban_tbl = os.path.join(DATA_DIR, data_fgdb, HEXAGON_FC + '_total_aadt_by_grid_fc_urban')

    arcpy.Statistics_analysis(total_by_grid_fc_route_urban_tbl, total_by_grid_fc_urban_tbl, 
        [["proportional_aadt", "SUM"]], 
        ["GRID_ID", "F_SYSTEM_VN", "URBAN_RURAL"])

    
    # CLEAN UP 
    # ----------  
    arcpy.Delete_management('tmp_routes_hex_lyr')
    arcpy.Delete_management('tmp_routes_hex_subset_lyr')



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


    fp_to_hexagons_fc = os.path.join(fp_to_data_fgdb, HEXAGON_FC)
    
    fp_to_urban_intersect_fc = os.path.join(fp_to_data_fgdb, HEXAGON_FC + '_urban_intersect')
    
    fp_to_routes_intersect_fc = os.path.join(fp_to_data_fgdb, HEXAGON_FC + '_routes_intersect')
    
        
        
    # CORE 
    # -----
    urban_area_intersect(fp_to_hexagons_fc)

    hpms_routes_intersect(fp_to_hexagons_fc)
    
    calculate_max_aadt(fp_to_hexagons_fc)
    
    calculate_aadt_for_vmt(fp_to_hexagons_fc)

    

    end_process_time = datetime.datetime.now()
    total_process_time = end_process_time - start_process_time
    print('\nEnd time = {}'.format(end_process_time))
    print('Total run time = {}'.format(total_process_time))