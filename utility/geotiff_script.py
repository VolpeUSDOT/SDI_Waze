# Imports

from datetime import datetime, date, time, timedelta
import os
import tempfile
import gc
import sys

from boto.s3.connection import S3Connection
# import metpy.calc as mpcalc
# from metpy.plots import simple_layout, StationPlot, StationPlotLayout
# from metpy.plots.wx_symbols import sky_cover, current_weather
from metpy.units import units
from netCDF4 import num2date
import netCDF4
import numpy as np
import pyart
import pytz

volpewazedir = "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
geotiffdir = volpewazedir + "Data/Weather_Geotiffs/"

# Helper functions for the search

def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

def _nearestDate(dates, pivot):
    return min(dates, key=lambda x: abs(x - pivot))


def get_radar_from_aws(site, datetime_t):
    """
    Get the closest volume of NEXRAD data to a particular datetime.
    
    Parameters
    ----------
    site : string
        four letter radar designation
    datetime_t : datetime
        desired date time
    Returns
    -------
    radar : Py-ART Radar Object
        Radar closest to the queried datetime
    """

    # First create the query string for the bucket knowing
    # how NOAA and AWS store the data
    my_pref = datetime_t.strftime('%Y/%m/%d/') + site

    # Connect to the bucket
    conn = S3Connection(anon = True)
    bucket = conn.get_bucket('noaa-nexrad-level2')

    # Get a list of files
    bucket_list = list(bucket.list(prefix = my_pref))

    # we are going to create a list of keys and datetimes to allow easy searching
    keys = []
    datetimes = []

    # populate the list
    for i in range(len(bucket_list)):
        this_str = str(bucket_list[i].key)
        if 'gz' in this_str:
            endme = this_str[-22:-4]
            fmt = '%Y%m%d_%H%M%S_V0'
            dt = datetime.strptime(endme, fmt)
            datetimes.append(dt)
            keys.append(bucket_list[i])

        if this_str[-3::] == 'V06':
            endme = this_str[-19::]
            fmt = '%Y%m%d_%H%M%S_V06'
            dt = datetime.strptime(endme, fmt)
            datetimes.append(dt)
            keys.append(bucket_list[i])

    # find the closest available radar to your datetime
    closest_datetime = _nearestDate(datetimes, datetime_t)
    index = datetimes.index(closest_datetime)

    localfile = tempfile.NamedTemporaryFile()
    keys[index].get_contents_to_filename(localfile.name)
    radar = pyart.io.read(localfile.name)
    
    return radar

# version get radar that returns a list of radar scan objects for each hour
def get_hourly_radars_from_aws(site, date_t):
    """
    Get 24 NEXRAD volumes for to a particular date.
    
    Parameters
    ----------
    site : string
        four letter radar designation
    date_t : date
        desired date
    Returns
    -------
    list of radars : Py-ART Radar Objects
        24 Radar scans within the given date
    """

    # First create the query string for the bucket knowing
    # how NOAA and AWS store the data
    my_pref = date_t.strftime('%Y/%m/%d/') + site

    # Connect to the bucket
    conn = S3Connection(anon = True)
    bucket = conn.get_bucket('noaa-nexrad-level2')

    # Get a list of files
    bucket_list = list(bucket.list(prefix = my_pref))

    # we are going to create a list of keys and datetimes to allow easy searching
    keys = []
    datetimes = []
    radars = []

    # populate the list
    for i in range(len(bucket_list)):
        this_str = str(bucket_list[i].key)
        if 'gz' in this_str:
            endme = this_str[-22:-4]
            fmt = '%Y%m%d_%H%M%S_V0'
            dt = datetime.strptime(endme, fmt)
            datetimes.append(dt)
            keys.append(bucket_list[i])

        if this_str[-3::] == 'V06':
            endme = this_str[-19::]
            fmt = '%Y%m%d_%H%M%S_V06'
            dt = datetime.strptime(endme, fmt)
            datetimes.append(dt)
            keys.append(bucket_list[i])

    # find the closest 24 available radars for each hour within the date
    for i in range(0,23):
        datetime_hour = datetime.combine(date_t, time(i))
        closest_datetime = _nearestDate(datetimes, datetime_hour)
        index = datetimes.index(closest_datetime)

        localfile = tempfile.NamedTemporaryFile()
        keys[index].get_contents_to_filename(localfile.name)
        radar = pyart.io.read(localfile.name)
        radars.append(radar)
    conn.close()
    localfile = 0
    return radars




def radars_to_grids(radars):
    """
    
    Takes a list of radar objects, filters out unwanted values and bounds, and returns a list of processed grid objects

    
    Parameter 
    ---------------------
    radars : list
    
    Returns
    ---------------------
    grids : list
    
    """
    grids = []
    for radar in radars:
        #  mask out last 10 gates of each ray, this removes the "ring" around the radar station. 
        # (we do this because the values close to the station itself are unreliable, and correcting them is beyond the scope of our use-case (although a well-defined problem with a lot of solutions))
        radar.fields['reflectivity']['data'][:, -10:] = np.ma.masked

        # exclude masked gates from the gridding
        gatefilter = pyart.filters.GateFilter(radar)
        gatefilter.exclude_transition()
        gatefilter.exclude_masked('reflectivity')

        # perform Cartesian mapping, limit to the reflectivity field.
        # We can modify the grid shape and limits to change our bounds and grid resolution for geotiff downstream
        grid = pyart.map.grid_from_radars(
            (radar,), gatefilters=(gatefilter, ),
            grid_shape=(1, 541, 541), # resolution
            grid_limits=((2000, 2000), (-223000.0, 223000.0), (-223000.0, 223000.0)), # bounds
            fields=['reflectivity']) #only selecting reflectivity right now for rainfall, other fields include velocity, might be a good sperate feature
        grids.append(grid)
    return grids

def write_daily_geotiffs(start_date,end_date, station, save_path = "/home/tannagram/Geotiffs/"):
    """
    Parameters:
    --------------
    Takes a start and end date (date objects), 
    a 4 letter NEXRAD station code (string), 
    and an optional filepath (string)
    
    Writes a series of 24 geotiff files per day in the range

    *optional filepath is a string specifying where to save the geotiffs
    
    TO DO: aggregate and fuse multiple stations before writing to grid/geotiff
    """
    
    os.chdir(save_path) #change to folder you wish to save geotiffs in
    my_daterange = daterange(start_date,end_date)
    grids = []
    radars = []
    for my_datetime in my_daterange:
        grids, radars = 0, 0
        gc.collect()
        print(my_datetime.strftime('%Y/%m/%d/'))
        radars = get_hourly_radars_from_aws(station, my_datetime)
        grids = radars_to_grids(radars)
	radars = 0
 

        for i, grid in enumerate(grids):
            #write to geotiff file 
            """
            Note: there is a warp argument that uses gdal to warp the output to a lat/lon WGS84 grid, 
            might be important for working on certain projections? unsure...

            ( from the man on pyart.io.write_grid_geotiff() ):

            warp : bool, optional
                True - Use gdalwarp (called from command line using os.system)
                       to warp to a lat/lon WGS84 grid.
                False - No warping will be performed. Output will be Az. Equidistant.)
            """

            pyart.io.write_grid_geotiff(grid, my_datetime.strftime('%Y-%m-%d-') + str(i+1) + station, 'reflectivity')
            print("wrote a geotiff!")


def main():

   ## main write out to geotiff function call
   arg = sys.argv[1:][0]
   start_date = datetime.strptime(arg, '%Y/%m/%d')
   end_date = datetime(2017,7,30)
   station = 'KLWX'

   write_daily_geotiffs(start_date,end_date,station)

if __name__ == "__main__":
    main()
