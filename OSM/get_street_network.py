# %%
import geopandas as gpd
import osmnx
import numpy as np
from shapely import Polygon, geometry, wkt
import pandas as pd
import osmnx as ox
import matplotlib.pyplot as plt

# %%

# Get the state network for Seattle and Tacoma
place_list = [
   	{
        'city': 'Seattle',
        'state': 'Washington',
        'country': 'USA'
}
]
# %%

ox.settings.requests_kwargs = {'verify': False}
ox.settings.log_console = True

place_network = ox.graph_from_place(place_list, network_type='drive', simplify=True)
#fig, ax = ox.plot_graph(washington, node_size=2, node_color='r', edge_color='w', edge_linewidth=0.2)
ox.save_graph_xml(place_network, filepath='./data/graph.osm')
# %%
nodes, edges = ox.utils_graph.graph_to_gdfs(place_network)
nodes.head()

edges.iloc[:, :4].head()





