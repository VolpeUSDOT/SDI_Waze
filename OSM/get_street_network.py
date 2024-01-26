# %%
import geopandas as gpd
import osmnx
import numpy as np
from shapely import Polygon, geometry, wkt
import pandas as pd
import osmnx as ox
import matplotlib.pyplot as plt
import time


# %%

# Get the state network for Seattle and Tacoma
place_list = [
   	{
        'state': 'Washington',
        'country': 'USA'
}
]
# %%
start = time.process_time()

ox.settings.requests_kwargs = {'verify': False}
ox.settings.log_console = True

place_network = ox.graph_from_place(place_list, network_type='drive', simplify=True)
#fig, ax = ox.plot_graph(washington, node_size=2, node_color='r', edge_color='w', edge_linewidth=0.2)

end = time.process_time()
print(end - start)


# %%
nodes, edges = ox.utils_graph.graph_to_gdfs(place_network)

# %%
nodes.head()

# %%
edges.iloc[:, :4].head()

# %%


start = time.process_time()

ox.save_graph_xml(place_network, filepath='./data/graph.osm')

end = time.process_time()
print(end - start)




