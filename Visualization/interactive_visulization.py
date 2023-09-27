import geopandas as gpd
import pandas as pd
import numpy as np
import datetime
import matplotlib
import glob
import os
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import matplotlib.cm as cm
import matplotlib.colors as colors

file_path = r"C:\Users\AminTorabi\Downloads\conus.gpkg"
flowpaths = gpd.read_file(file_path, layer='flowpaths')
print('gpkg is read')
def numeric_id(flowpath):
    id = flowpath['id'].split('-')[-1]
    toid = flowpath['toid'].split('-')[-1]
    flowpath['id'] = int(float(id))
    flowpath['toid'] = int(float(toid))
    return flowpath

flowpaths = flowpaths.apply(numeric_id, axis=1)
qvd = pd.read_csv(r"C:\Users\AminTorabi\Documents\Python Scripts\flowveldepth_2023-07-18.csv", index_col=0).iloc[:,0::3]
print('qvd is read')
nsteps = len(qvd.columns)
t0 = datetime.datetime.strptime('2021/10/22 00:00', '%Y/%m/%d %H:%M')
time_steps = [t0 + datetime.timedelta(minutes=i*15) for i in range(nsteps)]
qvd.columns = time_steps
plot_df = flowpaths[flowpaths['order']>1][['id','geometry']]
plot_df = pd.merge(plot_df, qvd.reset_index().rename(columns={'index': 'id'}), on='id')

plot_df['centroid'] = plot_df['geometry'].centroid
plot_df['centroid_wgs84'] = plot_df['centroid'].to_crs(epsg=4326)
plot_df['lon'] = plot_df['centroid_wgs84'].x
plot_df['lat'] = plot_df['centroid_wgs84'].y


# Define the colormap (cmap)
cmap = cm.get_cmap('tab20c', 30)  # 'tab20c' with 30 discrete colors

# Extract the color column and normalize it
color_column = plot_df[plot_df.columns[2]]  # Use the first timestamp column for coloring (change as needed)
vmin = 10
vmax = 5000
norm = colors.LogNorm(vmin=vmin, vmax=vmax)
normalized_colors = norm(color_column)

# Create the text label 
plot_df['text0'] = 'id: ' + plot_df['id'].astype(str)
plot_df['text1'] = plot_df.columns[2].strftime("%Y-%m-%d %H:%M:%S")[2:16] + ' Q: ' + plot_df[plot_df.columns[2]].astype(int).astype(str) + ' m3/s'
plot_df['text2'] = plot_df.columns[3].strftime("%Y-%m-%d %H:%M:%S")[2:16] + ' Q: ' + plot_df[plot_df.columns[3]].astype(int).astype(str) + ' m3/s'

# Adjust the size of the markers
marker_size = 0.75  # Set to your preferred size

fig = go.Figure(data=go.Scattergeo(
    lon=plot_df['lon'],
    lat=plot_df['lat'],
    text=plot_df['text0'] + '<br>' + plot_df['text1'] + '<br>' + plot_df['text2'],
    mode='markers',
    marker=dict(
        color=normalized_colors,  # Use the normalized colors based on the 'tab20c' colormap
        cmin=vmin,
        cmax=vmax,
        colorscale='Viridis',  # Specify a different colormap if needed
        size=marker_size,
        colorbar=dict(title='Colorbar Title')  # Customize the colorbar title as needed
    )
))

fig.update_layout(
    title=f"Flowrates at {plot_df.columns[2]}",
    geo_scope='usa',
    width=1500,
    height=1000
)

fig.show()
