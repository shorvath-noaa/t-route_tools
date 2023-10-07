import geopandas as gpd
import pandas as pd
import numpy as np
import datetime
import matplotlib
import glob
import os
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

file_path1 = r"C:\Users\AminTorabi\Documents\Python Scripts\flowveldepth_conus_2d_1hr.pkl"
qvd = pd.read_pickle(file_path1)

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
# qvd = pd.read_csv(r"C:\Users\AminTorabi\Documents\Python Scripts\flowveldepth_2023-07-18.csv", index_col=0).iloc[:,0::3]
print('qvd is read')
nsteps = len(qvd.columns)
t0 = datetime.datetime.strptime('2023/04/01 00:00', '%Y/%m/%d %H:%M')
time_steps = [t0 + datetime.timedelta(minutes=i*60) for i in range(nsteps)]
qvd.columns = time_steps
plot_df = flowpaths[flowpaths['order']>1][['id','geometry']]
plot_df = pd.merge(plot_df, qvd.reset_index().rename(columns={'index': 'id'}), on='id')
# Define the folder where you want to save the figures
output_folder = os.path.abspath(r'C:\Users\AminTorabi\Documents\Python Scripts\figure2')

# Create the output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

plot_dates = plot_df.columns[2:]

custom_colors = plt.cm.get_cmap('Blues', 30)

xmin = -2355820.0010157246  
xmax = 2245987.126845145  
ymin = 310953.6821908704   
ymax = 3162145.345157215  
 

for temp_date in time_steps:  # Use time_steps instead of plot_dates
    # print(temp_date)
    plot_data = plot_df[temp_date]
    temp_date_str = temp_date.strftime('%Y-%m-%d_%H-%M')  # Update temp_date_str using temp_date
    print(temp_date_str)
    vmin = 10
    vmax = 5000

    # Define custom line thickness values based on data value ranges (thinner values)
    custom_thickness = [
        5 if value > 10000 else
        4 if 5000 < value <= 10000 else
        3 if 2500 < value <= 5000 else
        2 if 1500 < value <= 2500 else
        1 if 1000 < value <= 1500 else
        0.5 if value <= 1000 else
        0.3  # Default thickness for other cases
        for value in plot_data
    ]

    # Create a figure with the specified extent
    fig, ax = plt.subplots(figsize=(15, 10))
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)

    # Plot the data within the specified extent with custom line thickness
    plot_df.plot(column=temp_date, cmap=custom_colors,
                 norm=matplotlib.colors.LogNorm(vmin=vmin, vmax=vmax),
                 ax=ax, linewidth=custom_thickness, legend=True)

    # Add a custom legend for line thickness
    # legend_elements = [
    #     Line2D([0], [0], color='k', lw=2, label='2'),
    #     Line2D([0], [0], color='k', lw=1.8, label='1.8'),
    #     Line2D([0], [0], color='k', lw=1.6, label='1.6'),
    #     Line2D([0], [0], color='k', lw=1.2, label='1.2'),
    #     Line2D([0], [0], color='k', lw=0.8, label='0.8'),
    #     Line2D([0], [0], color='k', lw=0.5, label='0.5'),
    #     Line2D([0], [0], color='k', lw=0.3, label='0.3'),
    # ]

    # ax.legend(handles=legend_elements, title='Line Thickness', loc='upper right')
    
    colorbar = ax.get_figure().get_axes()[1]  # Get the color legend
    colorbar.set_title('Flow Rate m³/s')
    ax.axis('off')
    ax.set_title(temp_date_str, fontsize=20)

    file_path = os.path.join(output_folder, 'figure_' + temp_date_str + '.jpg')

    plt.savefig(file_path, dpi=300)
    plt.close()
