import geopandas as gpd
import pandas as pd
import numpy as np
import datetime
import matplotlib
import glob
import os
import matplotlib.pyplot as plt


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

# Define the folder where you want to save the figures
output_folder = os.path.abspath(r'C:\Users\AminTorabi\Documents\Python Scripts\figure')

# Create the output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

plot_dates = plot_df.columns[2:]

# min_val = plot_df[plot_dates].min().min()
# if min_val == 0.0:
#     min_val += 0.001
# vmax = plot_df[plot_dates].max().max()
vmin = 10  # You can adjust this value as needed
vmax = 5000
custom_colors = plt.cm.get_cmap('tab20c', 30)

for temp_date in plot_dates:
    plot_data = plot_df[temp_date]
    # plot_data = np.log10(plot_df[temp_date] + 1)
    print(temp_date)
    temp_date_str = temp_date.strftime('%Y-%m-%d_%H-%M')
    vmin = 10  # You can adjust this value as needed

    print(vmin, vmax)
    fig = plot_df.plot(column=temp_date, cmap=custom_colors, 
                    norm=matplotlib.colors.LogNorm(vmin=vmin, vmax=vmax),
                    figsize=(15,10), linewidth=0.3, legend=True)
    fig.axis('off')
    fig.set_title(temp_date_str, fontsize=20)
    
    file_path = os.path.join(output_folder, 'figure_' + temp_date_str + '.jpg')
    
    # Save the figure directly
    plt.savefig(file_path, dpi=300)

    plt.close()  
    
