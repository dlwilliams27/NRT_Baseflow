import pathlib
import hydrostats
import numpy as np
import xarray as xr
import pandas as pd
import re
import pathlib as path
import matplotlib.pyplot as plt
import netCDF4 as nc

#creating initial file paths
nwm_file_dir=r'C:\Users\Delanie Williams\PycharmProjects\NRT_Eckhardt_Project_2025\NWM'
nwm_path= path.Path(nwm_file_dir)
usgs_file_dir=r'C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\Initial_Results'
usgs_path= path.Path(usgs_file_dir)

nwm_dic={}
nwm_path_list=[]

#extract and process data from each nwm.nc file
#output in a dictionary, gage:df
def nwm_processing(nwm_path, nwm_path_list):
    #create list of nwm region folders
    for folder in nwm_path.iterdir():
        match1=re.match('region_(\d|\d+)')
        if match1:
            path= nwm_path \ f'{match[0]}'
            nwm_path_list.append(path)

    #iterating through files in each region folder
    for subfolder in nwm_path_list:
        all_files=subfolder.glob('.nc')
        for nc in all_files:
            match = re.match("NWM_gage_(\d+)")
            if match:
                gage = match[1]
                ds=xr.open_dataset(nc)
                nwm = ds.to_dataframe()
                nwm['baseflow'] = nwm['q_lateral'] + nwm['qBucket']
                nwm.reset_index(inplace=True)
                nwm['time'] = nwm['time'].dt.date
                nwm_avg_base = nwm.groupby('time', sort=True)['baseflow'].mean()
                nwm_avg_stream = nwm.groupby('time', sort=True)['streamflow'].mean()
                nwm_tot = pd.merge(nwm_avg_base, nwm_avg_stream, left_index=True, right_index=True)
                nwm_dic['gage'] = nwm_tot
            else:
                print(f"No match for file {nc}.")
    return nwm_dic








