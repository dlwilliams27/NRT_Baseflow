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
nwm_file_dir=r'C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\NWM_Results'
nwm_path= path.Path(nwm_file_dir)
usgs_file_dir=r'C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\Initial_Results'
usgs_path= path.Path(usgs_file_dir)

nwm_dic={}
usgs_dic={}
eck_dic={}
nwm_path_list=[]
usgs_path_list=[]

#extract and process data from each nwm.nc file
#output in a dictionary, gage:df
def nwm_processing(nwm_path, nwm_path_list):
    #create list of nwm region folders
    for folder in nwm_path.iterdir():
        match1=re.match(r'region_(\d+)',folder.name)
        if match1:
            path= nwm_path / f'{match1[0]}'
            nwm_path_list.append(path)
    #iterating through files in each region folder
    for subfolder in nwm_path_list:
        # issue is that these are glob objects and not iterable, need them to be pure paths
        for file in subfolder.glob('.nc'):
            print("The files in the subfolder", file)
            match = re.match(r"NWM_gage_(\d+)", file.name)
            print("The match is:",match)
            if match:
                gage = match[1]
                ds=xr.open_dataset(nc)
                nwm = ds.to_dataframe()
                nwm['baseflow'] = nwm['q_lateral'] + nwm['qBucket']
                nwm.reset_index(inplace=True)
                nwm['date'] = nwm['time'].dt.date
                nwm_avg_base = nwm.groupby('date', sort=True)['baseflow'].mean()
                nwm_avg_stream = nwm.groupby('date', sort=True)['streamflow'].mean()
                nwm_tot = pd.merge(nwm_avg_base, nwm_avg_stream, left_index=True, right_index=True)
                nwm_dic[gage] = nwm_tot
            else:
                print(f"No match for file {nc}.")
    return nwm_dic

def usgs_processing(usgs_path):
    stream_path= usgs_path / "USGS_Streamflow_2024"
    for folder in stream_path.iterdir():
        match1=re.match(r'(\d+)',folder.name)
        if match1:
            path2= usgs_path / f'{match1.group(1)}'
            usgs_path_list.append(path2)

    for subfolder in nwm_path_list:
        all_files=subfolder.glob('.txt')
        for txt in all_files:
            match2 = re.match(r"(\d+)_streamflow_qc", txt.name)
            if match2:
                gage2=match2.group(1)
                usgs = pd.read_csv(txt, sep=' ', on_bad_lines='skip')
                usgs.columns = ['gage', 'year', 'month', 'day', 'NAN', 'nan', 'Q', 'nAn']
                usgs = usgs[['gage', 'year', 'month', 'day', 'Q']]
                usgs['date'] = pd.to_datetime(usgs[['year', 'month', 'day']])
                usgs = usgs[['date', 'Q']]
                usgs_dic['gage2'] = usgs
    return usgs_dic

def eck_processing(usgs_path):
    eck_path = usgs_path / "Eckhardt_2024"
    all_eck_files=eck_path.glob('.csv')
    for file in all_eck_files:
        match3 = re.match(r"(\d+)_streamflow_qc_processed", file.name)
        if match3:
            gage=match3.group(1)
            eck=pd.read_csv(file)
            eck['date'] = pd.to_datetime(eck['date'])
            eck_dic[gage]=eck
    return eck_dic


if __name__ == '__main__':
    nwm_dic=nwm_processing(nwm_path, nwm_path_list)
    usgs_dic=usgs_processing(usgs_path)
    eck_dic=eck_processing(usgs_path)
    print(nwm_dic)
    print(usgs_dic)
    print(eck_dic)





