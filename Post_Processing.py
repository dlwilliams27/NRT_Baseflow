import pathlib
import HydroErr as he
import numpy as np
import xarray as xr
import pandas as pd
import re
import pathlib as path
import matplotlib.pyplot as plt
import netCDF4 as nc
from collections import defaultdict

#creating initial file paths
nwm_file_dir=r'C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\NWM_Results'
nwm_path= path.Path(nwm_file_dir)
usgs_file_dir=r'C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\Initial_Results'
usgs_path= path.Path(usgs_file_dir)
base_dir=r'C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project'
base_path= path.Path(base_dir)

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
            path1= nwm_path / f'{match1[0]}'
            nwm_path_list.append(path1)
    #iterating through files in each region folder
    for subfolder in nwm_path_list:
        print("subfolder", subfolder)
        # issue is that these are glob objects and not iterable, need them to be pure paths
        for file in subfolder.glob('*.nc'):
            match = re.match(r"NWM_gage_(\d+)", file.name)
            if match:
                gage = match[1]
                ds=xr.open_dataset(file)
                nwm = ds.to_dataframe()
                nwm['baseflow'] = nwm['q_lateral'] + nwm['qBucket']
                nwm.reset_index(inplace=True)
                nwm['date'] = nwm['time'].dt.date
                nwm_avg_base = nwm.groupby('date', sort=True)['baseflow'].mean()
                nwm_avg_stream = nwm.groupby('date', sort=True)['streamflow'].mean()
                nwm_tot = pd.merge(nwm_avg_base, nwm_avg_stream, left_index=True, right_index=True)
                nwm_tot.reset_index(inplace=True, columns='date')
                nwm_dic[gage] = nwm_tot
            else:
                print(f"No match for file {nc}.")
    return nwm_dic

#process through each file in usgs streamflow
def usgs_processing(usgs_path):
    stream_path= usgs_path / "USGS_Streamflow_2024"
    for folder in stream_path.iterdir():
        match1=re.match(r'(\d+)',folder.name)
        if match1:
            path2= stream_path / f'{match1.group(1)}'
            usgs_path_list.append(path2)
    for subfolder in usgs_path_list:
        for file in subfolder.glob('*.txt'):
            match2 = re.match(r"(\d+)_streamflow_qc", file.name)
            if match2:
                gage2=match2.group(1)
                usgs = pd.read_csv(file, sep=' ', on_bad_lines='skip')
                if len(usgs.columns) == 7:
                    usgs.columns = ['gage', 'year', 'month', 'day', 'NAN', 'Q', 'nAn']
                elif len(usgs.columns) == 8:
                    usgs.columns = ['gage', 'year', 'month', 'day', 'NAN', 'nan', 'Q', 'nAn']
                elif len(usgs.columns) == 9:
                    usgs.columns = ['gage', 'year', 'month', 'day', 'NAN', 'nan', 'NaN', 'Q', 'nAn']
                elif len(usgs.columns) == 10:
                    usgs.columns = ['gage', 'year', 'month', 'day', 'NAN', 'nan', 'NaN', 'NAn', 'Q', 'nAn']
                usgs = usgs[['gage', 'year', 'month', 'day', 'Q']]
                usgs.dropna(inplace=True)
                usgs['day']=usgs['day'].astype(int)
                usgs['date'] = pd.to_datetime(usgs[['year', 'month', 'day']])
                usgs.set_index('date', inplace=True)
                usgs = usgs[['date', 'Q']]
                usgs_dic[gage2] = usgs
    return usgs_dic

#process through each transformed eckhardt baseflow file
def eck_processing(usgs_path):
    eck_path = usgs_path / "Eckhardt_2024"
    print("eckpath", eck_path)
    for file in eck_path.glob('*.csv'):
        match3 = re.match(r"(\d+)_streamflow_qc_processed", file.name)
        if match3:
            gage=match3.group(1)
            eck=pd.read_csv(file)
            eck['date'] = pd.to_datetime(eck['date'])
            eck.set_index('date', inplace=True)
            eck_dic[gage]=eck
    return eck_dic

def merge_dicts(nwm_dic, usgs_dic, eck_dic):
    complete=[]
    list=[nwm_dic, usgs_dic, eck_dic]
    common_keys= set(nwm_dic).intersection(usgs_dic, eck_dic)
    for key in common_keys:
        df1=nwm_dic[key].copy()
        df2=usgs_dic[key].copy()
        df3=eck_dic[key].copy()
        df3['gage']=key
        middle=pd.merge(df1, df2, left_index=True, right_index=True)
        final=pd.merge(middle, df3, left_index=True, right_index=True)
        complete.append(final)
        print(final)
    pd.concat(complete)
    complete['BFIobs']=(complete['Eckhardt'].astype(float))/complete['Q'] #issue with data types here
    complete['BFIsim']=complete['baseflow']/complete['streamflow']

    finaloutput_path = base_path / 'Complete_inputs4stats.csv'
    finaloutput_path.parent.mkdir(parents=True, exist_ok=True)
    complete.to_csv(finaloutput_path, index=True)

    return final, common_keys

def seasons(df):
    df.iterrows()
        if df['month']= 12 or 1 or 2:
            df['season']=1
        elif df['month']== 3 or 4 or 5:
            df['season']=2
        elif df['month']== 6 or 7 or 8:
            df['season']=3
        else:
            df['season']=4
    #return df not working???

def stats(final_df, common_keys):
    final_df.reset_index(inplace=True)
    final_df['year']=final_df['date'].dt.year
    final_df['month']=final_df['date'].dt.month
    final_df=seasons(final_df)
    final_stats=pd.DataFrame()
    grouped=final_df.groupby('gage')
    #overall stats
    for name, group in grouped:
        final_stats['gage']=name
        final_stats['nse_o']=he.nse(group['baseflow'].to_numpy(), group['Eckhardt'].to_numpy())
        final_stats['kge_12_o'] = he.kge_2012(group['baseflow'].to_numpy(), group['Eckhardt'].to_numpy())
        final_stats['rmse_o']=he.rmse(group['baseflow'],group['Eckhardt'])
        final_stats['pearson_o']=he.pearson_r(group['baseflow'],group['Eckhardt'])
    overall_path= base_path / 'Overall_stats.csv'
    overall_path.parent.mkdir(parents=True, exist_ok=True)
    final_stats.to_csv(overall_path, index=True)
    #year stats
    grouped=final_df.groupby(['gage', 'year'])
    # how to iterate with two "names"
        final_stats['gage'] = name
        final_stats['nse_y'] = he.nse(group['baseflow'].to_numpy(), group['Eckhardt'].to_numpy())
        final_stats['kge_12_y'] = he.kge_2012(group['baseflow'].to_numpy(), group['Eckhardt'].to_numpy())
        final_stats['rmse_y'] = he.rmse(group['baseflow'], group['Eckhardt'])
        final_stats['pearson_y'] = he.pearson_r(group['baseflow'], group['Eckhardt'])
    year_path = base_path / 'year_stats.csv'
    year_path.parent.mkdir(parents=True, exist_ok=True)
    final_stats.to_csv(year_path, index=True)
    #seasonal stats #want this to be grouped with w=(12,1,2)=Dec,Jan,Feb s=(3,4,5)=Mar,Apr,May su=(6,7,8)=Jun,Jul,Aug f=(9,10,11)=Sep, Oct, Nov
    grouped=final_df.groupby(['gage', 'season'])
    #how to iterate with two "names"
        final_stats['gage'] = name
        final_stats['nse_s'] = he.nse(group['baseflow'].to_numpy(), group['Eckhardt'].to_numpy())
        final_stats['kge_12_s'] = he.kge_2012(group['baseflow'].to_numpy(), group['Eckhardt'].to_numpy())
        final_stats['rmse_s'] = he.rmse(group['baseflow'], group['Eckhardt'])
        final_stats['pearson_s'] = he.pearson_r(group['baseflow'], group['Eckhardt'])
    season_path = base_path / 'seasonal_stats.csv'
    season_path.parent.mkdir(parents=True, exist_ok=True)
    final_stats.to_csv(season_path, index=True)

#for example purposes and so I don't have to run all the previous functions
nwm_ex=r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\NWM_Results\region_3\NWM_gage_2046000.nc"
usgs_ex=r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\Initial_Results\USGS_Streamflow_2024\03\02046000_streamflow_qc.txt"
eck_ex=r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\Initial_Results\Eckhardt_2024\02046000_streamflow_qc_processed.csv"

ds=xr.open_dataset(nwm_ex)
nwm = ds.to_dataframe()
nwm['baseflow'] = nwm['q_lateral'] + nwm['qBucket']
nwm.reset_index(inplace=True)
nwm['date'] = nwm['time'].dt.date
nwm_avg_base = nwm.groupby('date', sort=True)['baseflow'].mean()
nwm_avg_stream = nwm.groupby('date', sort=True)['streamflow'].mean()
nwm_tot = pd.merge(nwm_avg_base, nwm_avg_stream, left_index=True, right_index=True)
nwm_dic_sam= {'02046000':nwm_tot}

usgs = pd.read_csv(usgs_ex, sep=' ', on_bad_lines='skip')
if len(usgs.columns) == 7:
    usgs.columns = ['gage', 'year', 'month', 'day', 'NAN', 'Q', 'nAn']
elif len(usgs.columns) == 8:
    usgs.columns = ['gage', 'year', 'month', 'day', 'NAN', 'nan', 'Q', 'nAn']
elif len(usgs.columns) == 9:
    usgs.columns = ['gage', 'year', 'month', 'day', 'NAN', 'nan', 'NaN', 'Q', 'nAn']
elif len(usgs.columns) == 10:
    usgs.columns = ['gage', 'year', 'month', 'day', 'NAN', 'nan', 'NaN', 'NAn', 'Q', 'nAn']
usgs = usgs[['gage', 'year', 'month', 'day', 'Q']]
usgs.dropna(inplace=True)
usgs['day']=usgs['day'].astype(int)
usgs['date'] = pd.to_datetime(usgs[['year', 'month', 'day']])
usgs = usgs[['date', 'Q']]
usgs.set_index('date', inplace=True)
usgs_dic_sam= {'02046000':usgs}

eck=pd.read_csv(eck_ex)
eck['date'] = pd.to_datetime(eck['date'])
eck.set_index('date', inplace=True)
eck_dic_sam= {'02046000': eck}
print(nwm_dic_sam['02046000'])
print(usgs_dic_sam['02046000'])
print(eck_dic_sam['02046000'])

if __name__ == '__main__':
    #nwm_dic=nwm_processing(nwm_path, nwm_path_list)
    #usgs_dic=usgs_processing(usgs_path)
    #eck_dic=eck_processing(usgs_path)
    all, common_keys=merge_dicts(nwm_dic=nwm_dic_sam, usgs_dic=usgs_dic_sam, eck_dic=eck_dic_sam)
    stats(all, common_keys)





