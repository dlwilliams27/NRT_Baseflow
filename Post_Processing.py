
import HydroErr as he
import numpy as np
import xarray as xr
import pandas as pd
import re
import pathlib as path
import netCDF4 as nc

#creating initial file paths
nwm_file_dir=r'C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\NWM_Results'
nwm_path= path.Path(nwm_file_dir)
usgs_file_dir=r'C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\Initial_Results'
usgs_path= path.Path(usgs_file_dir)
base_dir=r'C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project'
base_path= path.Path(base_dir)

#initializing dictionaries and lists
nwm_dic={}
usgs_dic={}
eck_dic={}
nwm_path_list=[]
usgs_path_list=[]

#extract and process data from each nwm.nc file
#output in a dictionary, gage:df
def nwm_processing(nwm_path, nwm_path_list):
    print("Running NWM Processing")
    #create list of nwm region folders
    for folder in nwm_path.iterdir():
        match1=re.match(r'region_(\d+)',folder.name)
        if match1:
            path1= nwm_path / f'{match1[0]}'
            nwm_path_list.append(path1)
    #iterating through files in each region folder
    for subfolder in nwm_path_list:
        # issue is that these are glob objects and not iterable, need them to be pure paths
        for file in subfolder.glob('*.nc'):
            match = re.match(r"NWM_gage_(\d+)", file.name)
            if match:
                gage = match[1].zfill(8)
                ds=xr.open_dataset(file)
                nwm = ds.to_dataframe()
                nwm['baseflow'] = nwm['q_lateral'] + nwm['qBucket']
                nwm.reset_index(inplace=True)
                nwm['date'] = nwm['time'].dt.date
                nwm_avg_base = nwm.groupby('date', sort=True)['baseflow'].mean()
                nwm_avg_stream = nwm.groupby('date', sort=True)['streamflow'].mean()
                nwm_tot = pd.merge(nwm_avg_base, nwm_avg_stream, left_index=True, right_index=True)
                nwm_tot.reset_index(inplace=True)
                nwm_tot.set_index('date', inplace=True)
                nwm_tot.dropna(how='any', inplace=True)
                nwm_dic[gage] = nwm_tot
            else:
                print(f"No match for file {nc}.")
    return nwm_dic

#process through each file in usgs streamflow
def usgs_processing(usgs_path):
    print("Running USGS Processing")
    stream_path= usgs_path / "USGS_Streamflow_2024"
    for folder in stream_path.iterdir():
        match1=re.match(r'(\d+)',folder.name)
        if match1:
            path2= stream_path / f'{match1.group(1)}'
            usgs_path_list.append(path2)
            for subfolder in usgs_path_list:
                for file in subfolder.glob('*.txt'):
                    try:
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
                            usgs = usgs['Q']
                            usgs_dic[gage2] = usgs
                    except KeyError:
                        print(f"Column indexing error for file: {file.name}")
                        continue
    return usgs_dic

#process through each transformed eckhardt baseflow file
def eck_processing(usgs_path):
    print("Running Eck Processing")
    eck_path = usgs_path / "Eckhardt_2025"
    for file in eck_path.glob('*.csv'):
        match3 = re.match(r"(\d+)_streamflow_qc_processed", file.name)
        if match3:
            gage=match3.group(1)
            eck=pd.read_csv(file)
            eck['date'] = pd.to_datetime(eck['date'])
            eck.set_index('date', inplace=True)
            eck.dropna(how='any', inplace=True)
            eck_dic[gage]=eck
    return eck_dic

#merge together NWM, USGS, and Eckhardt dfs based off of gage ID key
def merge_dicts(nwm_dic, usgs_dic, eck_dic):
    print("Merging Dictionaries")
    complete=[]
    common_keys= list(set(nwm_dic).intersection(usgs_dic, eck_dic))
    print("Common Keys:", common_keys)
    for key in common_keys:
        df1=nwm_dic[key].copy()
        df2=usgs_dic[key].copy()
        df3=eck_dic[key].copy()
        df3['gage']=key
        middle=pd.merge(df1, df2, left_index=True, right_index=True)
        final=pd.merge(middle, df3, left_index=True, right_index=True)
        final.dropna(inplace=True)
        complete.append(final)
    complete=pd.concat(complete, ignore_index=False)
    complete['Q_was_non_numeric'] = (
            pd.to_numeric(complete['Q'], errors='coerce').isna()
            & complete['Q'].notna())
    complete['Eckhardt']=pd.to_numeric(complete['Eckhardt'], errors='coerce')
    complete['Q']=pd.to_numeric(complete['Q'], errors='coerce')
    complete['BFIobs']=(complete['Eckhardt'].astype(float))/(complete['Q'].astype(float)) #issue with data types here

    complete['BFIsim']=complete['baseflow']/complete['streamflow']

    finaloutput_path = base_path / 'Complete_inputs4stats.csv'
    finaloutput_path.parent.mkdir(parents=True, exist_ok=True)
    complete.to_csv(finaloutput_path, index=True)
    return complete, common_keys

#creates a new column which identifies the season of the year
def seasons(df):
    conditions = [
        df['month'].isin([12, 1, 2]),
        df['month'].isin([3, 4, 5]),
        df['month'].isin([6, 7, 8]),
        df['month'].isin([9, 10, 11]),
    ]
    choices = [1, 2, 3, 4]
    df['season'] = np.select(conditions, choices, default=np.nan)
    return df

#processes KGE, NSE, and Pearson R for all time frames and outputting
def stats(final_df):
    print("Calculating Statistics")
    #resets date index, and creates year, month, and season columns
    final_df.reset_index(inplace=True)
    final_df['year']=final_df['date'].dt.year
    final_df['month']=final_df['date'].dt.month
    final_df=seasons(final_df)

    #empty list
    rows=[]
    rows2=[]
    rows3=[]

    #RMSE, KGE, and Pearson R calculation for all time
    grouped=final_df.groupby(['gage'])
    for gage, group in grouped:
        rows.append({'gage': gage, 'nse_o': he.nse(group['baseflow'].to_numpy(), group['Eckhardt'].to_numpy()),
                     'rmse_o': he.rmse(group['baseflow'],group['Eckhardt']),
                     'kge_12_o': he.kge_2012(group['baseflow'].to_numpy(), group['Eckhardt'].to_numpy()),
                     'pearson_o': he.pearson_r(group['baseflow'],group['Eckhardt'])}
        )
    overall_stats=pd.DataFrame(rows)
    overall_path= base_path / 'Overall_stats.csv'
    overall_path.parent.mkdir(parents=True, exist_ok=True)
    overall_stats.to_csv(overall_path, index=True)

    #RMSE, KGE, and Pearson R calculation for each year
    grouped2=final_df.groupby(['gage', 'year'])
    for (gage, year), group2 in grouped2:
        rows2.append({'gage': gage, 'year': year, 'nse_o': he.nse(group2['baseflow'].to_numpy(), group2['Eckhardt'].to_numpy()),
                     'rmse_o': he.rmse(group2['baseflow'], group2['Eckhardt']),
                     'kge_12_o': he.kge_2012(group2['baseflow'].to_numpy(), group2['Eckhardt'].to_numpy()),
                     'pearson_o': he.pearson_r(group2['baseflow'], group2['Eckhardt'])}
                    )
    year_stats=pd.DataFrame(rows2)
    year_path = base_path / 'year_stats.csv'
    year_path.parent.mkdir(parents=True, exist_ok=True)
    year_stats.to_csv(year_path, index=True)

    #RMSE, KGE, and Pearson R calculation for each season
    final_df=seasons(final_df)
    grouped3=final_df.groupby(['gage', 'season'])
    for (gage, season), group3 in grouped3:
        rows3.append(
            {'gage': gage, 'season': season, 'nse_o': he.nse(group3['baseflow'].to_numpy(), group3['Eckhardt'].to_numpy()),
             'rmse_o': he.rmse(group3['baseflow'], group3['Eckhardt']),
             'kge_12_o': he.kge_2012(group3['baseflow'].to_numpy(), group3['Eckhardt'].to_numpy()),
             'pearson_o': he.pearson_r(group3['baseflow'], group3['Eckhardt'])}
            )
    season_stats = pd.DataFrame(rows3)
    season_path = base_path / 'seasonal_stats.csv'
    season_path.parent.mkdir(parents=True, exist_ok=True)
    season_stats.to_csv(season_path, index=True)

nwm_dic=nwm_processing(nwm_path, nwm_path_list)
usgs_dic=usgs_processing(usgs_path)
eck_dic=eck_processing(usgs_path)
all_df, common_keys=merge_dicts(nwm_dic=nwm_dic, usgs_dic=usgs_dic, eck_dic=eck_dic)
print(len(eck_dic)) #660 #all being read in file
print(len(nwm_dic)) #670 #all accounted for in files, missing some/not downloaded
print(len(usgs_dic)) #666
print(len(common_keys)) #657
stats(all_df)
#check which gages had errors/didn't go through
binary=all(key in eck_dic for key in common_keys)
binary1=all(key in nwm_dic for key in common_keys)
binary2=all(key in usgs_dic for key in common_keys)
print(binary)
print(binary1)
print(binary2)
if not binary:
    missing=[key for key in common_keys if key not in eck_dic]
    print("The missing Eck gages:")
    print(missing)
if not binary1:
    missing=[key for key in common_keys if key not in nwm_dic]
    print("The missing NWM gages:")
    print(missing)
if not binary2:
    missing=[key for key in common_keys if key not in usgs_dic]
    print("The missing USGS gages:")
    print(missing)






