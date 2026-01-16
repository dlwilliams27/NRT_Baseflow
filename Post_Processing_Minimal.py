import HydroErr as he
import numpy as np
import xarray as xr
import pandas as pd
import re
import pathlib as path

'''This code is a single gage file example for debugging post_processing.py'''

def nwm_processing(nwm_file):
    match = re.match(r"NWM_gage_(\d+)", nwm_file.name)
    if match:
        gage = match[1].zfill(8)
        ds=xr.open_dataset(nwm_file)
        nwm = ds.to_dataframe()
        nwm['baseflow'] = nwm['q_lateral'] + nwm['qBucket']
        nwm.reset_index(inplace=True)
        nwm['date'] = nwm['time'].dt.date
        nwm_avg_base = nwm.groupby('date', sort=True)['baseflow'].mean()
        nwm_avg_stream = nwm.groupby('date', sort=True)['streamflow'].mean()
        nwm_tot = pd.merge(nwm_avg_base, nwm_avg_stream, left_index=True, right_index=True)
        nwm_tot.reset_index(inplace=True)
        nwm_tot.set_index('date', inplace=True)
        print(nwm_tot.head())
        nwm_dic[gage] = nwm_tot
    else:
        print(f"No match for file {nwm_file}.")
    return nwm_dic

def usgs_processing(usgs_file):
    match2 = re.match(r"(\d+)_streamflow_qc", usgs_file.name)
    if match2:
        gage2=match2.group(1)
        usgs = pd.read_csv(usgs_file, sep=' ', on_bad_lines='skip')
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
    else:
        print(f"Column indexing error for file: {usgs_file}")
    return usgs_dic

def eck_processing(eck_file):
    match3 = re.match(r"(\d+)_streamflow_qc_processed", eck_file.name)
    if match3:
        gage=match3.group(1)
        eck=pd.read_csv(eck_file)
        eck['date'] = pd.to_datetime(eck['date'])
        eck.set_index('date', inplace=True)
        eck_dic[gage]=eck
    else:
        print(f"Error for file {eck_file}.")
    return eck_dic

def merge_dicts(nwm_dic, usgs_dic, eck_dic):
    complete=[]
    common_keys= list(set(nwm_dic).intersection(usgs_dic, eck_dic))
    for key in common_keys:
        df1=nwm_dic[key].copy()
        print("NWM dataframe:", df1)
        df2=usgs_dic[key].copy()
        print("USGS dataframe:", df2)
        df3=eck_dic[key].copy()
        print("eck dataframe:", df3)
        df3['gage']=key
        middle=pd.merge(df1, df2, left_index=True, right_index=True)
        print("middle merge:",middle.head())
        final=pd.merge(middle, df3, left_index=True, right_index=True)
        print("final merge:",final.head())
        complete.append(final)
    complete=pd.concat(complete, ignore_index=False)
    complete['BFIobs']=(complete['Eckhardt'].astype(float))/(complete['Q'].astype(float)) #issue with data types here
    complete['BFIsim']=complete['baseflow']/complete['streamflow']
    return complete

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

def stats(final_df, min_path):
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
    overall_stats.to_csv(min_path / "overall_stats.csv", index=True)

    #RMSE, KGE, and Pearson R calculation for each year
    grouped2=final_df.groupby(['gage', 'year'])
    for (gage, year), group2 in grouped2:
        rows2.append({'gage': gage, 'year': year, 'nse_o': he.nse(group2['baseflow'].to_numpy(), group2['Eckhardt'].to_numpy()),
                     'rmse_o': he.rmse(group2['baseflow'], group2['Eckhardt']),
                     'kge_12_o': he.kge_2012(group2['baseflow'].to_numpy(), group2['Eckhardt'].to_numpy()),
                     'pearson_o': he.pearson_r(group2['baseflow'], group2['Eckhardt'])}
                    )
    year_stats=pd.DataFrame(rows2)
    year_stats.to_csv(min_path / "year_stats.csv", index=True)

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
    season_stats.to_csv(min_path / "season_stats.csv", index=True)

if __name__ == '__main__':
    nwm_dic={}
    eck_dic={}
    usgs_dic={}

    min_path = path.Path(r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\Debugging")

    nwm_file= path.Path(r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\NWM_Results\region_02\NWM_gage_1544500.nc")
    usgs_file= path.Path(r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\Initial_Results\USGS_Streamflow_2024\02\01544500_streamflow_qc.txt")
    eck_file= path.Path(r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\Initial_Results\Eckhardt_2024\01544500_streamflow_qc_processed.csv")

    nwm_wdic=nwm_processing(nwm_file)
    usgs_wdic=usgs_processing(usgs_file)
    eck_wdic=eck_processing(eck_file)
    fin_df=merge_dicts(nwm_wdic, usgs_wdic, eck_wdic)
    print(fin_df)
    stats(fin_df, min_path)