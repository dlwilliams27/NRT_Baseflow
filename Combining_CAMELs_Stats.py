import pandas as pd
import pathlib as path
import os

camels_path=r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\CAMELS_data\all_attr.csv"
stats_path=r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\Overall_stats.csv"

camels_df=pd.read_csv(camels_path)
stats_df=pd.read_csv(stats_path)

#remove all the weird characters
stats_df['gage']=stats_df['gage'].str.strip(to_strip="(,)'")

#Adding leading zero before camels_df
camels_df['gage'] = camels_df['gauge_id'].astype(str).str.zfill(8)

#setting the index to gage
camels_df.set_index('gage', inplace=True)
stats_df.set_index('gage', inplace=True)

#merging
combined=pd.merge(stats_df,camels_df, how='inner', left_index=True,right_index=True)
print("All:", combined)

#exporting to csv
csv_path= path.Path(r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project")
combined.to_csv(csv_path / "Camels_Stats_Combined.csv")