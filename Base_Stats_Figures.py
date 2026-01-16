import plotly.graph_objects as go
import pandas as pd

#I want to add a column into each of these files which has their region number...
region_data=r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\CAMELS_gages.xlsx"
overall_path=r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\Overall_stats.csv"
year_path=r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\year_stats.csv"
season_path=r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\seasonal_stats.csv"

#basic pre-processing
o_stats=pd.read_csv(overall_path)
o_stats['gage']=o_stats['gage'].str.strip(to_strip="(,)'")
o_stats.drop(columns='Unnamed: 0', inplace=True)

#retrieving region/gage match data
rlist=['region_1','region_2','region_2_done','region_3_1','region_3_2','region_3_3','region_4','region_5','region_6','region_7','region_8','region_9','region_10'
      ,'region_10_done','region_11','region_12','region_12_done','region_13','region_14','region_15','region_16', 'region_17_1',
       'region_17_2','region_17_3','region_18']
dflist=[]
for region in rlist:
    working_df=pd.read_excel(region_data, sheet_name=region)
    working_df=working_df.iloc[:,0:2]
    #print(working_df['region'].dtype)
    working_df['region']=working_df['region'].astype(float)
    working_df.dropna(inplace=True)
    working_df['region']=working_df['region'].astype(int)
    dflist.append(working_df)

#creation and processing of final_df w/ region, gage, and stats
final_df=pd.concat(dflist)
final_df['gage_id']=final_df['gage_id'].astype(int)
final_df['gage_id']=final_df['gage_id'].astype(str).str.zfill(8)
o_stats.rename(columns={'gage':'gage_id'}, inplace=True)
o_stats=pd.merge(o_stats,final_df,how='inner',on='gage_id')
print(o_stats.head())

fig=go.Figure()
for i in range (0,19):
    ex=o_stats['kge_12_o'].loc[o_stats['region']==i]
    fig.add_trace(go.Box(y=ex))
fig.show()



