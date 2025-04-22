# -*- coding: utf-8 -*-
"""
Created on Thu Mar 27 10:35:30 2025

@author: Delanie Williams
"""

import pandas as pd
import dask.dataframe as dd
import dask
import os
from dask.diagnostics import ProgressBar

#Conversion of USGS ID to COMID
def USGS_to_NWM(USGS_gage, crosstable_path):
    df = pd.read_csv(crosstable_path)  
    gage_no=df['Gage_no']
    COMID=df['COMID']
    if USGS_gage in gage_no.values:
        row_number=gage_no.index[gage_no == USGS_gage][0]
        return COMID[row_number]
    else:
        print("the USGS ID %s does not have a COMID match" %(USGS_gage))
        return None
    
#def extract_baseflow(COMID_individ, dataset):
#.compute() is for intermediate saving, only when necessary or crash memory 
def extract_baseflow_NWM(COMID_individ, ds):
    qBucket=ds["qBucket"]
    baseflow_timeseries=qBucket.sel(feature_id=COMID_individ)
    with ProgressBar():
        df=baseflow_timeseries.to_dataframe().reset_index()
        ddf=dd.from_pandas(df, npartitions=10).compute()
    return ddf

#convert to parquet instead of .csv for efficiency
def clean(ddf,USGS_gage,output_path):
    ddf=ddf.drop(['elevation','latitude','longitude','order'], axis=1)
    ddf.to_parquet(output_path, engine="pyarrow", compression="snappy")
    
#gonna have the NWM files by USGS id for comparison
def process_one_gage(USGS_gage, crosstable_path, ds, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    COMID_individ=USGS_to_NWM(USGS_gage, crosstable_path)
    if COMID_individ:
        ddf=extract_baseflow_NWM(COMID_individ, ds)
        output_path=os.path.join(output_folder, f"NWM_gage_{USGS_gage}.parquet")
        clean(ddf, USGS_gage, output_path)
        
def process_gages(USGS_gages, crosstable_path, ds, output_folder):
    for gage in USGS_gages:
        process_one_gage(gage, crosstable_path, ds, output_folder)
        
    
