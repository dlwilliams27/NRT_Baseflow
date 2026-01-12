# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 21:13:41 2025

@author: Delanie Williams
"""
#required libraries
import baseflow 
import pandas as pd
import os

#determining streamflow column 
def find_numeric_column(df, min_valid_values=30):
    best_col= None
    best_count=0
    for col in df.columns[5:9]: 
        try: 
            converted=pd.to_numeric(df[col], errors='coerce')
            num_valid= converted.notna().sum()
            
            if num_valid> best_count and num_valid >= min_valid_values:
                best_col = col
                best_count=num_valid
        except: 
            continue
    return best_col

#directories
folder_path=r'C:\Users\Delanie Williams\OneDrive - The University of Alabama\Coding\NRT Eckhardt Project\USGS_Streamflow'
output_folder=r'C:\Users\Delanie Williams\OneDrive - The University of Alabama\Coding\NRT Eckhardt Project\Eckhardt'

#list of regions run through
subfolder_list=[]

#Iteration process through each subfolder and file within
for subfolder in os.listdir(folder_path):
    subfolder_path=os.path.join(folder_path,subfolder)
    subfolder_list.append(subfolder)
    for filename in os.listdir(subfolder_path):
        if filename.endswith(".txt"):
            path= os.path.join(folder_path, filename)
            trial=pd.read_csv(path, sep=" ",header=None,on_bad_lines='skip')
            q_col=find_numeric_column(trial)
            if q_col is None:
                print(f"Skipping {filename}- no good streamflow column")
                continue
            columns=[1,2,3,q_col]
            extracted_df=trial.iloc[:,columns]
            extracted_df.columns=['year','month','day','q']
            dates=pd.to_datetime(extracted_df[['year','month','day']], errors='coerce')
            working_df=pd.concat([dates,extracted_df['q']],axis=1)
            working_df.columns=['date','q']
            #turning into a series
            working_df['q']=pd.to_numeric(working_df['q'],errors='coerce')
            working_df.dropna(subset=['q'], inplace=True)

            working_series=working_df.set_index('date')['q']
            if working_series.isna().any() or not pd.api.types.is_numeric_dtype(working_series):
                print(f"Skipping {filename}- likely wrong column reference")
                continue
            else:
                #creation of baseflow value array
                new=baseflow.single(working_series,method='Eckhardt',return_kge=True)
                baseflow_series, kge_value=new
                #convert to m3/s
                baseflow_series=baseflow_series*(.3048**3)
                new_filename=filename.replace(".txt","_processed.csv")
                new_file_path=os.path.join(output_folder, new_filename)
                baseflow_series.to_csv(new_file_path)

        
