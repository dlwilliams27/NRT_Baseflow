# -*- coding: utf-8 -*-
"""
Created on Fri Apr  4 12:05:54 2025

@author: Delanie Williams
"""

import os 
import pandas as pd
from datetime import datetime
import s3fs
import xarray as xr
import dask.array as da
import NWM_GW_Retrieval as NWM

# Define the AWS S3 bucket and dataset path
s3_bucket = "noaa-nwm-retrospective-3-0-pds"
zarr_path_chrt = "CONUS/zarr/chrtout.zarr"
zarr_path_force="CONUS/zarr/forcing/v2d.zarr"
prefix="CONUS/zarr"

# Create an S3 filesystem object with anonymous access
fs = s3fs.S3FileSystem(anon=True)

# Open the chrt dataset remotely
ds = xr.open_zarr(fs.get_mapper(f"s3://{s3_bucket}/{zarr_path_chrt}"), consolidated=False
                 ,chunks={'time':1000})

crosstable_path=r'C:\Users\Delanie Williams\OneDrive - The University of Alabama\Coding\NRT Eckhardt Project\NMW_GW_data\swim_gage.csv'

region_gages={ 
    "Region_1":[1054200,1187300,1121000,1123000],
    "Region_2":[1557500,2038850],
    "Region_5":[3384450],
    "Region_6":[3450000,3455500], #error in gauge id in spatial extent .xslx
    "Region_7":[5454000],
    "Region_10":[6803510,6622700,6614800],
    "Region_11":[7195800,7335700],
    "Region_12":[8050800,8103900],
    "Region_13":[8271000],
    "Region_16":[10310500,10205030],
    "Region_17":[12447390,13018300,14362250],
    "Region_18":[11481200,11237500,11230500,11143000]
}

for region_name, gages in region_gages.items():
    output_folder=region_name
    NWM.process_gages(gages, crosstable_path, ds, output_folder)