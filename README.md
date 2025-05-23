# NRT_Baseflow
The following code was used for the computation of Eckhardt baseflow and extraction of NWM baseflow values.
```
├── NWM_GW_Retrieval.py       # Functions to extract and save NWM baseflow data
├── NWM_example.py            # Example script running NWM retrieval for selected gages
├── Eckhardt Code.py          # Processes USGS streamflow files and applies Eckhardt filter
├── README.md                 # You are here
├── LICENSE                   # Licensure details
```


## Part 1: USGS Streamflow into Eckhardt Baseflow
**Outputs: Eckhardt baseflow time-series in m3/s in a .csv file** </br>
Required datasets: [CAMELs] (https://gdex.ucar.edu/dataset/camels.html) **or** any other streamflow timeseries. </br>
Dependent Libraries: [Baseflow] (https://github.com/xiejx5/baseflow) by xiejx5 </br>
Required inputs: local input folder path, local output folder path
For set up, use the following: 

```
pip install pandas baseflow numpy os
```
Eckhardt Code.py is a script which requires the input of a USGS streamflow file and converts it into a
time series formatted baseflow csv file utilizing the Eckhardt digital filter. 


## Part 2: NWM Retrospective Dataset Access & Extraction
**Outputs: NWM baseflow time series in cfs in a .parquet file**
Required datasets: </br>
Required inputs: list of USGS gage IDs, local crosstable path, desired output folder </br>
For set-up, use the following:
```
pip install pandas dask s3fs xarray pyarrow
```
NWM_GW_Retrieval.py sets up a series of functions for the processing and extraction of baseflow data from the
retrospective NWM database. Accessed databases can be any of the available versions within the amazon web-service.
An example implementation of these functions is in the NWM_example.py.

The NWM_GW_Retrieval code can be altered to extract other output variables from the dataset. 
```
print(ds)
```
Using one of the attributes/variables printed, the following functions can be altered: Extract_GW_NWM
```
qBucket=ds['qBucket'] --> q=ds['streamflow']
```

