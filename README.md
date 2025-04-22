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
**Outputs: Eckhardt baseflow time-series in m3/s in a .csv file**
Required dataset: [CAMELs] (https://gdex.ucar.edu/dataset/camels.html) </br>
Required inputs: 
Dependent Libraries: [Baseflow] (https://github.com/xiejx5/baseflow) by xiejx5 </br>
```
pip install pandas baseflow
```
Eckhardt Code.py is a script which requires the input of a USGS streamflow file and converts it into a
time series formatted baseflow csv file utilizing the Eckhardt digital filter. 


## Part 2: NWM Retrospective Dataset Access & Extraction
**Outputs: NWM baseflow time series in cfs in a .parquet file**
Required datasets: </br>
Required inputs: list of USGS gage IDs, local crosstable path, desired output folder </br>
Dependent Libraries: </br>
```
pip install pandas dask s3fs xarray pyarrow baseflow matplotlib
```
NWM_GW_Retrieval.py sets up a series of functions for the processing and extraction of baseflow data from the
retrospective NWM database. Accessed databases can be any of the available versions within the amazon web-service.
An example implementation of these functions is in the NWM_example.py.

