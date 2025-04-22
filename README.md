# NRT_Baseflow
The following code was used for the computation of Eckhardt baseflow and extraction of NWM baseflow values.

## Part 1: USGS Streamflow Inputs into Eckhardt Baseflow
Required dataset: [CAMELs] (https://gdex.ucar.edu/dataset/camels.html) </br>
Dependent Libraries: Baseflow </br>
Eckhardt Code.py is a script which requires the input of a USGS streamflow file and converts it into a
time series formatted baseflow csv file utilizing the Eckhardt digital filter. 



## Part 2: NWM Retrospective Dataset Access & Extraction
Required datasets: </br>
Dependent Libraries: </br>
NWM_GW_Retrieval.py sets up a series of functions for the processing and extraction of baseflow data from the
retrospective NWM database. Accessed databases can be any of the available versions within the amazon web-service.
An example implementation of these functions is in the NWM_example.py.
