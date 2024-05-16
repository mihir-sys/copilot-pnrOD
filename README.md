# copilot-pnrOD

Please use the following prompt to generate the code blocks and refer main.py for the final code.

#Prompt -1 
Generate Python code that imports the necessary modules and functions for a PySpark application. 
This includes the SparkSession from pyspark.sql, functions from pyspark.sql, udf from pyspark.sql.functions, DoubleType and IntegerType from pyspark.sql.types, and the math functions radians, sin, cos, sqrt, atan2 from the math module. 
Also import the entire math module and the re module.

#Prompt -2 - To create haversine function

Create a Python function using PySpark's User Defined Function (UDF) decorator that calculates the haversine distance between two points given their latitudes and longitudes. The latitudes and longitudes are given as strings in the format "degrees.minutes.seconds.direction", where direction is either 'N', 'S', 'E', or 'W' for north, south, east, or west respectively.

The function should convert the latitude and longitude from degrees, minutes, and seconds to decimal degrees, adjust the sign based on the direction, convert the latitude and longitude from decimal degrees to radians, and then use the haversine formula to calculate the distance between the two points.

The haversine formula is given by:

a = sin²(Δφ/2) + cos φ1 ⋅ cos φ2 ⋅ sin²(Δλ/2) c = 2 ⋅ atan2( √a, √(1−a) ) d = R ⋅ c

where φ is latitude, λ is longitude, R is earth’s radius (mean radius = 6,371km).

The function should return the distance in kilometers as an integer. If either of the input latitudes is None, the function should return -1. If an exception occurs during the calculation, the function should also return -1.

After function creation register the UDF

#Prompt -3  - Read Data into pyspark dataframe

Generate PySpark code that reads four Delta format datasets located at specific paths: 'distance_master', 'BacktrackExceptionmaster', 'pnr_sample', and 'Location_master'. 

The data should be read into DataFrames with the same names as the datasets. The header option should be set to True and the schema should be inferred from the data. 

After reading the data, create temporary tables with the same names as the DataFrames.

path_distance_master = '/mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master/'
path_BacktrackExceptionmaster = '/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/BacktrackExceptionmaster'
path_pnr_sample = '/mnt/ppeedp/raw/competition/pnr_sample'
path_Location_master = '/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master/'

#Prompt -4 - Transform data to create final table

Write SQL query is used to calculate the total journey distance and direct displacement for each passenger in a flight dataset.

first creates two subqueries using CTE: TotalJourneyDistance and DirectDisplacement.

Using haversine formula if distance_in_km is null in CTE table

TotalJourneyDistance calculates the total journey distance for each ID in the distance_master table. sums up the distances between the origin and destination airports,
 either using the distance provided in the distance_master table or calculating it using the haversine function if the distance is not available.

DirectDisplacement calculates the direct displacement for each passenger (identified by PNRHash in the pnr_sample table). calculate similar way to TotalJourneyDistance, but also calculates an allowed_displacement which is 1.6 times the minimum distance in the distance_master table.

In The main query then selects various details about each passenger's journey, including the passenger's ID (PNRHash), the airline code (OPT_ALN_CD), the sequence number of the segment (SEG_SEQ_NBR), the origin and destination airports, the true and online itineraries, the reason for the break in the true OD (origin-destination), and the true and online OD distances.

Write join queries for several tables, including pnr_sample, distance_master, BacktrackExceptionmaster, and Location_master, as well as the TotalJourneyDistance and DirectDisplacement subqueries. 

#Please refer following schema details :

Table : pnr_sample
-------------------------
PNRHash:string
SEG_SEQ_NBR:string
ORIG_AP_CD:string
DESTN_AP_CD:string
OPT_ALN_CD:string
OPT_ALN_FLT_NBR:string
MKT_ALN_CD:string
fl_dt:string

Table : distance_master
-------------------------
ID:string
airport_origin_code:string
airport_destination_code:string
distance_in_km:integer
distance_in_miles:decimal(14,2)
source:string
from_dt:date
thru_dt:date

Table : BacktrackExceptionmaster
-------------------------
orig:string
dest:string

Table : Location_master
-------------------------
ap_cd_val:string
latitude:string
longitude:string


#Prompt -5 
Create parameterise Python function using PySpark that encapsulates  SQL query and saves the result to a Delta table at a specified path.
and handel exceptions as well

#Prompt -6 
Call the function with passing required parameter ad provide the write location path 
