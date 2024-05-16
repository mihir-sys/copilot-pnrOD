
#Prompt -1 
# /* Generate Python code that imports the necessary modules and functions for a PySpark application. 
# This includes the SparkSession from pyspark.sql, functions from pyspark.sql, udf from pyspark.sql.functions, DoubleType and IntegerType from pyspark.sql.types, and the math functions radians, sin, cos, sqrt, atan2 from the math module. 
# Also import the entire math module and the re module.
# */

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from math import radians, sin, cos, sqrt, atan2
import math
import re

# #Prompt -2 - To create haversine function
# /*
# Create a Python function using PySpark's User Defined Function (UDF) decorator that calculates the haversine distance between two points given their latitudes and longitudes. The latitudes and longitudes are given as strings in the format "degrees.minutes.seconds.direction", where direction is either 'N', 'S', 'E', or 'W' for north, south, east, or west respectively.

# The function should convert the latitude and longitude from degrees, minutes, and seconds to decimal degrees, adjust the sign based on the direction, convert the latitude and longitude from decimal degrees to radians, and then use the haversine formula to calculate the distance between the two points.

# The haversine formula is given by:

# a = sin²(Δφ/2) + cos φ1 ⋅ cos φ2 ⋅ sin²(Δλ/2) c = 2 ⋅ atan2( √a, √(1−a) ) d = R ⋅ c

# where φ is latitude, λ is longitude, R is earth’s radius (mean radius = 6,371km).

# The function should return the distance in kilometers as an integer. If either of the input latitudes is None, the function should return -1. If an exception occurs during the calculation, the function should also return -1.

# After function creation register the UDF
# */



# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

@udf(IntegerType())
def haversine(lat1, lon1, lat2, lon2):
    try:
        if lat1 is None or lat2 is None:
            distance=-1
        elif lat1==lat2:
            distance=0
        else:
            lat1Dir=lat1[-1:]
            lon1Dir=lon1[-1:]
            lat2Dir=lat2[-1:]
            lon2Dir=lon2[-1:]

            lat1=[int(x) for x in lat1[:-1].split(".")]
            lon1=[int(x) for x in lon1[:-1].split(".")]
            lat2=[int(x) for x in lat2[:-1].split(".")]
            lon2=[int(x) for x in lon2[:-1].split(".")]
            # Convert latitude and longitude from degrees, minutes, seconds to decimal degrees
            lat1_decimal = lat1[0] + lat1[1]/60 + lat1[2]/3600
            lon1_decimal = lon1[0] + lon1[1]/60 + lon1[2]/3600
            lat2_decimal = lat2[0] + lat2[1]/60 + lat2[2]/3600
            lon2_decimal = lon2[0] + lon2[1]/60 + lon2[2]/3600

            # Adjust sign based on direction (N, S, E, W)
            if lat1Dir == 'S':
                lat1_decimal *= -1
            if lon1Dir == 'W':
                lon1_decimal *= -1
            if lat2Dir == 'S':
                lat2_decimal *= -1
            if lon2Dir == 'W':
                lon2_decimal *= -1

            # Convert latitude and longitude from decimal degrees to radians
            lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(radians, [lat1_decimal, lon1_decimal, lat2_decimal, lon2_decimal])

            # Haversine formula
            dlon = lon2_rad - lon1_rad
            dlat = lat2_rad - lat1_rad
            a = sin(dlat/2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon/2)**2
            c = 2 * atan2(sqrt(a), sqrt(1-a))
            distance = int(6371 * c)  # Earth radius in kilometers
    except:
        distance=-1
    return distance

# Register the UDF
spark.udf.register("haversine", haversine)


#Prompt -3  - Read Data into pyspark dataframe
# /*
# Generate PySpark code that reads four Delta format datasets located at specific paths: 'distance_master', 'BacktrackExceptionmaster', 'pnr_sample', and 'Location_master'. 

# The data should be read into DataFrames with the same names as the datasets. The header option should be set to True and the schema should be inferred from the data. 

# After reading the data, create temporary tables with the same names as the DataFrames.

# path_distance_master = '/mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master/'
# path_BacktrackExceptionmaster = '/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/BacktrackExceptionmaster'
# path_pnr_sample = '/mnt/ppeedp/raw/competition/pnr_sample'
# path_Location_master = '/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master/'
# */


# Define the paths
path_distance_master = '/mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master/'
path_BacktrackExceptionmaster = '/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/BacktrackExceptionmaster'
path_pnr_sample = '/mnt/ppeedp/raw/competition/pnr_sample'
path_Location_master = '/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master/'

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the distance_master.csv file
distance_master_df = spark.read.format("delta").load(path_distance_master, header=True, inferSchema=True)

# Read the BacktrackExceptionmaster.csv file
BacktrackExceptionmaster_df = spark.read.format("delta").load(path_BacktrackExceptionmaster, header=True, inferSchema=True)

# Read the pnr_sample.csv file
pnr_sample_df = spark.read.format("delta").load(path_pnr_sample, header=True, inferSchema=True)

# Read the Location_master.csv file
Location_master_df = spark.read.format("delta").load(path_Location_master, header=True, inferSchema=True)

# Create temporary tables
distance_master_df.createOrReplaceTempView("distance_master")
BacktrackExceptionmaster_df.createOrReplaceTempView("BacktrackExceptionmaster")
pnr_sample_df.createOrReplaceTempView("pnr_sample")
Location_master_df.createOrReplaceTempView("Location_master")


#Prompt -4 - Transform data to create final table - refer readme file for the prompt input


%sql
WITH TotalJourneyDistance AS (
    SELECT
        dm.ID,
        SUM(
            COALESCE(
                dm.distance_in_km,
                haversine(lm1.longitude, lm1.latitude, lm2.longitude, lm2.latitude)
            )
        ) AS total_journey_distance
    FROM 
        pnr_sample ps
    JOIN 
        distance_master dm ON dm.airport_origin_code = ps.ORIG_AP_CD AND dm.airport_destination_code = ps.DESTN_AP_CD
    LEFT JOIN 
        Location_master lm1 ON dm.airport_origin_code = lm1.ap_cd_val
    LEFT JOIN 
        Location_master lm2 ON dm.airport_destination_code = lm2.ap_cd_val
    GROUP BY 
        dm.ID
),
DirectDisplacement AS (
   SELECT
    ps.PNRHash as id,
    SUM(COALESCE(
        dm.distance_in_km,
        haversine(lm1.longitude, lm1.latitude, lm2.longitude, lm2.latitude)
    )) AS direct_displacement,
    1.6 * MIN(dm.distance_in_km) AS allowed_displacement
FROM 
    pnr_sample ps
JOIN 
    distance_master dm 
    ON dm.airport_origin_code = ps.ORIG_AP_CD AND dm.airport_destination_code = ps.DESTN_AP_CD
LEFT JOIN 
    Location_master lm1 ON dm.airport_origin_code = lm1.ap_cd_val
LEFT JOIN 
    Location_master lm2 ON dm.airport_destination_code = lm2.ap_cd_val
GROUP BY 
    ps.PNRHash
)
SELECT 
  ps.PNRHash PNR,
  ps.OPT_ALN_CD IssueAirlineCode,
  ps.SEG_SEQ_NBR SegSequenceNumber,
  dm.ID AS true_od,
  CASE WHEN ps.OPT_ALN_CD = 'EY' THEN dm.ID ELSE null END AS online_od,
  dm.airport_origin_code as point_of_commencement,
  dm.airport_destination_code as point_of_finish,
  dm.distance_in_km,
  concat(dm.airport_origin_code, '-',ps.OPT_ALN_CD, '-',dm.airport_destination_code) true_od_iteinerary,
  CASE WHEN ps.OPT_ALN_CD = 'EY' THEN concat(dm.airport_origin_code, '-',ps.OPT_ALN_CD, '-',dm.airport_destination_code) ELSE null END as online_od_iteinerary,
  CASE WHEN dd.direct_displacement < dd.allowed_displacement THEN 'Circuit rule' ELSE 'Default rule' END as true_od__break_reason,
  dd.direct_displacement as true_od_distance,
  CASE WHEN ps.OPT_ALN_CD = 'EY' THEN dd.direct_displacement ELSE null end as online_od_distance
FROM delta.`/mnt/ppeedp/raw/competition/pnr_sample` ps
JOIN delta.`/mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master`  dm 
  ON ps.ORIG_AP_CD = dm.airport_origin_code AND ps.DESTN_AP_CD = dm.airport_destination_code
LEFT JOIN delta.`/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/BacktrackExceptionmaster` bem
  ON ps.ORIG_AP_CD = bem.orig AND ps.DESTN_AP_CD = bem.dest
LEFT JOIN delta.`dbfs:/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master/` lm1 
  ON ps.ORIG_AP_CD = lm1.ap_cd_val
LEFT JOIN delta.`dbfs:/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master/` lm2 
  ON ps.DESTN_AP_CD = lm2.ap_cd_val
LEFT JOIN
    TotalJourneyDistance tjd ON ps.PNRHash = tjd.ID
LEFT JOIN
    DirectDisplacement dd ON ps.PNRHash = dd.ID


# #Prompt -5 
# /*
# Create parameterise Python function using PySpark that encapsulates  SQL query and saves the result to a Delta table at a specified path.
# and handel exceptions as well
# */


from pyspark.sql import SparkSession

def execute_query_and_save(spark, output_path=None):
    try:
        # Execute SQL query and save result into a DataFrame
        result_df = spark.sql("""
            WITH TotalJourneyDistance AS (
                SELECT
                    dm.ID,
                    SUM(
                        COALESCE(
                            dm.distance_in_km,
                            haversine(lm1.longitude, lm1.latitude, lm2.longitude, lm2.latitude)
                        )
                    ) AS total_journey_distance
                FROM 
                    delta.`/mnt/ppeedp/raw/competition/pnr_sample` ps
                JOIN 
                    delta.`/mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master` dm 
                    ON dm.airport_origin_code = ps.ORIG_AP_CD AND dm.airport_destination_code = ps.DESTN_AP_CD
                LEFT JOIN 
                    delta.`dbfs:/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master/` lm1 
                    ON dm.airport_origin_code = lm1.ap_cd_val
                LEFT JOIN 
                    delta.`dbfs:/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master/` lm2 
                    ON dm.airport_destination_code = lm2.ap_cd_val
                GROUP BY 
                    dm.ID
            ),
            DirectDisplacement AS (
                SELECT
                    ps.PNRHash as id,
                    SUM(COALESCE(
                        dm.distance_in_km,
                        haversine(lm1.longitude, lm1.latitude, lm2.longitude, lm2.latitude)
                    )) AS direct_displacement,
                    1.6 * MIN(dm.distance_in_km) AS allowed_displacement
                FROM 
                    delta.`/mnt/ppeedp/raw/competition/pnr_sample` ps
                JOIN 
                    delta.`/mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master` dm 
                    ON dm.airport_origin_code = ps.ORIG_AP_CD AND dm.airport_destination_code = ps.DESTN_AP_CD
                LEFT JOIN 
                    delta.`dbfs:/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master/` lm1 
                    ON dm.airport_origin_code = lm1.ap_cd_val
                LEFT JOIN 
                    delta.`dbfs:/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master/` lm2 
                    ON dm.airport_destination_code = lm2.ap_cd_val
                GROUP BY 
                    ps.PNRHash
            )
            SELECT 
              ps.PNRHash PNR,
              ps.OPT_ALN_CD IssueAirlineCode,
              ps.SEG_SEQ_NBR SegSequenceNumber,
              dm.ID AS true_od,
              CASE WHEN ps.OPT_ALN_CD = 'EY' THEN dm.ID ELSE null END AS online_od,
              dm.airport_origin_code as point_of_commencement,
              dm.airport_destination_code as point_of_finish,
              dm.distance_in_km,
              concat(dm.airport_origin_code, '-',ps.OPT_ALN_CD, '-',dm.airport_destination_code) true_od_iteinerary,
              CASE WHEN ps.OPT_ALN_CD = 'EY' THEN concat(dm.airport_origin_code, '-',ps.OPT_ALN_CD, '-',dm.airport_destination_code) ELSE null END as online_od_iteinerary,
              CASE WHEN dd.direct_displacement < dd.allowed_displacement THEN 'Circuit rule' ELSE 'Default rule' END as true_od__break_reason,
              dd.direct_displacement as true_od_distance,
              CASE WHEN ps.OPT_ALN_CD = 'EY' THEN dd.direct_displacement ELSE null end as online_od_distance
            FROM delta.`/mnt/ppeedp/raw/competition/pnr_sample` ps
            JOIN delta.`/mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master`  dm 
              ON ps.ORIG_AP_CD = dm.airport_origin_code AND ps.DESTN_AP_CD = dm.airport_destination_code
            LEFT JOIN delta.`/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/BacktrackExceptionmaster` bem
              ON ps.ORIG_AP_CD = bem.orig AND ps.DESTN_AP_CD = bem.dest
            LEFT JOIN TotalJourneyDistance tjd ON ps.PNRHash = tjd.ID
            LEFT JOIN DirectDisplacement dd ON ps.PNRHash = dd.ID
        """)

        # Display the DataFrame
        result_df.display()

        # Optionally save the DataFrame as a Delta table
        if output_path:
            result_df.write.format("delta").mode("overwrite").save(output_path)

    except Exception as e:
        # Handle exceptions that may occur during query execution or writing
        print("An error occurred:", e)


#Prompt -6 
# /*
# call the function with passing required parameter ad provide the write in location path : /mnt/ppeedp/raw/competition/copilot_output_mihir/od_output
# */

# Example usage
path = '/mnt/ppeedp/raw/competition/copilot_output_mihir/od_output'
spark = SparkSession.builder.appName("Query Execution").getOrCreate()
execute_query_and_save(spark,path)
