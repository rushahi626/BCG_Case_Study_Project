

import yaml
from pyspark.sql import SparkSession
from src.utils import read_yaml,write_output
from src.us_vehicle_accident_analysis import USVehicleAccidentAnalysis

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder.master("local").appName("My BCG Project").getOrCreate()
    # Load the config file
    config_file_name = "config.yaml"
    config = read_yaml(config_file_name)
    output_file_paths = config.get("OUTPUT_PATH")
    
    # Create an instance of the analysis class
    us_accident_obj = USVehicleAccidentAnalysis(spark, config)
    
    # Print the result of the analysis
    print(us_accident_obj.males_killed_count(output_file_paths.get(1)))
    print(us_accident_obj.no_two_wheelers_crashed(output_file_paths.get(2)))
    print(us_accident_obj.top5_carsMakers_crash(output_file_paths.get(3)))
    print(us_accident_obj.lic_veh_HNR_count(output_file_paths.get(4)))
    print(us_accident_obj.state_with_no_female_acdnt(output_file_paths.get(5)))
    print(us_accident_obj.top3to5_vehMake_injury_death(output_file_paths.get(6)))
    print(us_accident_obj.top_ethnic_grp_crash_body_style_wise(output_file_paths.get(7)))
    print(us_accident_obj.top5_zip_cd_with_max_OH_crash(output_file_paths.get(8)))
    print(us_accident_obj.dist_crash_ids_with_insurance(output_file_paths.get(9)))
    print(us_accident_obj.top5_vehmaker_licDriver_tencolor_25states(output_file_paths.get(10)))
    spark.stop()
