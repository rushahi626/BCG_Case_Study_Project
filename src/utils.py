import yaml

def load_csv_data_df(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def read_yaml(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)
    
'''def write_output(df,output_path):
    df.write.format("csv").option("header","true").save(output_path)'''

def write_output(df, file_path):
    """
    Write data frame to csv
    :param write_format: Write file format
    :param df: dataframe
    :param file_path: output file path
    :return: None
    """
    df.write.format('csv').mode("overwrite").option("header", "true").save(file_path)