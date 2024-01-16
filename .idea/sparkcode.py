from pyspark.sql import SparkSession
from pyspark.sql.functions import col

S3_DATA_SOURCE_PATH = 'gs://spark-code-dag-test/source/spark_source_data.csv' # location of csv data file
S3_DATA_OUTPUT_PATH = 'gs://spark-code-dag-test/target/' # output files saving location

def func_run():
    spark = SparkSession.builder.appName('hmda_app').getOrCreate()
    all_data = spark.read.csv(S3_DATA_SOURCE_PATH, header=True)
    selected_data = all_data.select('id', 'name', 'salary')
    selected_data.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH)
    print('Total number of records: %s' % all_data.count())

if __name__ == "__main__":
    func_run()