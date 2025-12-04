from pyspark.sql.functions import current_date, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DoubleType


my_catalog = 'g5_catalog'
my_prefix = 'ram'

table_input = f'{my_catalog}.bronze.{my_prefix}_spark_input'

source_path = "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G5/archivo_G5_test_20251204_012151_875660.parquet"

schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("email", StringType(), True),
])

spark.sql(f'DROP TABLE IF EXISTS {table_input}')

df_input = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(source_path)
)

df_input = df_input.withColumn("inserted_at", current_timestamp())

df_input.write.format("delta").mode("overwrite").saveAsTable(table_input)

spark.sql(f'SELECT * FROM {table_input}').display()