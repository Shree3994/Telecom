from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, lit, regexp_replace, regexp_extract, round

# Initialize Spark session
spark = SparkSession.builder.appName("CleanCorruptedData").getOrCreate()

# Load datasets
network_optm_df = spark.read.csv(
    r"C:\Users\shree\OneDrive\Desktop\Brain_works-LAPTOP-HAD5JC9F\Telecommunication Project\FINAL_FILES_Initial_Clean\Operations_files\3. Network_Optimization_Data_Table.csv",
    header=True, inferSchema=True)
network_optm_df.show(60)

df = (network_optm_df
      .withColumn("Data_Traffic (%)", regexp_replace(col("Data_Traffic (%)"), "[()%]", "").cast("float"))
      .withColumn("coverage_area(square miles)", regexp_replace(col("coverage_area(square miles)"), "[()square miles]", "").cast("float"))
     )

df.show(50)
df.printSchema()