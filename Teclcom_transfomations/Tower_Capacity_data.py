from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, lit, regexp_replace, regexp_extract, round

# Initialize Spark session
spark = SparkSession.builder.appName("CleanCorruptedData").getOrCreate()

# Load datasets
tower_capacity_df = spark.read.csv(
    r"C:\Users\shree\OneDrive\Desktop\Brain_works-LAPTOP-HAD5JC9F\Telecommunication Project\FINAL_FILES_Initial_Clean\input_files\Tower_capacity.csv",
    header=True, inferSchema=True)
# tower_capacity_df.show(60)



# Cleaning_data

# 1.Handle invalid zones
valid_zones = ["1", "2", "3"]
tower_capacity_df = tower_capacity_df.withColumn("Zone",
                                                 when(col("Zone").isin(valid_zones), col("Zone")).otherwise(lit(None)))


# 2.Handle invalid Cell_ID (removing underscores, fixing formats)
tower_capacity_df = tower_capacity_df.withColumn("cell_tower_id", when(col("cell_tower_id").isNotNull(), regexp_replace(col("cell_tower_id"), "_", " "))
.otherwise(lit(None)))


# 3.Define the regex pattern for special characters
pattern = r'[^a-zA-Z0-9\s]'
# Extract special characters
tower_capacity_df = tower_capacity_df.withColumn("SpecialChars", regexp_extract(col("region"), pattern, 0))


# 4. removing missing value rows
tower_capacity_df = tower_capacity_df.dropna()

# 5.Remove duplicate records
tower_capacity_df = tower_capacity_df.dropDuplicates()

# 6.Drop rows where SpecialChars column has a value
tower_capacity_df = tower_capacity_df[tower_capacity_df['SpecialChars'] == '']


# Main Transformations
# 1.Intruduce new column to calculate Cell_Capacity_Utilization_df
cell_Capacity_Utilization_df = tower_capacity_df.withColumn(
    "Cell_Capacity_Utilization(%)",
    (col("Average_Daily_Load") / col("Total_capacity(Mbps)")) * 100
)

# 2.Intruduce new column to calculate future_capacity_requirement
future_capacity_requirement = cell_Capacity_Utilization_df.withColumn("future_capacity_requirement ",
                                                                      (col("Total_capacity(Mbps)") * col(
                                                                          "projected_growth_rate") / 100 + col(
                                                                          "Total_capacity(Mbps)")))

# 3.Rename columns as per standards
tower_capacity_df = future_capacity_requirement.withColumnRenamed("latitude", "latitude (degrees)") \
                                               .withColumnRenamed("longitude", "longitude (degrees)") \
                                               .withColumnRenamed("frequency", "frequency (MHz)") \
                                               .withColumnRenamed("Average_Daily_Load", "Average_Daily_Load (%)") \
                                               .drop("SpecialChars")

# tower_capacity_df.show(50)

# 4.Round the latitude column to 2 decimal points
tower_capacity_df = tower_capacity_df.withColumn("latitude (degrees)", round(col("latitude (degrees)"), 2))

tower_capacity_df = tower_capacity_df.orderBy("ID")
# tower_capacity_df.show(50)
# print(tower_capacity_df.count())

# --------------------------------------------------------------------------------------------------------------------------------
# Load Data file
fault_df = spark.read.csv(
    r"C:\Users\shree\OneDrive\Desktop\Brain_works-LAPTOP-HAD5JC9F\Telecommunication Project\FINAL_FILES_Initial_Clean\input_files\10.  Fault_Detection_Data.csv",
    header=True, inferSchema=True)

# fault_df.show()

# 1. dropping duplicates

distinct_df = fault_df.drop_duplicates()
# distinct_df.show(70)
# print("Count of Records After Removing Duplicates:", distinct_df.count())


# 2. removing missing value rows

# miss_df = distinct_df.dropna()
#
# print("Count of Records After Removing Missing Rows:", miss_df.count())

# 3. removing special characters from specific column

# special_df = miss_df.withColumn('fault_type', regexp_replace(col('fault_type'),
#                                                              '[^a-zA-Z0-9]', ''))
# special_df.show()

# 4. handling invalid names for detection_method column

# detect_df = special_df.withColumn('detection_method',
#                                   when(special_df.detection_method.contains("Auto"), "Automatic")
#                                  .otherwise("Manual"))
# detect_df.show()

# 5. handling mean time repair if more than 150 min drop

# repair_df = detect_df.filter(col("Mean_Time_to_Repair(mins)") < 150)

# repair_df.show()

# 6. affected service check

# listvalues = ["Voice", "Both", "Data"]

# affected_df = repair_df.filter(repair_df.affected_services.isin(listvalues))
#
# affected_df.show()

###############################################################################################################

df_final = spark.read.csv(r"C:\Users\shree\OneDrive\Desktop\Brain_works-LAPTOP-HAD5JC9F\Telecommunication Project\FINAL_FILES_Initial_Clean\part-00000-b8f8fd4c-d7a6-4ecc-b93a-78aa8e399af3-c000.csv", header= True, inferSchema= True)

df_final.show()
