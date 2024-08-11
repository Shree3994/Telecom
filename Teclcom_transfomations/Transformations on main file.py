from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, lit, regexp_replace, regexp_extract, round, avg, max, sum, count

# Initialize Spark session
spark = SparkSession.builder.appName("CleanCorruptedData").getOrCreate()

df_main = spark.read.csv(r"C:\Users\shree\OneDrive\Desktop\Brain_works-LAPTOP-HAD5JC9F\Telecommunication Project\FINAL_FILES_Initial_Clean\part-00000-b8f8fd4c-d7a6-4ecc-b93a-78aa8e399af3-c000.csv", header= True, inferSchema= True)

# df_main.show(50)

df_select_column = df_main.select("ID", "Region","Zone", "Cell_ID","C_Customer_Satisfaction_Score","P_Latency(ms)","P_Signal_Strength(dBm)","P_Throughput(Mbps)","Jitter(ms)","Upload_Speed(Mbps)","Download_Speed(Mbps)","Data_Traffic (%)","Network_Utilization(%)","coverage_area(%)","Service_Availability(%)","S_Packet_Loss(%)","Average_Daily_Load(Mbps)","Total_capacity(Mbps)","User_Count","earthquake_prone_area","flood_prone_area","frequency(MHz)","projected_growth_rate(%)","future_capacity_requirement(Mbps)","fault_type","Mean_Time_to_Repair(mins)","maintenance_status","age_of_equipment(years)")

# Monitor and analyze the performance of cell towers to ensure optimal network efficiency and address any potential issues promptly.
# Main Transformations
# 1.Intruduce new column to calculate Cell_Capacity_Utilization_df
df = df_select_column.withColumn("Cell_Capacity_Utilization(%)",(col("Average_Daily_Load(Mbps)") / col("Total_capacity(Mbps)")) * 100)

##########################################################################################################################

# 2.Intruduce new column to calculate future_capacity_requirement
df = df.withColumn("future_capacity_requirement ",(col("Total_capacity(Mbps)") * col(
                                                "projected_growth_rate(%)") / 100 + col(
                                                "Total_capacity(Mbps)")))

##########################################################################################################################

# 3.Aggregate data to analyze the average and total capacity utilization by region and zone.
df_3 = df.select("Region","Zone", "Cell_Capacity_Utilization(%)", "Total_capacity(Mbps)")

df_agg = df_3.groupBy("Region", "Zone") \
    .agg(
        avg(col("Cell_Capacity_Utilization(%)")).alias("Avg_Utilization(%)"),
        sum(col("Total_capacity(Mbps)")).alias("Max_capacity(Mbps)")
)

# df_agg.show(50)


##########################################################################################################################

# 4.Identify towers with the highest projected growth rates
# df = df_main.select("projected_growth_rate(%)")
# df_high_growth = df_main.orderBy(col("projected_growth_rate(%)").desc())

# df_high_growth.show()

##########################################################################################################################

# 4&5.Identify towers operating at critical capacity utilization levels.Calculate and flag towers with utilization above 90% for potential upgrades. Identify and flag towers with utilization above 90%. Identify towers with the highest projected growth rates
df = df.select("ID", "Region", "Zone", "Cell_Capacity_Utilization(%)","projected_growth_rate(%)")
df_transformed = df.withColumn("Upgrade_Needed",
                               when((col("Cell_Capacity_Utilization(%)") > 90) &
                                    (col("projected_growth_rate(%)") > 5), "Yes")
                               .otherwise("No")).orderBy(col("projected_growth_rate(%)").desc())

# df_transformed.show()

# Group by Upgrade_Needed and count the number of occurrences
# df_count = df_transformed.groupBy("Upgrade_Needed").count()
# df_count.show()

##########################################################################################################################

# 7.Analyze geographical distribution of towers and their load. Calculate the average and maximum load per region.
# Calculate average and maximum load per region
df = df_select_column.select("Region", "Average_Daily_Load(Mbps)",)
df_geo_insights = df.groupBy("Region") \
    .agg(
        avg(col("Average_Daily_Load(Mbps)")).alias("Avg_daily_Load"),
        max(col("Average_Daily_Load(Mbps)")).alias("Max_daily_load")
)

# df_geo_insights.show()

##########################################################################################################################

# 8.Frequency Analysis:Group and analyze towers by frequency bands to determine if certain frequencies are over or under-utilized.
# Analyze towers by frequency bands
df_freq_analysis = df_main.groupBy("frequency(MHz)") \
    .agg( {"Total_capacity(Mbps)": "sum", "Average_Daily_Load(Mbps)": "avg"}) \
    .withColumnRenamed("sum(Total_capacity(Mbps))", "Total_Capacity(Mbps)") \
    .withColumnRenamed("avg(Average_Daily_Load(Mbps))", "Avg_Daily_Load(Mbps)")

# df_freq_analysis.show()

##########################################################################################################################

# 9.Frequency Analysis:Group and analyze towers by frequency bands to determine if certain frequencies are over or under-utilized.
# Analyze towers by frequency bands
df_freq_analysis = df_main.groupBy("frequency(MHz)") \
.agg( {"Total_capacity(Mbps)": "sum", "Average_Daily_Load(Mbps)": "avg"} ) \
    .withColumnRenamed("sum(Total_capacity(Mbps))", "Total_Capacity(Mbps)") \
    .withColumnRenamed("avg(Average_Daily_Load(Mbps))", "Avg_Daily_Load(Mbps)")

# df_freq_analysis.show()

##########################################################################################################################

##################################################################################################################################

# To address the issue of poor customer satisfaction, we need to implement targeted improvement strategies:

Avg_df_conditions = df_select_column.withColumn("customer_satisfaction",
                                                when((col('Jitter(ms)') > 35) &
                                                     (col("Upload_Speed(Mbps)") < 30) &
                                                     (col('Download_Speed(Mbps)') < 200) &
                                                     (col('Service_Availability(%)') > 90) &
                                                     (col('C_Customer_Satisfaction_Score').isin(0, 1)) |
                                                     (col('S_Packet_Loss(%)') > 2), "Poor").otherwise("Average"))

# Avg_df_conditions.show(50)

# Group by customer_satisfaction and count the number of occurrences
# df_count = Avg_df_conditions.groupBy("Region","Zone").count()
df_count = Avg_df_conditions.groupBy("customer_satisfaction").count()

# df_count.show()

########################################################
# Infrastructure Risk Assessment:

df = df_select_column.withColumn("risk_factor",
    when(col("earthquake_prone_area") & col("flood_prone_area"), "High").otherwise("Low")) \
    .withColumn('maintenance_action',
    when(col('age_of_equipment(years)') > 15, 'Need Replacement')
    .when(col('age_of_equipment(years)').between(10, 15), 'Need Maintenance')
    .otherwise('Good condition')) \
    .filter(col('maintenance_action') == 'Need Replacement')
# df.show()
# print(df.count())

###################################################
# Infrastructure Risk Assessment with grouping by Zone and Region
df = (df_select_column.withColumn("risk_factor",
        when(col("earthquake_prone_area") & col("flood_prone_area"), "High")
        .otherwise("Low"))
        .withColumn("maintenance_action",
        when(col('age_of_equipment(years)')
        .between(10, 15), 'Need Maintenance')
        .when(col('age_of_equipment(years)') > 15, 'Need Replacement')
        .otherwise('Good condition'))
        .filter(col('maintenance_action') == 'Need Replacement')
        .groupBy("Region") \
        .agg(count('*').alias('count_of_replacements')))

# Display the grouped and aggregated DataFrame
# df.show()


