from pyspark.sql import Window
import common_utils
from pyspark.sql.functions import *
object_for_common_utils = common_utils.TelecomUtility()

# Reading 0.raw_network_metadata

# Load dataset
raw_network_metadata_df = object_for_common_utils.dataframe_creator("0. raw_Network_Metadata_Table","raw")
window_spec = Window.partitionBy("Network").orderBy("ID")
clean_network_metadata_df = raw_network_metadata_df\
                            .withColumn("Cell_ID", explode(split(col("Cell_ID"), ",\\s*|\\sand\\s")))\
                            .withColumn("New_ID", row_number().over(window_spec))\
                            .drop("ID").withColumnRenamed("New_ID", "ID")\
                            .withColumn("Date",to_date(col("Date"),"dd-MM-yyyy"))\
                            .select("ID", "Date", "Region", "Zone", "Network", "Cell_ID")
# clean_network_metadata_df.show(50)
# clean_network_metadata_df.printSchema()
object_for_common_utils.write_to_silver(clean_network_metadata_df,"silver_network_metadata")

# Reading 1. raw_network_performance_data

raw_Performance_Monitoring_df = object_for_common_utils.dataframe_creator("1. raw_Performance_Monitoring_Table","raw")

clean_Performance_Monitoring_df = raw_Performance_Monitoring_df.withColumn("P_Latency(ms),P_Signal_Strength(dBm),P_Throughput(Mbps)", split(col("P_Latency(ms),P_Signal_Strength(dBm),P_Throughput(Mbps)"),","))\
    .withColumn("P_Latency(ms)",col("P_Latency(ms),P_Signal_Strength(dBm),P_Throughput(Mbps)").getItem(0))\
    .withColumn("P_Signal_Strength(dBm)",col("P_Latency(ms),P_Signal_Strength(dBm),P_Throughput(Mbps)").getItem(1))\
    .withColumn("P_Throughput(Mbps)",col("P_Latency(ms),P_Signal_Strength(dBm),P_Throughput(Mbps)").getItem(2)).drop("P_Latency(ms),P_Signal_Strength(dBm),P_Throughput(Mbps)")\
                                .dropDuplicates()\
                                .fillna({'P_Latency(ms)': 0, 'Performance_id': 'Unknown'})\
                                .filter((col('Performance_id') != 'Unknown') & (col('ID') != 0) & (col('P_Latency(ms)') != 0) & (
                                col('P_Signal_Strength(dBm)') != 0) & (col('P_Throughput(Mbps)') != 0))\
                                .withColumn('Performance_id',regexp_replace(col('Performance_id'),'[^a-zA-Z0-9]', ''))\
                                .filter((col('P_Latency(ms)') >= 0) & (col('P_Latency(ms)') <= 100))\
                                .orderBy('ID')
# clean_Performance_Monitoring_df.show(50)
object_for_common_utils.write_to_silver(clean_Performance_Monitoring_df,"silver_performance_monitoring")

# Reading 2. raw_Customer_Experience_Management_Data

# Load dataset
raw_Customer_Experience_Management_df = object_for_common_utils.dataframe_creator("2.raw_Customer_Experience_Management_Table","raw")

clean_Customer_Experience_Management_df = raw_Customer_Experience_Management_df\
                                            .dropDuplicates()\
                                            .na.drop()\
                                            .withColumn('Experience_id', regexp_replace(col('Experience_id'), '[^a-zA-Z0-9]', '')).withColumn("C_Connection_Drop_Rate(%)", round(
                                            col("C_Connection_Drop_Rate(%)"), 2))\
                                            .withColumn(
                                            "C_Customer_Satisfaction_Score",
                                            when(col("C_Customer_Satisfaction_Score") > 5, 5)
                                            .when(col("C_Customer_Satisfaction_Score") < 0, 0)
                                            .otherwise(col("C_Customer_Satisfaction_Score")))\
                                            .orderBy('ID')

object_for_common_utils.write_to_silver(clean_Customer_Experience_Management_df,"silver_Customer_Experience_Management")



# Reading 3. raw_network_optimization_data

raw_network_optimization_df= object_for_common_utils.dataframe_creator("3. raw_Network_Optimization_Data_Table","raw")
# raw_network_optimization_df.show(100)

clean_network_optimisation_df = raw_network_optimization_df\
                                .withColumn("Data_Traffic (%)", regexp_replace(col("Data_Traffic (%)"), "[()%]", "").cast("float"))\
                                .withColumn("coverage_area(square miles)", regexp_replace(col("coverage_area(square miles)"), "[()square miles]", "").cast("float"))

object_for_common_utils.write_to_silver(clean_network_optimisation_df,"silver_network_optimisation")

# Reading 4. Service_Assurance_Data

raw_Service_Assurance_df = object_for_common_utils.dataframe_creator("4.raw_Service_Assurance_Data","raw")

clean_Service_Assurance_df = raw_Service_Assurance_df\
                            .withColumn("Service_id", regexp_replace(col("Service_id"), "[^a-zA-Z0-9]", ''))\
                            .withColumn("Service_Availability(%)", round(col("Service_Availability(%)"), 2))\
                            .orderBy('Service_id')\
                            .dropDuplicates(["Service_id"])\
                            .orderBy("ID")
# clean_Service_Assurance_df.show(50)
# clean_Service_Assurance_df.printSchema()
object_for_common_utils.write_to_silver(clean_Service_Assurance_df,"silver_Service_Assurance")

# Reading 5. raw_network_analytics_data

raw_Network_Analytics_Data_df = object_for_common_utils.dataframe_creator("5. raw_Network_Analytics_Data","raw")
# raw_Network_Analytics_Data_df.show(50)
# print(raw_Network_Analytics_Data_df.count())

clean_network_analytics_df = raw_Network_Analytics_Data_df\
                                .withColumn("Analytics_id", when(col("Analytics_id").isNotNull(), regexp_replace(col("Analytics_id"),"_", "")))\
                                .withColumn("Active_users", regexp_replace(col("Active_users"), r'[^0-9]', ""))\
                                .dropna()\
                                .filter(length(col("Analytics_id")) <= 5)\
                                .withColumn("Active_users", regexp_replace(col("Active_users"), ",", ""))\
                                .dropDuplicates(['Analytics_id'])\
                                .orderBy("ID").withColumn("Active_Users",col("Active_Users").cast("integer"))
# clean_network_analytics_df.show(50)
# print(clean_network_analytics_df.count())
object_for_common_utils.write_to_silver(clean_network_analytics_df,"silver_network_analytics")

# Reading 6. raw_RAN_management_data

# Load dataset
raw_RAN_Management_df = object_for_common_utils.dataframe_creator("6. raw_RAN_Management_Data","raw")
# raw_RAN_Management_df.show(60)

clean_RAN_Management_df = raw_RAN_Management_df\
                            .dropDuplicates()\
                            .fillna({'R_Handover_Success_Rate(%)': 0, 'Radio_id': 'Unknown'})\
                            .filter((col('Radio_id') != 'Unknown') & (col('ID') != 0) & (col('R_Handover_Success_Rate(%)') != 0) & (
                                col('Signal-to-Noise_Ratio(dB)') != 0))\
                            .withColumn('Radio_id', regexp_replace(col('Radio_id'), '[^a-zA-Z0-9]', ''))\
                            .withColumn('R_Handover_Success_Rate(%)',round(col('R_Handover_Success_Rate(%)'), 2))\
                            .orderBy('ID')

object_for_common_utils.write_to_silver(clean_RAN_Management_df,"silver_RAN_Management")

# 7. Read raw_optimal_cell_tower_placement_table

raw_cell_tower_placement_df = object_for_common_utils.dataframe_creator("7. raw_Optical_Cell_Tower_Placement_Data","raw")

clean_cell_tower_placement_df = raw_cell_tower_placement_df\
                                .withColumn("latitude(degrees)", round(col("latitude(degrees)"), 2))\
                                .withColumn("longitude(degrees)", round(col("longitude(degrees)"), 2))\
                                .drop_duplicates(["cell_tower_id"])\
                                .dropna(subset=['latitude(degrees)', 'longitude(degrees)'])\
                                .filter(~col("soil_type").rlike(r'[^a-zA-Z0-9]')).orderBy("Region","cell_tower_id").withColumnRenamed("cell_tower_id","cell_tower_id_ctp").drop("region","zone")

# clean_cell_tower_placement_df.show(60)
object_for_common_utils.write_to_silver(clean_cell_tower_placement_df,"silver_cell_tower_placement")

# Reading 8. raw_tower_range_and_coverage_data

raw_Tower_range_coverage_df = object_for_common_utils.dataframe_creator("8. raw_Tower_Range_and_Coverage_Data","raw")

clean_Tower_range_coverage_df = raw_Tower_range_coverage_df.dropDuplicates()\
                                .na.drop()\
                                .withColumn('region', regexp_replace(col('region'), '[^a-zA-Z0-9]', ''))\
                                .filter((col('zone').isin('1', '2', '3')))\
                                .orderBy('region','cell_tower_id').drop("region","zone","latitude(degrees)","longitude(degrees)")

# clean_Tower_range_coverage_df.show(60)
object_for_common_utils.write_to_silver(clean_Tower_range_coverage_df,"silver_Tower_range_coverage")

# Reading 9. raw_tower_capacity_data

raw_tower_capacity_df = object_for_common_utils.dataframe_creator("9. raw_Tower_capacity","raw")

clean_tower_capacity_df = raw_tower_capacity_df\
                            .withColumn("Zone", when(col("Zone").isin(["1", "2", "3"]), col("Zone")).otherwise(lit(None)))\
                            .withColumn("cell_tower_id", when(col("cell_tower_id").isNotNull(), regexp_replace(col("cell_tower_id"), "_", " ")).otherwise(lit(None)))\
                            .withColumn("SpecialChars", regexp_extract(col("region"), r'[^a-zA-Z0-9\s]', 0))\
                            .dropna()\
                            .dropDuplicates()

clean_tower_capacity_df = clean_tower_capacity_df[clean_tower_capacity_df['SpecialChars'] == '']

clean_tower_capacity_df = clean_tower_capacity_df.withColumnRenamed("frequency", "frequency (MHz)") \
                                               .withColumnRenamed("Average_Daily_Load(Mbps)", "Average_Daily_Load (Mbps)")\
                                                .drop("SpecialChars","latitude","longitude","cell_tower_id","region","zone","current_load").orderBy("ID")

object_for_common_utils.write_to_silver(clean_tower_capacity_df,"silver_tower_capacity")


#  Reading File 10.raw_Fault_Detection_Data

raw_fault_detection_df = object_for_common_utils.dataframe_creator("10.  raw_Fault_Detection_Data","raw")

clean_fault_detection_df = raw_fault_detection_df\
                .drop_duplicates()\
                .dropna()\
                .withColumn('fault_type', regexp_replace(col('fault_type'),'[^a-zA-Z0-9]', ''))\
                .withColumn('detection_method',when(raw_fault_detection_df.detection_method.contains("Auto"), "Automatic").otherwise("Manual"))\
                .filter(col("Mean_Time_to_Repair(mins)") < 150)\
                .filter(raw_fault_detection_df.affected_services.isin(["Voice", "Both", "Data"])).orderBy("ID")

# clean_fault_detection_df.show(50)
object_for_common_utils.write_to_silver(clean_fault_detection_df,"silver_fault_detection")

# 11. read raw_maintenance_data

raw_Maintenance_data_df = object_for_common_utils.dataframe_creator("11. raw_Maintenance_data","raw")
# raw_Maintenance_data_df.show(60)

clean_Maintenance_data_df = raw_Maintenance_data_df\
                            .dropDuplicates()\
                            .na.drop()\
                            .withColumn('fault_id', regexp_replace(col('fault_id'), '[^a-zA-Z0-9\-]', ''))\
                            .orderBy('ID').withColumn("time_stamp", to_date(col("time_stamp"), "dd-MM-yyyy"))

# clean_Maintenance_data_df.show()
# clean_Maintenance_data_df.printSchema()

object_for_common_utils.write_to_silver(clean_Maintenance_data_df,"silver_Maintenance_data")


