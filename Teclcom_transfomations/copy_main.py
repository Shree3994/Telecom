import common_utils
from pyspark.sql.functions import *
object_for_common_utils = common_utils.TelecomUtility()

###### Reading Operations Tables ######

metadata_df = object_for_common_utils.dataframe_creator("silver_network_metadata","silver").withColumn("Date",to_date("Date","dd-MM-yyyy"))
# metadata_df.printSchema()
# metadata_df.show(50)
# print(metadata_df.count())


performance_monitoring_df = object_for_common_utils.dataframe_creator("silver_performance_monitoring","silver")
# performance_monitoring_df.show(50)
# performance_monitoring_df.printSchema()
# print(performance_monitoring_df.count())


customer_experience_df = object_for_common_utils.dataframe_creator("silver_Customer_Experience_Management","silver")
# customer_experience_df.show(60)
# customer_experience_df.printSchema()
# print(customer_experience_df.count())


network_optimization_df = object_for_common_utils.dataframe_creator("silver_network_optimisation","silver")
# network_optimization_df.show(60)
# network_optimization_df.printSchema()
# print(network_optimization_df.count())


service_assurance_df = object_for_common_utils.dataframe_creator("silver_Service_Assurance","silver")
# service_assurance_df.show()
# service_assurance_df.printSchema()
# print(service_assurance_df.count())


network_analysis_df = object_for_common_utils.dataframe_creator("silver_network_analytics","silver")
# network_analysis_df.show()
# network_analysis_df.printSchema()
# print(network_analysis_df.count())


ran_df = object_for_common_utils.dataframe_creator("silver_RAN_Management","silver")
# ran_df.show()
# ran_df.printSchema()
# print(ran_df.count())


# sla_df = spark.read.csv(path=r"C:\Users\shree\OneDrive\Desktop\Brain_works-LAPTOP-HAD5JC9F\Telecommunication Project\FINAL_FILES_Initial_Clean\Operations_files\13. SLA.csv",header=True,inferSchema=True)
# # sla_df.show()
# # sla_df.printSchema()

##### Reading Infrastructure Tables #####

cell_tower_placement_df = object_for_common_utils.dataframe_creator("silver_cell_tower_placement","silver")
# cell_tower_placement_df.show(50)
# cell_tower_placement_df.printSchema()
# print(cell_tower_placement_df.count())


tower_range_and_coverage_df = object_for_common_utils.dataframe_creator("silver_Tower_range_coverage","silver")
# tower_range_and_coverage_df.show(60)
# tower_range_and_coverage_df.printSchema()
# print(tower_range_and_coverage_df.count())


tower_capacity_df = object_for_common_utils.dataframe_creator("silver_tower_capacity","silver")
# tower_capacity_df.show(60)
# tower_capacity_df.printSchema()
# print(tower_capacity_df.count())


fault_detection_df = object_for_common_utils.dataframe_creator("silver_fault_detection","silver").withColumn("time_stamp",to_timestamp(col("time_stamp"),"dd-MM-yyyy HH:mm")).withColumn("time_stamp",to_date(col("time_stamp"),"dd-MM-yyyy"))
# fault_detection_df.show(50)
# fault_detection_df.printSchema()
# print(fault_detection_df.count())

maintenance_df = object_for_common_utils.dataframe_creator("silver_Maintenance_data", "silver").withColumn("time_stamp",
                                                                                                           to_date(
                                                                                                               col("time_stamp"),
                                                                                                               "dd-MM-yyyy")).withColumnRenamed(
    "time_stamp", "need_maintenance_time_stamp").withColumn("scheduled_maintenance_date",
                                                            to_date(col("scheduled_maintenance_date"),
                                                                    "dd-MM-yyyy")).withColumn("last_maintenance",
                                                                                              to_date(
                                                                                                  col("last_maintenance"),
                                                                                                  "dd-MM-yyyy")).withColumnRenamed(
    "fault_id", "fault_id_maintenance_table")

# maintenance_df.show(50)
# maintenance_df.printSchema()
# print(maintenance_df.count())


###### JOINING ALL TABLES ######

joined_df = (
    metadata_df.join(performance_monitoring_df, "ID", "inner").join(customer_experience_df, "ID", "inner").join(
        network_optimization_df, "ID", "inner").join(service_assurance_df, "ID", "inner").join(network_analysis_df,
                                                                                               "ID", "inner").join(
        ran_df, "ID", "inner").join(cell_tower_placement_df,
                                    metadata_df.Cell_ID == cell_tower_placement_df.cell_tower_id_ctp, "inner").join(
        tower_range_and_coverage_df,
        cell_tower_placement_df.cell_tower_id_ctp == tower_range_and_coverage_df.cell_tower_id, "inner").join(
        tower_capacity_df, "ID", "inner").join(fault_detection_df, "ID", "inner").join(maintenance_df, "ID", "inner"))

# joined_df.show(50)
# print(joined_df.count())
# joined_df.write.csv(path=r"C:\Users\iamsh\Desktop\FINAL_FILES_Initial_Clean_data\all_tables_joined_final",header=True)

######## Business Requirement 1 : Checking Performance of 5G wireless services across zones using composite index Quality of Services (QoS) Index: The output of this business requirement is as per the data generated by the Applications of the telecom company.

# selecting the required columns :

req1_select_columns_df = joined_df.select("ID", "Date", metadata_df.Region, metadata_df.Zone, "Network", "Cell_ID",
                                          "P_Latency(ms)", "P_Signal_Strength(dBm)", "P_Throughput(Mbps)",
                                          "C_Customer_Satisfaction_Score", "Service_Availability(%)",
                                          "C_Connection_Drop_Rate(%)", "Network_Utilization(%)", "S_Packet_Loss(%)",
                                          "R_Handover_Success_Rate(%)", "O_Interference_Level_(dB)")

# Normalizing performance metrics to a range from 0 to 100 :

normalize_metrics_df = req1_select_columns_df.withColumn('norm_Latency_score',when(col("P_Latency(ms)") < 0, 100).when(col("P_Latency(ms)") > 100, 0).otherwise(100 - col("P_Latency(ms)"))).withColumn("norm_Signal_Strength",when(col("P_Signal_Strength(dBm)") > -50, 100).when(col("P_Signal_Strength(dBm)") < -100, 0).otherwise(2 * col("P_Signal_Strength(dBm)")+200)).withColumn("norm_service_availability", when(col("Service_Availability(%)") < 90, 0).otherwise(10 * col("Service_Availability(%)")-900)).withColumn("norm_connection_drop_rate",when(col("C_Connection_Drop_Rate(%)") > 2, 0).otherwise((-50 * col("C_Connection_Drop_Rate(%)"))+ 100)).withColumn("norm_Handover_Success_Rate", when(col("R_Handover_Success_Rate(%)") < 90, 0).otherwise(10 * col("R_Handover_Success_Rate(%)")-900)).withColumn("norm_Packet_Loss_rate",when(col("S_Packet_Loss(%)") > 3, 0).otherwise((-33.33 * col("S_Packet_Loss(%)")+ 100))).withColumn("norm_customer_satisfaction_rate",when(col("C_Customer_Satisfaction_Score") > 5, 100).otherwise(20 * col("C_Customer_Satisfaction_Score"))).withColumn("norm_Network_Utilization",when(col("Network_Utilization(%)") < 60, 100).otherwise((-2.5 * col("Network_Utilization(%)")+250))).withColumn("norm_throughtput_rate",when(col("P_Throughput(Mbps)") > 500, 100).when(col("P_Throughput(Mbps)") < 50, 0).otherwise((0.22 * col("P_Throughput(Mbps)")) - 11.11 )).withColumn("norm_interference_level_rate",when(col("O_Interference_Level_(dB)") > -50, 100).when(col("O_Interference_Level_(dB)") < -100, 0).otherwise((2 * col("O_Interference_Level_(dB)"))+ 200))

# aggregating the normalized metrics to find out the Quality score out of 100.

with_metrics_agg_df = normalize_metrics_df.select("Cell_ID","norm_Latency_score","norm_Signal_Strength","norm_service_availability","norm_connection_drop_rate","norm_Handover_Success_Rate","norm_Packet_Loss_rate","norm_customer_satisfaction_rate","norm_Network_Utilization","norm_throughtput_rate","norm_interference_level_rate").withColumn("Quality_Score",col("norm_Latency_score")+col("norm_Signal_Strength")+col("norm_service_availability")+col("norm_connection_drop_rate")+col("norm_Handover_Success_Rate")+col("norm_Packet_Loss_rate")+col("norm_customer_satisfaction_rate")+col("norm_Network_Utilization")+col("norm_throughtput_rate")+col("norm_interference_level_rate")).withColumn("Quality_Score",(1.25 * col("Quality_Score"))/10)

# Defining Quality Of Service index on the basis of Quality Score.

QoS_Index_df = with_metrics_agg_df.withColumn("QoS",when(col("Quality_Score")>75,"Excellent").when(col("Quality_Score")<50,"Poor").otherwise("Good"))
# QoS_Index_df.show(50)
# print(QoS_Index_df.count())
# Finding The total number of Excellent, Average and Poor Zones.
count_QoS = QoS_Index_df.select("QoS").groupBy("QoS").count()
# count_QoS.show()
# QoS_Index_df.show(50)

######Business Requirement 2 : Checking Performance of 5G wireless services across zones based upon the customer feedback.

#selecting the required columns needed to fulfil this business requirement

req2_select_columns_df =joined_df.select("ID","Region","Zone","Cell_ID","C_Customer_Satisfaction_Score","P_Latency(ms)","P_Signal_Strength(dBm)","P_Throughput(Mbps)","Jitter(ms)","Upload_Speed(Mbps)","Download_Speed(Mbps)","Data_traffic (%)","Network_utilization(%)","coverage_area(%)","Service_Availability(%)","S_Packet_Loss(%)","User_Count","earthquake_prone_area","flood_prone_area","frequency (MHz)","Total_capacity(Mbps)","Average_Daily_Load (Mbps)","projected_growth_rate(%)","fault_type","Mean_Time_to_Repair(mins)","maintenance_status")

# Defining what relates to excellent, average or poor customer feedback.

CS_parameters_df  = joined_df.select("Region","Zone","Cell_ID","C_Customer_Satisfaction_Score","C_Connection_Drop_Rate(%)","Jitter(ms)","Upload_Speed(Mbps)","Download_Speed(Mbps)","P_Latency(ms)","P_Signal_Strength(dBm)","O_interference_level_(dB)").withColumn("Customer_Satisfaction",
                   when((
                       (col("C_Customer_Satisfaction_Score") <= 1) |
                       (col("C_Connection_Drop_Rate(%)") > 1.25) &
                       (col("Upload_Speed(Mbps)") < 30) &
                       (col("Download_Speed(Mbps)") < 200) &
                       (col("P_Latency(ms)") > 50) &
                       (col("O_interference_level_(dB)") < -95) &
                       ((col("P_Signal_Strength(dBm)") < -75)) &
                       (col("Jitter(ms)") > 35)), "poor"
                   ).when((
                       (col("C_Customer_Satisfaction_Score") >= 4) |
                       (col("C_Connection_Drop_Rate(%)") < 0.75) &
                       (col("Upload_Speed(Mbps)") > 70) &
                       (col("Download_Speed(Mbps)") > 350) &
                       (col("P_Latency(ms)") < 25) &
                       (col("O_interference_level_(dB)") > -81) &
                       ((col("P_Signal_Strength(dBm)") > -61)) &
                        (col("Jitter(ms)") < 15)), "excellent"
                   ).otherwise("average"))


# CS_parameters_df.show(50)
# print(CS_parameters_df.count())
# Evaluating the number of cell tower areas  and their performance based upon the feedback provided by the customers.
# Every zone has 3 cell tower areas. Similarly, every region has 9 cell tower areas.
CS_count_df = (CS_parameters_df.groupBy("Customer_Satisfaction").count().withColumnRenamed("count","Number_of_cell_tower_areas"))
# CS_count_df.show()
# CS_count_df.printSchema()

# Evaluating the Performance of service for every region based upon customer feedback.
CS_count_region_df = CS_parameters_df.groupBy("Region","Customer_Satisfaction").count().withColumnRenamed("count","Number_of_Cell_Tower_Areas").orderBy("Region")
# CS_count_region_df.show()

# Finding out those Cell Towers that have been flagged as poor by customers

poor_areas_df = CS_parameters_df.filter(col("Customer_Satisfaction")=="poor").select("Region","Zone","Cell_ID","Customer_Satisfaction")
# poor_areas_df.show()

# Finding the total number of cell towers and then finding the percentage of cell towers providing excellent, average and poor service as per customer feedback.

total_cell_tower_areas = CS_count_df.agg(sum(col("Number_of_cell_tower_areas"))).collect()[0][0]
# print(total_cell_tower_areas)

percentage_distribution_df = CS_count_df.withColumn("Percentage",(col("Number_of_cell_tower_areas") / total_cell_tower_areas) * 100)
# percentage_distribution_df.show()
# It means that 17.77% of cell towers are able to provide excellent service as per customer feedback.

############################################## Combining the Outputs of both the above requirements to find out those Cell Towers that are providing poor service as per both - the company data as well as the customer feedback #############################################

req1_req2_df = QoS_Index_df.join(CS_parameters_df,"Cell_ID","inner").select("Region","Zone","Cell_ID","QoS","Customer_Satisfaction").filter((col("QoS")== "Poor") & (col("Customer_Satisfaction")=="poor"))
# req1_req2_df.show(50)


##### Business requirement 3:

# 1. Infra/Cell Tower Risk assessment on the basis of earthquake prone area and flood prone area(if both conditions are met then give high otherwise low)
risk_assessment_df = joined_df.withColumn("risk_factor", when((col("earthquake_prone_area") & col("flood_prone_area")),"High").otherwise("Low"))
# risk_assessment_df.show(50)

# 2. Maintenance action to be taken on the basis of age of equipment:
maintenance_action_df = joined_df.withColumn("maintenance_action", when(col('age_of_equipment') > 25, 'Need Replacement').when(col('age_of_equipment').between(15, 25), 'Need Maintenance')
                        .otherwise('Good condition'))
# maintenance_action_df.show(50)
#
#
# 3. Replacement of equipment on the basis of maintenance action

replace_equip_df = maintenance_action_df.filter(col("maintenance_action") == "Need Replacement")
# replace_equip_df.show()
# print(replace_equip_df.count())

# 4. Maintenance of equipment on the basis of maintenance action
maintain_equip_df = maintenance_action_df.filter(col("maintenance_action") == "Need Maintenance")
# maintain_equip_df.show()
# print(maintain_equip_df.count())

#5 Infrastructure that requires urgent attention: factors : high risk, need to be replaced.

infra_immediate_upgrade = risk_assessment_df.join(maintenance_action_df,"Cell_ID","inner").filter((col("risk_factor")=="High")& (col("maintenance_action")=="Need Replacement"))
# infra_immediate_upgrade.show()

######## Business Requirement 4: Future Proofing Cell Towers : Enhancing the traffic handling capacity of cell towers based upon projected growth rates :

req2_select_columns_df =joined_df.select("ID","Region","Zone","Cell_ID","C_Customer_Satisfaction_Score","P_Latency(ms)","P_Signal_Strength(dBm)","P_Throughput(Mbps)","Jitter(ms)","Upload_Speed(Mbps)","Download_Speed(Mbps)","Data_traffic (%)","Network_utilization(%)","coverage_area(%)","Service_Availability(%)","S_Packet_Loss(%)","User_Count","earthquake_prone_area","flood_prone_area","frequency (MHz)","Total_capacity(Mbps)","Average_Daily_Load (Mbps)","projected_growth_rate(%)","fault_type","Mean_Time_to_Repair(mins)","maintenance_status")
# req2_select_columns_df.show(50)

# 1.Introduce new column to calculate Cell_Capacity_Utilization_df
Cell_Capacity_Utilization_df = req2_select_columns_df.withColumn("Cell_Capacity_Utilization(%)",(col("Average_Daily_Load (Mbps)") / col("Total_capacity(Mbps)"))* 100)
# Cell_Capacity_Utilization_df.show(50)

# 2.Intruduce new column to calculate future_capacity_requirement
future_capacity_requirement_df  = Cell_Capacity_Utilization_df.withColumn("future_capacity_requirement(Mbps) ",(col("Total_capacity(Mbps)") * col(
                                                "projected_growth_rate(%)") / 100 + col(
                                                "Total_capacity(Mbps)")))
# future_capacity_requirement_df.show(50)

# 3.Aggregate data to analyze the average and total capacity utilization by region and zone.
utilization_df = future_capacity_requirement_df.select("Region","Zone", "Cell_Capacity_Utilization(%)", "Total_capacity(Mbps)")

capacity_utilization_by_zone = utilization_df.groupBy("Region", "Zone") \
    .agg(
        {"Cell_Capacity_Utilization(%)": "avg", "Total_capacity(Mbps)": "sum"}
    ) \
    .withColumnRenamed("avg(Cell_Capacity_Utilization(%))", "Average_Capacity_Utilization(%)") \
    .withColumnRenamed("sum(Total_capacity(Mbps))", "Total_Capacity(Mbps)")

# capacity_utilization_by_zone.show()

# 4.Identify towers with the highest projected growth rates
df = future_capacity_requirement_df.select("Cell_ID","projected_growth_rate(%)")
df_high_growth = df.orderBy(col("projected_growth_rate(%)").desc())

# df_high_growth.show()

# 5.Determine the towers that need immediate upgrades based on current and projected capacity utilization
immediate_upgrade_df = future_capacity_requirement_df.select("ID", "Region", "Zone", "Cell_ID","Cell_Capacity_Utilization(%)","projected_growth_rate(%)")
immediate_upgrade_df = immediate_upgrade_df.withColumn("Upgrade_Needed", when((col("Cell_Capacity_Utilization(%)") > 90) & (col("projected_growth_rate(%)")>5), "Yes").otherwise("No"))
# need to add projected_growth_rate > 5%.
immediate_upgrade_df = immediate_upgrade_df.filter(col("Upgrade_Needed")== "Yes")
# immediate_upgrade_df.show()





