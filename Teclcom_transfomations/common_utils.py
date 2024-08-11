from pyspark.sql import SparkSession,Window


class TelecomUtility():

    def dataframe_creator(self,source_file,layer):
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("create_data_frame") \
            .getOrCreate()
        if layer == "raw" :
            base_path_var = r"C:\Users\iamsh\Desktop\FINAL_FILES_Initial_Clean_data\raw_files"
            input_file_path = fr"{base_path_var}\{source_file}.csv"

            df = spark.read.csv(path=input_file_path, header=True,inferSchema=True)
        else:
            base_path_var = r"C:\Users\iamsh\Desktop\FINAL_FILES_Initial_Clean_data\silver"
            input_file_path = fr"{base_path_var}\{source_file}"

            df = spark.read.csv(path=input_file_path, header=True, inferSchema=True)

        return df

    def write_to_silver(self,df,output_folder):
        base_path = r"C:\Users\iamsh\Desktop\FINAL_FILES_Initial_Clean_data\silver"
        output_folder_name = fr"{base_path}\{output_folder}"

        df = df.write.mode("overwrite").csv(path=output_folder_name, header=True)

        return df