from pyspark.sql import SparkSession,Window


class TelecomUtility():

    def dataframe_creator(self,source_file,layer):
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("create_data_frame") \
            .getOrCreate()
        if layer == "raw" :
            base_path_var = r"C:\Users\shree\OneDrive\Desktop\Brain_works-LAPTOP-HAD5JC9F\Telecommunication Project\FINAL_FILES_Initial_Clean\raw_files"
            input_file_path = fr"{base_path_var}\{source_file}.csv"

            df = spark.read.csv(path=input_file_path, header=True,inferSchema=True)
        else:
            base_path_var = r"C:\Users\shree\OneDrive\Desktop\Brain_works-LAPTOP-HAD5JC9F\Telecommunication Project\FINAL_FILES_Initial_Clean\silver"
            input_file_path = fr"{base_path_var}\{source_file}"

            df = spark.read.csv(path=input_file_path, header=True, inferSchema=True)

        return df

    def write_to_silver(self,df,output_folder):
        base_path = r"C:\Users\shree\OneDrive\Desktop\Brain_works-LAPTOP-HAD5JC9F\Telecommunication Project\FINAL_FILES_Initial_Clean\silver"
        output_folder_name = fr"{base_path}\{output_folder}"

        df = df.write.mode("overwrite").csv(path=output_folder_name, header=True)

        return df