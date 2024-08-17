from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import utils
from utils import comm_df

if __name__ == '__main__':
    cmn_ut = comm_df()
#########################################################################################
# Data file 1 Table_name = T_73
    # condition 1 => fetch table number =  on table_numberas 1,2,3,
    df1 = cmn_ut.create_df("T_73","Table_number","73.#1")
    # df1.show()

    df2 = cmn_ut.create_df("T_73","Table_number","73.#2")
    # df2.show()
    #
    df3 = cmn_ut.create_df("T_73","Table_number","73.#3")
    # df3.show()
################################################################################################

#Transformations for table_number "73.#1"

# Extract Amount column into Min_Amount and Max_Amount
df_amt1 = cmn_ut.separate_amount(df1)
df_amt1 = cmn_ut.separate_int_alpha(df_amt1)
df_amt1 = cmn_ut.selected_column(df_amt1)
# df_amt1.show()
# print(df_amt1.count())

########################################################################

#Transformations for table_number "73.#2"

# Extract Amount column into Min_Amount and Max_Amount
df_amt2 = cmn_ut.separate_amount(df2)
# df_amt2.show()
# print(df_amt2.count())

# Extract numerical and alphabetical parts from SYMBOL
df_sep_apl2 = cmn_ut.separate_int_alpha(df_amt2)
# df_sep_apl2.show()
# print(df_sep_apl2.count())

########################################################################

#Transformations for table_number "73.#3"

# Extract Amount column into Min_Amount6 and Max_Amount
df_amt3 = cmn_ut.separate_amount(df3)
# df_amt3.show()
# print(df_amt3.count())

# Extract numerical and alphabetical parts from SYMBOL
df_sep_apl3 = cmn_ut.separate_int_alpha(df_amt3)
# df_sep_apl3.show()
# print(df_sep_apl3.count())

#########################################################################

#Transformations for table_number "75.#1"
# Data file 2 Table_name = T_75

T_75_df1 = cmn_ut.create_df("T_75","Table_number","75.#1")
T_75_df1 = cmn_ut.add_rename_and_separate_amount(T_75_df1)
T_75_df1 = cmn_ut.separate_int_alpha(T_75_df1)
T_75_df1.show(50)
print(T_75_df1.count())


T_75_df2 = cmn_ut.create_df("T_75","Table_number","75.#2")
T_75_df2 = cmn_ut.add_rename_and_separate_amount(T_75_df2)
T_75_df2 = cmn_ut.separate_int_alpha(T_75_df2)
# T_75_df2.show()
# print(T_75_df2.count())


T_75_df3 = cmn_ut.create_df("T_75","Table_number","75.#3")
T_75_df3 = cmn_ut.add_rename_and_separate_amount(T_75_df3)
T_75_df3 = cmn_ut.separate_int_alpha(T_75_df3)
# T_75_df3.show()
# print(T_75_df3.count())

T_75_df4 = cmn_ut.create_df("T_75","Table_number","75.#4")
T_75_df4 = cmn_ut.add_rename_and_separate_amount(T_75_df4)
T_75_df4 = cmn_ut.separate_int_alpha(T_75_df4)
# T_75_df4.show()
# print(T_75_df4.count())

T_75_df5 = cmn_ut.create_df("T_75","Table_number","75.#5")
T_75_df5 = cmn_ut.add_rename_and_separate_amount(T_75_df5)
T_75_df5 = cmn_ut.separate_int_alpha(T_75_df5)
# T_75_df5.show()
# print(T_75_df5.count())

T_75_df6 = cmn_ut.create_df("T_75","Table_number","75.#6")
T_75_df6 = cmn_ut.add_rename_and_separate_amount(T_75_df6)
T_75_df6 = cmn_ut.separate_int_alpha(T_75_df6)
# T_75_df6.show()
# print(T_75_df6.count())

T_75_df7 = cmn_ut.create_df("T_75","Table_number","75.#7")
T_75_df7 = cmn_ut.add_rename_and_separate_amount(T_75_df7)
T_75_df7 = cmn_ut.separate_int_alpha(T_75_df7)
# T_75_df7.show()
# print(T_75_df7.count())

T_75_df8 = cmn_ut.create_df("T_75","Table_number","75.#8")
T_75_df8 = cmn_ut.add_rename_and_separate_amount(T_75_df8)
T_75_df8 = cmn_ut.separate_int_alpha(T_75_df8)
# T_75_df8.show()
# print(T_75_df8.count())

T_75_df9 = cmn_ut.create_df("T_75","Table_number","75.#9")
T_75_df9 = cmn_ut.add_rename_and_separate_amount(T_75_df9)
T_75_df9 = cmn_ut.separate_int_alpha(T_75_df9)
# T_75_df9.show()
# print(T_75_df9.count())

######################################################################################

#Transformations for table_number "75.#10"
# Data file 3 Table_name = T_75_3
# Extract Amount column into Min_Amount and Max_Amount

T_75_3_df10 = cmn_ut.create_df("T_75_3","Table_number","75.#10")
T_75_3_df10 = cmn_ut.add_rename_and_separate_amount(T_75_3_df10)
T_75_3_df10 = cmn_ut.separate_int_alpha(T_75_3_df10)
# T_75_3_df10.show()
# print(T_75_3_df10.count())

T_75_3_df11 = cmn_ut.create_df("T_75_3","Table_number","75.#11")
T_75_3_df11 = cmn_ut.add_rename_and_separate_amount(T_75_3_df11)
T_75_3_df11 = cmn_ut.separate_int_alpha(T_75_3_df11)
# T_75_3_df11.show()
# print(T_75_3_df11.count())

T_75_3_df12 = cmn_ut.create_df("T_75_3","Table_number","75.#12")
T_75_3_df12 = cmn_ut.add_rename_and_separate_amount(T_75_3_df12)
T_75_3_df12 = cmn_ut.separate_int_alpha(T_75_3_df12)
# T_75_3_df12.show()
# print(T_75_3_df12.count())

T_75_3_df13 = cmn_ut.create_df("T_75_3", "Table_number", "75.#13")
T_75_3_df13 = cmn_ut.add_rename_and_separate_amount(T_75_3_df13)
T_75_3_df13 = cmn_ut.separate_int_alpha(T_75_3_df13)
# T_75_3_df13.show()
# print(T_75_3_df13.count())


T_75_3_df14 = cmn_ut.create_df("T_75_3","Table_number","75.#14")
T_75_3_df14 = cmn_ut.add_rename_and_separate_amount(T_75_3_df14)
T_75_3_df14 = cmn_ut.separate_int_alpha(T_75_3_df14)
# T_75_3_df14.show()
# print(T_75_3_df14.count())

T_75_3_df15 = cmn_ut.create_df("T_75_3","Table_number","75.#15")
T_75_3_df15 = cmn_ut.add_rename_and_separate_amount(T_75_3_df15)
T_75_3_df15 = cmn_ut.separate_int_alpha(T_75_3_df15)
# T_75_3_df15.show()
# print(T_75_3_df15.count())

T_75_3_df16 = cmn_ut.create_df("T_75_3","Table_number","75.#16")
T_75_3_df16 = cmn_ut.add_rename_and_separate_amount(T_75_3_df16)
T_75_3_df16 = cmn_ut.separate_int_alpha(T_75_3_df16)
# T_75_3_df16.show()
# print(T_75_3_df16.count())

T_75_3_df17 = cmn_ut.create_df("T_75_3","Table_number","75.#17")
T_75_3_df17 = cmn_ut.add_rename_and_separate_amount(T_75_3_df17)
T_75_3_df17 = cmn_ut.separate_int_alpha(T_75_3_df17)
# T_75_3_df17.show()
# print(T_75_3_df17.count())

T_75_3_df18 = cmn_ut.create_df("T_75_3","Table_number","75.#18")
T_75_3_df18 = cmn_ut.add_rename_and_separate_amount(T_75_3_df18)
T_75_3_df18 = cmn_ut.separate_int_alpha(T_75_3_df18)
# T_75_3_df18.show()
# print(T_75_3_df18.count())

T_75_3_df19 = cmn_ut.create_df("T_75_3","Table_number","75.#19")
T_75_3_df19 = cmn_ut.add_rename_and_separate_amount(T_75_3_df19)
T_75_3_df19 = cmn_ut.separate_int_alpha(T_75_3_df19)
# T_75_3_df19.show()
# print(T_75_3_df19.count())

T_75_3_df20 = cmn_ut.create_df("T_75_3","Table_number","75.#20")
T_75_3_df20 = cmn_ut.add_rename_and_separate_amount(T_75_3_df20)
T_75_3_df20 = cmn_ut.separate_int_alpha(T_75_3_df20)
# T_75_3_df20.show()
# print(T_75_3_df20.count())

##############################################################################

