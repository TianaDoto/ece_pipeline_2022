from pyspark.sql import SparkSession
import pandas as pd
import pyspark.sql.types as T
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder\
    .appName("ECE pipeline")\
    .getOrCreate()

filepath = "/home/heritiana/Project/Projet_exam/data/tour2.csv"

pdf = pd.read_csv(filepath, header = 0, sep = ";", encoding="latin_1")  

#creer un nouveau schema pour attribuer un nom au columns qui n ont pas de nom

column = pdf.columns.tolist()
temp_schema = []
for i in range (len(column), 35):
    temp_schema.append("col"+str(i))
schema = column + temp_schema

final_schema = T.StructType()
for items in schema: 
    final_schema.add(items, T.StringType(), True)

df = spark.read.option("encoding", "latin1").schema(final_schema).options(delimiter=";").csv(filepath)

df = df.withColumnRenamed("Code du b.vote", "Code du b vote")

df = df.na.drop()

new_names = []
x = 1

for j in range(21, 28):
    new_names.append(df.columns[j] + "_" + str(x))
old_col = df.columns[28:105]        
mapping = dict(zip(old_col, new_names))

# df1 contenant les candidats qui avaient pas de schema
df1 = df.select([col(c).alias(mapping.get(c, c)) for c in df.columns[28:35]])

#df2 contenant df sans les candidats sans schema
df2 = df.drop(*old_col)

df1 = df1.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
df2 = df2.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))

#df3 contenant df1 et df2 cad df initial avec tous les candidats avec schema
df_final = df2.join(df1, df2.row_index == df1.row_index, "outer").drop("row_index")

#drop les columns qui ne sont pas pertinent (comme les pourcentage car on peut les calculer facilement au besoin)
cols_to_drop = [c for c in df_final.columns if "%" in c]
df_final = df_final.drop(*cols_to_drop)

df_final.write.option("header",True) \
    .options(delimiter=";")\
    .options(overwrite = True)\
    .csv("/home/heritiana/Project/Projet_exam/data/tour2_par_departement")
