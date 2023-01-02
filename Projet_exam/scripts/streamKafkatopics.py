from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

#Read the csv to a rdd
file1 = spark.read.csv('data/2022-12-27-15_49_30-tour1.csv')
file2 = spark.read.csv('data/2022-12-27-16_01_19-tour2.csv')
print(file2.take(4))
#Rename columns using a dataframe
df = file1.toDF(*("Code du departement","Libellé du département", "Code de la circonscription", "Libellé de la circonscription", "Code de la commune", "Libellé de la commune", "Code du b vote", "Inscrits", "Abstentions", "% Abs/Ins", "Votants", "% Vot/Ins", "Blancs", "% Blancs/Ins", "% Blancs/Vot", "Nuls", "% Nuls/Ins", "% Nuls/Vot", "Exprimés", "% Exp/Ins", "% Exp/Vot", "N°Panneau c1", "Sexe c1", "Nom c1", "Prénom c1", "Voix c1", "% Voix/Ins c1", "% Voix/Exp c1", "N°Panneau c2", "Sexe c2", "Nom c2", "Prénom c2", "Voix c2", "% Voix/Ins c2", "% Voix/Exp c2","N°Panneau c3", "Sexe c3", "Nom c3", "Prénom c3", "Voix c3", "% Voix/Ins c3", "% Voix/Exp c3","N°Panneau c4", "Sexe c4", "Nom c4", "Prénom c4", "Voix c4", "% Voix/Ins c4", "% Voix/Exp c4","N°Panneau c5", "Sexe c5", "Nom c5", "Prénom c5", "Voix c5", "% Voix/Ins c5", "% Voix/Exp c5","N°Panneau c6", "Sexe c6", "Nom c6", "Prénom c6", "Voix c6", "% Voix/Ins c6", "% Voix/Exp c6","N°Panneau c7", "Sexe c7", "Nom c7", "Prénom c7", "Voix c7", "% Voix/Ins c7", "% Voix/Exp c7","N°Panneau c8", "Sexe c8", "Nom c8", "Prénom c8", "Voix c8", "% Voix/Ins c8", "% Voix/Exp c8","N°Panneau _c9", "Sexe _c9", "Nom _c9", "Prénom _c9", "Voix _c9", "% Voix/Ins _c9", "% Voix/Exp _c9","N°Panneau _c10", "Sexe _c10", "Nom _c10", "Prénom _c10", "Voix _c10", "% Voix/Ins _c10", "% Voix/Exp _c10","N°Panneau _c11", "Sexe _c11", "Nom _c11", "Prénom _c11", "Voix _c11", "% Voix/Ins _c11", "% Voix/Exp _c11","N°Panneau _c12", "Sexe _c12", "Nom _c12", "Prénom _c12", "Voix _c12", "% Voix/Ins _c12", "% Voix/Exp _c12"))
print(df.columns)
#df.show()

#Delete duplicated rows
df.dropDuplicates()

#Delete null values
df.dropna()

#Delete rows with the value Unnamed
#df.where(df['Sexe c1'] != "Unnamed").show()