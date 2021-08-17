import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import split,percentile_approx,lit,row_number,desc,collect_list,concat_ws,trim
from pyspark.sql import Window


parser = argparse.ArgumentParser()
parser.add_argument("--crimes_path", help="Crimes CSV file path", default="./data/crime.csv")
parser.add_argument("--codes_path", help="Codes CSV file path",  default="./data/offense_codes.csv")
parser.add_argument("--output_path", help="Output parquet file path",  default="./boston_crimes.parquet")

args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

# Crimes dataframe
df = spark.read.load(args.crimes_path,format="csv", sep=",", inferSchema="true", header="true")

# Codes dataframe
df_codes = spark.read.load(args.codes_path,format="csv", sep=",", inferSchema="true", header="true")

# Remove duplicates from crime codes
df_codes = df_codes.dropDuplicates(subset=["CODE"])

# Create 'crime_type' column
df_codes = df_codes.withColumn('crime_type', trim(split(df_codes.NAME,'-').getItem(0)))

# Remove duplicates from crimes (incident numbers)
df = df.dropDuplicates(subset=["INCIDENT_NUMBER"])

df = df.join(df_codes,df.OFFENSE_CODE == df_codes.CODE)

crimes_total = df.groupby("DISTRICT").count().withColumnRenamed("count", "crimes_total")
crimes_monthly_total = df.groupby("DISTRICT", "YEAR", "MONTH").count().withColumnRenamed("count", "crimes_monthly_total")
crimes_monthly = crimes_monthly_total.groupby("DISTRICT").agg(percentile_approx(crimes_monthly_total["crimes_monthly_total"],
                                                                                0.5).alias("crimes_monthly"))
frequent_crime_types = df.groupby("DISTRICT", "crime_type").count().withColumn("row", row_number().over(
    Window.partitionBy("DISTRICT").orderBy(desc("count")))).where("row <= 3").drop("row").groupBy("DISTRICT").agg(concat_ws(', ',collect_list("crime_type")).alias("frequent_crime_types"))

geo_avg = df.groupBy("DISTRICT").mean("Lat", "Long").withColumnRenamed("avg(Lat)","lat").withColumnRenamed("avg(Long)","lng")

# Resulting dataframe
boston_crimes = crimes_total.join(crimes_monthly,"DISTRICT").join(frequent_crime_types, "DISTRICT").join(geo_avg, "DISTRICT")

boston_crimes.write.parquet(args.output_path)