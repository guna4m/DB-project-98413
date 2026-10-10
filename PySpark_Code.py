from pyspark import sparksession
spark=sparksession.builder.appName("MyApp").getOrCreate()

columns = ["name", "age", "city"]
data = [("Alice", 30, "New York"), ("Bob", 25, "Los Angeles"), ("Charlie", 35, "Chicago")]
df = spark.createDataFrame(data, columns)
df.show()
s3_path = "s3://your-bucket-name/your-folder-name/"
df.write.parquet(s3_path, mode="overwrite")
df.write.format("delta").mode("overwrite").save(s3_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS my_table USING DELTA LOCATION '{s3_path}'")

df.write.format("delta").mode("overwrite").option("path", s3_path).saveAsTable("my_table")


df.write.format("delta").mode("overwrite").saveAsTable("my_table")
