from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

spark = SparkSession.builder.appName("HDFS").getOrCreate()
sparkcont = SparkContext.getOrCreate(SparkConf().setAppName("HDFS"))
logs = sparkcont.setLogLevel("ERROR")

from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType,DateType

data2 = [("James", "", "Smith", "36636", "M", 3000),
         ("Michael", "Rose", "", "40288", "M", 4000),
         ("Robert", "", "Williams", "42114", "M", 4000),
         ("Maria", "Anne", "Jones", "39192", "F", 4000),
         ("Jen", "Mary", "Brown", "", "F", -1)
         ]



schema = StructType() \
    .add("Stock Price", DoubleType()) \
    .add("OPEN PRICE", DoubleType()) \
    .add("HIGH ", DoubleType()) \
    .add("LOW", DoubleType()) \
    .add("RANK", DoubleType()) \
    .add("PE_RATIO", DoubleType()) \
    .add("EPS", DoubleType()) \
    .add("MCAP", DoubleType()) \
    .add("SECTOR MCAP RANK", StringType()) \
    .add("PB RATIO", DoubleType()) \
    .add("Div Yield percentage", StringType()) \
    .add("FACE VALUE", StringType()) \
    .add("BETA", StringType()) \
    .add("VWAP", StringType()) \
    .add("Overall Recommendation", StringType()) \
    .add("Strong Buy", StringType()) \
    .add("Buy", StringType()) \
    .add("Hold", StringType()) \
    .add("Sell", StringType()) \
    .add("Date", StringType()) \
    .add("Time", StringType())
df = spark.createDataFrame(data=data2, schema=schema)
df.printSchema()
df.show(truncate=False)
hdfc_write_path = "hdfs://localhost:9000/user/sunbeam/busstops/input"
df.coalesce(1).write.option("header",True)\
    .parquet(hdfc_write_path, mode='overwrite')