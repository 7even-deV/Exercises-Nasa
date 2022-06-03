from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import re

spark = (SparkSession.builder
            .appName("Nasa")
            .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")


_route = "./resources/"
_file = "access.log"
nasa_file = _route + _file

df = (spark.read.format("text")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(nasa_file))
df.show()


sample_logs = [item['value'] for item in df.collect()]
sample_logs


host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
hosts = [re.search(host_pattern, item).group(1)
            if re.search(host_pattern, item)
            else 'no match'
            for item in sample_logs]
