from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from konlpy.tag import Okt
from pyspark.sql.types import StructField, StructType, StringType, LongType, Row

if __name__ =="__main__" :

	myManualSchema = StructType([
		StructField("news_id", StringType(), True),
		StructField("title", StringType(), True),
		StructField("timestamp", StringType(), True),
		StructField("content", StringType(), True),
	])

	spark = SparkSession.builder.appName("test").getOrCreate()
	df_news = spark.read.schema(myManualSchema).load("hdfs:///user/maria_dev/dataset.csv",\
			format="csv", sep = ",", inferSchema = "true", header = "true")	
	df_news = df_news.filter("content is not null")
	df = df_news\
			.rdd\
			.map(lambda row: Row(news_id=row.news_id, title=row.title, timestamp = row.timestamp, content=Okt().nouns(row.content)))\
			.toDF()
	df.show()

	



	
#	a = df.select(
#			udf_token(F.col('c')).alias('new_c'),
#			F.col('a')
#	)
	#df.withColumn('new', F.udf(lambda x : okt.nouns(x))( "본문")).show()