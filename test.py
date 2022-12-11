from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from konlpy.tag import Okt
from pyspark.sql.types import StructField, StructType, StringType, LongType, Row

if __name__ =="__main__" :

	myManualSchema = StructType([
		StructField("a", StringType(), True),
		StructField("b", StringType(), True),
		StructField("c", StringType(), True),
		StructField("d", StringType(), True),
		StructField("e", StringType(), True),
		StructField("f", StringType(), True),
		StructField("g", StringType(), True),
		StructField("h", StringType(), True),
		StructField("i", StringType(), True)
		#StructField("g", StringType(), True),
		#StructField("k", StringType(), True),
		#StructField("l", StringType(), True),
		#StructField("m", StringType(), True),
		#StructField("n", StringType(), True),
		#StructField("o", StringType(), True),
		#StructField("p", StringType(), True),
		#StructField("q", StringType(), True),
		#StructField("r", StringType(), True)
	])

	spark = SparkSession.builder.appName("test").getOrCreate()
	df_news = spark.read.schema(myManualSchema).load("hdfs:///user/maria_dev/NewsResult_20220826-20221126.csv",\
			format="csv", sep = ",", inferSchema = "true", header = "true", charset = "euc-kr")	
	df_news = df_news.filter("c is not null")
	df = df_news\
			.rdd\
			.map(lambda row: Row(title=row.a, content=row.b, text=Okt().nouns(row.c)))\
			.toDF()


	okt = Okt()
	def tokenizer(x) :
		return okt.nouns(x)

	udf_token = F.udf(tokenizer)
	
	a = df.select(
			udf_token(F.col('c')).alias('new_c'),
			F.col('a')
	)
	#df.withColumn('new', F.udf(lambda x : okt.nouns(x))( "본문")).show()
	a.show()

	
