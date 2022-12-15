from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF


if __name__ =="__main__" :
    
    # 스키마 정의
    myManualSchema = StructType([
        StructField("index", StringType(), True),
		StructField("news_id", StringType(), True),
        StructField("timestamp", StringType(), True),
		StructField("title", StringType(), True),
		StructField("content", StringType(), True),
	])
    
    # 스파크 데이터프레임에 데이터셋 가져오기
    spark = SparkSession.builder.appName("temp").getOrCreate()
    df_news = spark.read.schema(myManualSchema).load("hdfs:///user/maria_dev/data_2018.csv",\
		format="csv", sep = ",", inferSchema = "true", header = "true")	
    df_news = df_news.filter("content is not null")
    
    df = df_news.withColumn('content', regexp_replace('content', ',', ' '))
    
    stopwords_list = []
    
    # 불용어 구축을 위한 학습
    tokenizer = Tokenizer(inputCol='content',outputCol='mytokens')
    stopwordsremover = StopWordsRemover(inputCol='mytokens',outputCol='newtokens')
    vectorizer = CountVectorizer(inputCol='newtokens',outputCol='vectors',vocabSize= 2000) # ,vocabSize= 2000
    idf = IDF(inputCol="vectors", outputCol="features")
    
    pipeline = Pipeline(
        stages = [tokenizer, stopwordsremover, vectorizer]
    )
    vec_result = pipeline.fit(df)
    vectorized_data = vec_result.transform(df)

    
    # vectorized된 단어 빈도수를 바탕으로 너무 자주 등장하거나, 한 음절 단어인 불용어 사전 생성
    temp = CountVectorizer(inputCol="newtokens",vocabSize= 2000) 
    temp_model = temp.fit(vectorized_data)
    
    #불용어 사전 생성
    top10 = list(temp_model.vocabulary[0:10])
    less_then_2_character = [word for word in temp_model.vocabulary if len(word) <= 1]
    stopwords_list = top10 + less_then_2_character

    print(stopwords_list)
