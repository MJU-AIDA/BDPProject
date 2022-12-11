from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.sql import Row
from pyspark.sql.functions import regexp_replace
from pyspark.ml.linalg import Vectors, SparseVector
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer

from konlpy.tag import Okt

from pyspark.sql.types import StructField, StructType, StringType, LongType, Row


if __name__ =="__main__" : 
    
    myManualSchema = StructType([
		StructField("news_id", StringType(), True),
        StructField("timestamp", StringType(), True),
		StructField("title", StringType(), True),
		StructField("content", StringType(), True),
	])
    spark = SparkSession.builder.appName("temp").getOrCreate()
    df_news = spark.read.schema(myManualSchema).load("hdfs:///user/maria_dev/News_20221021-20221126.csv",\
		format="csv", sep = ",", inferSchema = "true", header = "true")	
    df_news = df_news.filter("content is not null")
    df = df_news
    
    # df = df_news.withColumn('content', regexp_replace('content', ',', ' '))
    
    
    #df = df_news\
	# 		.rdd\
    #        .map(lambda row: Row(news_id=row.news_id, timestamp = row.timestamp, title=row.title ,content=Okt().nouns(row.content)))\
    #        .toDF()
    
    df.show()
    tokenizer = Tokenizer(inputCol='content',outputCol='mytokens')
    stopwordsremover = StopWordsRemover(inputCol='content',outputCol='newtokens')
    vectorizer = CountVectorizer(inputCol='newtokens',outputCol='features',vocabSize= 2000)

    pipeline = Pipeline(
        stages = [tokenizer, stopwordsremover, vectorizer] # 
    )
    vectorized_data = pipeline.fit(df).transform(df)

    lda = LDA(k=7, seed = 1, optimizer = 'em').setMaxIter(10)    

    model = lda.fit(vectorized_data) # 학습된 lda 모델
    
    # 문서별 주제의 관여정도
    # 토픽 별 연관된 단어
    # (문서에 등장하는) 단어는 어느 토픽으로부터 할당(뽑혔나)되었나 
    
    cv  = CountVectorizer(inputCol="newtokens", outputCol="features2")
    cv_model = cv.fit(vectorized_data)
    vectorized_tokens = cv_model.transform(vectorized_data) # (총 단어수, [기사 내 존재하는 단어],[단어 빈도수])

    vocab = cv_model.vocabulary
    topics = model.describeTopics()
    topics_rdd = topics.rdd

    topics_words = topics_rdd\
       .map(lambda row: row['termIndices'])\
       .map(lambda idx_list: [vocab[idx] for idx in idx_list])\
       .collect()

    for idx, topic in enumerate(topics_words):
        print("topic: {}".format(idx))
        print("*"*25)
        for word in topic:
            print(word)
        print("*"*25)






    