from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.evaluation import ClusteringEvaluator

if __name__ =="__main__" :
    
    # 스키마 정의
    myManualSchema = StructType([
        StructField("index", LongType(), True),
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
    
    df = df_news.withColumn('content', regexp_replace('content', ',', ' ')).where(" index % 10 == 1")



    stopwords_list = []
    
    
    # train val test split = 6 : 2 : 2
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df, val_df = train_df.randomSplit([0.75, 0.25], seed=42)
    
    
    # 문서별 주제의 관여정도
    # 토픽 별 연관된 단어
    # (문서에 등장하는) 단어는 어느 토픽으로부터 할당(뽑혔나)되었나 
    
    # make_stopwords.py에서 생성한 불용어 리스트를 가져와 불용어 사전에 등록
    with open("stopwords.txt", "r") as f:
        k = f.readline()
    stopwords_list = k.replace('\'', '').split(",")

    # 자연어 전처리를 위한 파이프라인 구축
    tokenizer = Tokenizer(inputCol='content',outputCol='mytokens')
    vectorizer = CountVectorizer(inputCol='newtokens',outputCol='vectors',vocabSize= 2000) # ,vocabSize= 2000
    idf = IDF(inputCol="vectors", outputCol="features")
    stopwordsremover = StopWordsRemover(inputCol='mytokens',outputCol='newtokens', stopWords= stopwords_list)
    
    pre_pipeline = Pipeline(
        stages = [tokenizer, stopwordsremover, vectorizer, idf]
    )
    
    # 파이프라인에 따라 데이터가공
    vec_result = pre_pipeline.fit(train_df)
    vectorized_data = vec_result.transform(train_df)

    # LDA 모델 생성 및 학습
    lda = LDA(k=30, seed = 1, optimizer = 'em').setMaxIter(10)
    
    #####     LDA모델 하이퍼파라미터 바꾸기     #####
    
    model = lda.fit(vectorized_data) # 학습된 lda 모델

    ll = model.logLikelihood(vectorized_data)
    lp = model.logPerplexity(vectorized_data)
    print("The lower bound on the log likelihood of the entire corpus: " + str(ll))
    print("The upper bound on perplexity: " + str(lp))
    
    
    # Word_index화된 단어를  원래 의미를 가지는 단어로 바꾸기 위해 복원 사전 생성
    cv  = CountVectorizer(inputCol="newtokens" ,vocabSize= 2000)
    cv_model = cv.fit(vectorized_data)
    
    ## vectorized_tokens = cv_model.transform(vectorized_data) # (총 단어수, [기사 내 존재하는 단어], [단어 빈도수])
    vocab = cv_model.vocabulary
    topics = model.describeTopics()
    topics_rdd = topics.rdd
    
    
    
    
    # 토픽별 키워드 수집
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


        