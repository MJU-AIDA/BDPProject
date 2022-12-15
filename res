+-----+-----------------+----------+--------------------+--------------------+
|index|          news_id| timestamp|               title|             content|
+-----+-----------------+----------+--------------------+--------------------+
|    0|2100201.201801012|2018-01-01|여승객 하차요구 무시해 감금 기...|무시 여승객 하차 요구 감금 기...|
|    1|2100311.201801011|2018-01-01|美 무차별 특허공세...'한국 ...|특허공세 특허 공세 한국 반도체...|
|    2|2100311.201801011|2018-01-01|"이채원 한국밸류운용 신임 대표...|금리인상 이채원 한국밸류운용 신...|
|    3|1100901.201801012|2018-01-01|준희양 친아버지 “딸 심하게 때렸다”|준희양 친아버지 고준희 친아버지...|
|    4|1100901.201801012|2018-01-01|[이하경 칼럼] 보세요. 결국엔...|결국 검사 의사 기득권 학생 시...|
|    5|1100901.201801011|2018-01-01|[논설위원이 간다] ‘가즈아~’...|가즈아 존버 비트코 체험기 이름...|
|    6|1100901.201801012|2018-01-01|[전문] 北 김정은 2018년 신년사|김정은 신년사 김정은 북한 노동...|
|    7|1100901.201801012|2018-01-01|[단독] 광주 아파트서 화재 3...|광주 아파트 사망 화재 긴급체포...|
|    8|1100901.201801012|2018-01-01|제천 화재 경찰 수사 '제천시청...|제천시청 제천 화재 경찰 수사 ...|
|    9|1100901.201801012|2018-01-01|[이슈추적] 방화냐? 실화냐? ...|방화 실화 광주 사망 화재 경찰...|
|   10|1100901.201801011|2018-01-01|3%성장 기대  단기성적 취해 ...|3%성장 단기성적 개혁 지연 6...|
|   11|1100901.201801012|2018-01-01|마약 투여 혐의로 체포된 저가항...|혐의 마약 투여 체포 저가항공사...|
|   12|1100901.201801012|2018-01-01|“치료 도와줄게” 친구 딸 상습...|치료 친구 친구 상습 성추행 항...|
|   13|1100901.201801012|2018-01-01|82년 임술년 검사 생활 시작 ...|시작 임술년 검사 생활 원숭이띠...|
|   14|1100901.201801011|2018-01-01|[J report] “문제는 배...|배터리 아이폰 혁신 신뢰 배터리...|
|   15|1100901.201801012|2018-01-01|  성범죄자 취업 최대 10년간 금지|취업 최대 7월 아동 청소년 헌...|
|   16|1100901.201801012|2018-01-01|아동 납치, 성폭행 동반 살인하...|아동 납치 살인 성폭행 동반 구...|
|   17|1100901.201801012|2018-01-01|미성년자 납치 강간살해 구형 대...|납치 강간살해 구형 최대 사형 ...|
|   18|2100311.201801011|2018-01-01|새해에도 법원 檢 행보 살피는 기업들|새해 법원 행보 기업들 이건희 ...|
|   19|8100201.201801011|2018-01-01|광주 3남매 사망 화재사건 방화...|수사 광주 사망 화재 사건 방화...|
+-----+-----------------+----------+--------------------+--------------------+
only showing top 20 rows

Traceback (most recent call last):
  File "/home/maria_dev/project/BDP_final_project/topic_modeling.py", line 78, in <module>
    cv_model = cv.fit(vectorized_data)
  File "/usr/hdp/current/spark2-client/python/lib/pyspark.zip/pyspark/ml/base.py", line 132, in fit
  File "/usr/hdp/current/spark2-client/python/lib/pyspark.zip/pyspark/ml/wrapper.py", line 288, in _fit
  File "/usr/hdp/current/spark2-client/python/lib/pyspark.zip/pyspark/ml/wrapper.py", line 285, in _fit_java
  File "/usr/hdp/current/spark2-client/python/lib/py4j-0.10.7-src.zip/py4j/java_gateway.py", line 1257, in __call__
  File "/usr/hdp/current/spark2-client/python/lib/pyspark.zip/pyspark/sql/utils.py", line 63, in deco
  File "/usr/hdp/current/spark2-client/python/lib/py4j-0.10.7-src.zip/py4j/protocol.py", line 328, in get_return_value
py4j.protocol.Py4JJavaError: An error occurred while calling o214.fit.
: org.apache.spark.SparkException: Job 7 cancelled because SparkContext was shut down
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$cleanUpAfterSchedulerStop$1.apply(DAGScheduler.scala:837)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$cleanUpAfterSchedulerStop$1.apply(DAGScheduler.scala:835)
	at scala.collection.mutable.HashSet.foreach(HashSet.scala:78)
	at org.apache.spark.scheduler.DAGScheduler.cleanUpAfterSchedulerStop(DAGScheduler.scala:835)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onStop(DAGScheduler.scala:1848)
	at org.apache.spark.util.EventLoop.stop(EventLoop.scala:83)
	at org.apache.spark.scheduler.DAGScheduler.stop(DAGScheduler.scala:1761)
	at org.apache.spark.SparkContext$$anonfun$stop$8.apply$mcV$sp(SparkContext.scala:1931)
	at org.apache.spark.util.Utils$.tryLogNonFatalError(Utils.scala:1361)
	at org.apache.spark.SparkContext.stop(SparkContext.scala:1930)
	at org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend$MonitorThread.run(YarnClientSchedulerBackend.scala:112)
	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:642)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2034)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2131)
	at org.apache.spark.rdd.RDD$$anonfun$fold$1.apply(RDD.scala:1092)
	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
	at org.apache.spark.rdd.RDD.withScope(RDD.scala:363)
	at org.apache.spark.rdd.RDD.fold(RDD.scala:1086)
	at org.apache.spark.mllib.clustering.EMLDAOptimizer.computeGlobalTopicTotals(LDAOptimizer.scala:230)
	at org.apache.spark.mllib.clustering.EMLDAOptimizer.next(LDAOptimizer.scala:217)
	at org.apache.spark.mllib.clustering.EMLDAOptimizer.next(LDAOptimizer.scala:81)
	at org.apache.spark.mllib.clustering.LDA.run(LDA.scala:336)
	at org.apache.spark.ml.clustering.LDA.fit(LDA.scala:912)
	at org.apache.spark.ml.clustering.LDA.fit(LDA.scala:814)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
	at py4j.Gateway.invoke(Gateway.java:282)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.GatewayConnection.run(GatewayConnection.java:238)
	at java.lang.Thread.run(Thread.java:748)

