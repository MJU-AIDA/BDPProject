from pyspark.sql.functions import *
from pyspark.sql.types import Row
from konlpy.tag import Okt

okt = Okt()

conf = SparkConf().setAppName("test")
sc = SparkContext(conf = conf)
