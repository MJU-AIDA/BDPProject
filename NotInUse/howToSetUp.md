# 데이터 전처리를 위한 설정

## 1. Jvm이 필요하므로, 버전에 맞는 자바 다운로드

sudo yum install gcc-c++ java-1.8.0-openjdk-devel python3 python3-devel python3-pip make diffutils

## 2. Konlpy 설치

/bin/python3.6 install konlpy

## 3. 한글 데이터를 다뤄야하므로 encoding 수정

-run.sh 파일을 다음과 같이 설정

```jsx
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
export PYSPARK_PYTHON='/bin/python3.6'
export PYTHONIOENCODING=utf8
spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/bin/python3.6 \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/bin/python3.6 \
[test.py](http://test.py/)
```

## 4. mallet module 다운로드

$ wget https://[mallet.cs.umass.edu/dist/mallet-2.0.8.zip](http://mallet.cs.umass.edu/dist/mallet-2.0.8.zip)

(다운로드한 폴더 경로를 잘 기억)