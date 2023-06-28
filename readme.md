## 빅데이터프로그래밍 팀프로젝트 - 스파크 환경에서 뉴스기사 LDA 토픽 모델링


### 파일 및 디렉토리 정의
 

        
    [폴더]
        - DataCollection : 데이터를 수집하기 위한 크롤링 코드와 API 요청을 통해 받은 data를 csv 형태로 바꿔주기 위한 코드로 구성되어 있다.
            * bdp_data.ipynb : API 요청을 통해 받은 excel 파일을 csv 파일 형태로 저장
            * bigkinds_crawling_1.ipynb :  빅카인즈 사이트 크롤링을 위한 코드
            * crawler_221210.ipynb  : 빅카인즈 사이트 크롤링을 위한 코드, 크롤링 코드가 두 개인 이유는 빠른 데이터 수집을 위해 성능이 더 나은 코드를 선택하기 위해 두 가지 코드를 작성하였다.

        - NotInUse
            * flickr_scarapping.ipynb : 초기 프로젝트, '사진 exif 데이터를 이용한 여행지 추천시스템'을 위해 exif 데이터를 수집하기 위해 작성한 코드, 수집 결과 대부분의 사진에는 exif 값이                 누락되어 있어 프로젝트 주제를 변경하였다.
            * howToSetUp : konlpy, gensim 등을 하둡에서 사용하기 위한 환경설정 방법등이 담겨 있다. konlpy 대신 CountVectorizer를 사용하기에 제외하였다.
            * konlpy를 사용해 spark 환경에서 전처리한 코드
            * test_konly.py 정상적으로 konlpy 라이브러리를 사용할 수 있는지 확인하기 위한 코드

        - result : 연도별 출력 결과물, 하이퍼 파라미터 조정과, 불용어 사전 추가 등의 성능 개선을 위한 노력 이후의 출력결과물을 보여준다.
            

    [파일]
        - readmd.md : 파일과 디렉토리에 대한 설명을 담고 있다.
        - run.sh : topic_modeling.py를 실행하기 위한 python 버전, hdfs 정보를 가지고 있다.
        - topic_modeling.py : LDA 학습을 위한 데이터 LOAD, train/test set 분리, 파이프라인 구축 및 각 객체 선언, 결과 출력을 위한 코드
        - make_stopwords.py : 불용어 사전을 만들기 위한 코드, countervectorizer로부터 최다 빈도 단어 10개, 한 글자 짜리 단어 등 제외
        - stopwords.txt : make_stopwords.py일서 생성한 불용어 사전을 .txt 형태로 저장한 것 
        - res : 결과 임시 출력을 위한 파일


