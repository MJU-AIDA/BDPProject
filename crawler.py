import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import csv
import re


def pop():
    # 맨 위 뉴스 팝업
    time.sleep(2)
    first = browser.find_element(By.CSS_SELECTOR, '#news-results > div:nth-child(1) > div > div.cont > a > div > strong > span')
    time.sleep(2)
    first.click()

def gettitle(tmp):
    news_title = browser.find_element(By.CSS_SELECTOR, '#news-detail-modal > div > div > div.modal-body > div > div.news-view-head > h1.title')
    print('newsTitle:', news_title.text)
    tmp["newsTitle"].append(news_title.text)

def getbody(tmp):
    # 날짜 저장
    news_date = browser.find_elements(By.XPATH, '//*[@id="news-detail-modal"]/div/div/div[1]/div/div[1]/div[1]/ul/li[1]')
    for value in news_date:
        print('newsDate:',value.text)
        tmp["newsDate"].append(value.text)
    # 하나의 뉴스기사 기준 본문 저장
    news_bodies = browser.find_elements(By.XPATH, '//*[@id="news-detail-modal"]/div/div/div[1]/div/div[2]')
    news_body =''
    for i in news_bodies:
        news_body += i.text
    print('newsBody:',news_body[:50]," ...중략...")
    tmp["newsBody"].append(news_body)

def crawl(newsIdFrom, fromdate, todate):
    # 날짜 구간 설정
    browser.find_element(By.XPATH, '//*[@id="collapse-step-1"]').click()
    browser.find_element(By.XPATH, '//*[@id="collapse-step-1-body"]/div[3]/div/div[1]/div[1]/a').click()
    time.sleep(2)

    from_ = browser.find_element(By.XPATH, '//*[@id="search-begin-date"]')
    browser.execute_script("arguments[0].value = {};".format(fromdate), from_)
    to_ = browser.find_element(By.XPATH, '//*[@id="search-end-date"]')
    browser.execute_script("arguments[0].value = {};".format(todate), to_)


    # 사건사고 분류 카테고리 - 범죄 선택
    browser.find_element(By.XPATH, '//*[@id="collapse-step-1-body"]/div[3]/div/div[2]/div[3]/a').click()
    browser.find_element(By.XPATH,'//*[@id="srch-tab4"]/ul/li[1]/div/span[4]').click()
    time.sleep(2)
    browser.find_element(By.XPATH,'//*[@id="search-foot-div"]/div[2]/button[2]').click()
    time.sleep(2)

    # 빈 데이터셋 생성
    tmp = {"newsId":[],"newsTitle":[],"newsDate":[],"newsBody":[]}

    #기간 내 기사를 크롤링
    newsId = 0
    while (newsId < 15000):
        try:
            print('newsId:',newsIdFrom)
            tmp["newsId"].append(newsIdFrom)
            time.sleep(2)
            if (newsId % 10 == 0):
                pop()
            time.sleep(1)
            gettitle(tmp)
            getbody(tmp)
            # 다음 뉴스 글로 넘기기
            if (newsId == 0):
                browser.find_element(By.CSS_SELECTOR,
                    '#news-detail-modal > div > div > div.modal-body > div > div.list_prev_next > ul > li:nth-child(2) > dd > a > span').click()
            elif (newsId % 10 == 9 or newsId % 10 == 0 ):
                next_news = browser.find_element(By.CSS_SELECTOR, 
                    '#news-detail-modal > div > div > div.modal-body > div > div.list_prev_next > ul > li:nth-child(3) > dd > a > span')
                time.sleep(1)
                next_news.click()
            else:
                next_news = browser.find_element(By.CSS_SELECTOR,
                    '#news-detail-modal > div > div > div.modal-body > div > div.list_prev_next > ul > li.nextNewsItem > dd > a > span')
                time.sleep(1)
                next_news.click()
            newsId += 1
            newsIdFrom += 1
            # save to csv
            df = pd.DataFrame(tmp)
            filename = "dataset_"+fromdate+"_"+todate+".csv"
            if not os.path.exists(filename):
                df.to_csv(filename, index=False, mode="w", encoding="utf-8-sig")
            else:
                df.to_csv(filename, index=False, mode="a", encoding="utf-8-sig", header=False)
        except:
            newsId += 1
            newsIdFrom += 1
        finally:
            if (newsId > 2 and tmp["newsTitle"][-1] == tmp["newsTitle"][-2] == tmp["newsTitle"][-3]):
                break
    # 창 닫기
    time.sleep(2)
    browser.find_element(By.XPATH, '//*[@id="news-detail-modal"]/div/div/button').click()
    time.sleep(1)
    
    print("-"*40,"Complete from {} to {}".format(fromdate, todate),"-"*40)
    return newsId


# selenium
browser = webdriver.Chrome()

browser.set_window_size(1200,800)
url = "https://www.bigkinds.or.kr/v2/news/search.do"

browser.get(url)
time.sleep(2)

# 기간 설정
fromdate = "\'2018-01-01\'"
todate ="\'2018-03-31\'"

fromyear = int(fromdate[1:5])
toyear = int(todate[1:5])
frommonth = int(fromdate[6:8])
tomonth = int(todate[6:8])
day31 = [1,3,5,7,8,10,12]; day30 = [4,6,9,11]; day28 = [2]

newsId = 0
for i in range(fromyear ,toyear+1):
    for j in range(frommonth,tomonth+1):
        if j in day28:
            split_fromdate = "\'{}-{}-{}\'".format(i, str(j).zfill(2), '01')
            split_todate = "\'{}-{}-{}\'".format(i, str(j).zfill(2), '28')
            print("-"*40,split_fromdate, "부터", split_todate, "까지","-"*40)
            newsId += crawl(newsId, split_fromdate, split_todate)
        elif j in day30:
            split_fromdate = "\'{}-{}-{}\'".format(i, str(j).zfill(2), '01')
            split_todate = "\'{}-{}-{}\'".format(i, str(j).zfill(2), '30')
            print("-"*40,split_fromdate, "부터", split_todate, "까지","-"*40)
            newsId += crawl(newsId, split_fromdate, split_todate)
        elif j in day31:
            split_fromdate = "\'{}-{}-{}\'".format(i, str(j).zfill(2), '01')
            split_todate = "\'{}-{}-{}\'".format(i, str(j).zfill(2), '31')
            print("-"*40,split_fromdate, "부터", split_todate, "까지","-"*40)
            newsId += crawl(newsId, split_fromdate, split_todate)


print("-"*40,"Complete to acquisite.","-"*40)