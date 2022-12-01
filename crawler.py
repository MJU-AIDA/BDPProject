import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from pandas import DataFrame
from datetime import datetime
import time
import csv

def pop():
    # 맨 위 뉴스 팝업
    first = browser.find_element(By.CSS_SELECTOR, '#news-results > div:nth-child(1) > div > div.cont > a > div > strong > span')
    time.sleep(2)
    first.click()

def gettitle():
    news_title = browser.find_element(By.CSS_SELECTOR, '#news-detail-modal > div > div > div.modal-body > div > div.news-view-head > h1.title')
    print('newstitle:', news_title.text)

def getbody():
    # 날짜 저장
    news_date = browser.find_elements(By.XPATH, '//*[@id="news-detail-modal"]/div/div/div[1]/div/div[1]/div[1]/ul/li[1]')
    for value in news_date:
        print('newsdate:',value.text)
    # 하나의 뉴스기사 기준 본문 저장
    news_bodies = browser.find_elements(By.XPATH, '//*[@id="news-detail-modal"]/div/div/div[1]/div/div[2]')
    news_body =''
    for i in news_bodies:
        news_body += i.text
    print('newsbody:',news_body)


# selenium
browser = webdriver.Chrome()

browser.set_window_size(1000,800)
url = "https://www.bigkinds.or.kr/v2/news/search.do"

browser.get(url)
time.sleep(0.2)

# 카테고리 - 범죄 선택
browser.find_element(By.XPATH, '//*[@id="collapse-step-2-body"]/div/div[1]/ul[1]/li[4]/a').click()

browser.find_element(By.XPATH,'//*[@id="filterTab04"]/li[1]/span').click()
time.sleep(2)


newsId = 0
iteration = 10**2

while (newsId < iteration):
    print('newsId:',newsId)
    if (newsId % 10 == 0):
        time.sleep(2)
        pop()
    time.sleep(2)
    gettitle()
    getbody()
    if (newsId == 0):
        browser.find_element(By.CSS_SELECTOR,
            '#news-detail-modal > div > div > div.modal-body > div > div.list_prev_next > ul > li:nth-child(2) > dd > a > span').click()
    elif (newsId % 10 == 9 or newsId % 10 == 0 ):
        next_news = browser.find_element(By.CSS_SELECTOR, 
            '#news-detail-modal > div > div > div.modal-body > div > div.list_prev_next > ul > li:nth-child(3) > dd > a > span')
        time.sleep(2)
        next_news.click()
    else:
        next_news = browser.find_element(By.CSS_SELECTOR,
            '#news-detail-modal > div > div > div.modal-body > div > div.list_prev_next > ul > li.nextNewsItem > dd > a > span')
        time.sleep(2)
        next_news.click()
    newsId += 1
        
        
        
    
