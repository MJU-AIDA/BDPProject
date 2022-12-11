# -*- coding: utf-8 -*-
from konlpy.tag import Okt

def clean_text(text):
    """
    한글, 영문, 숫자만 남기고 제거한다.
    :param text:
    :return:
    """
    text = text.replace(".", " ").strip()
    text = text.replace("·", " ").strip()
    pattern = '[^ ㄱ-ㅣ가-힣|0-9|a-zA-Z]+'
    text = re.sub(pattern=pattern, repl='', string=text)
    return text



okt = Okt()

data_word = okt.nouns('김밥 말이 김밥말이 할아버지가 방에 들어가신다')


id2word = corpora.Dictionary(data_word)



print(okt.nouns('김밥 말이 김밥말이 할아버지가 방에 들어가신다'))


