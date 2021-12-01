# %%

from datetime import datetime
from kafka.metrics import measurable
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from kafka import KafkaProducer
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import time
from konlpy.tag import Okt

BROKER_LIST = "13.125.8.247:9092"
CONSUMER_TOPIC = "urls"
PRODUCER_TOPIC = "crawledResults"

sc = SparkContext(appName='crawl')
ssc = StreamingContext(sc, 2)
sc.setLogLevel("WARN")
urls = KafkaUtils.createDirectStream(ssc, topics=[CONSUMER_TOPIC],
                                     kafkaParams={"metadata.broker.list": BROKER_LIST})


def text_preprocessing(text, tokenizer):
    stopwords = ['은', '는', '이', '가', '을', '를', '중', '등']
    txt = re.sub('/^[가-힣a-zA-Z]+$/', '', text)
    clean_token = []
    for token in tokenizer.pos(txt, stem=True):
        if token[1] in ['Noun', 'Alpha', 'Number']:
            if token[0] not in stopwords:
                clean_token.append(token[0])
    return clean_token


def get_text(tag):
    return re.sub(r'[^\w]+', ' ', tag.get_text())


def get_contents(t):

    try:
        key = t[0]
        html = urlopen(t[1])

        soup = BeautifulSoup(html, 'html.parser')

        #head = soup.find_all(['h1', 'h2', 'h3'])
        content = soup.find_all('p')

        #head = ' '.join(map(get_text, head))
        content = ' '.join(map(get_text, content))
        # map을 통하면 걸러져서 list로 나옴.
        # ' '.join(단어 리스트)
        # '단어1 단어2 단어3 단어4'
        tokenizer = Okt()
        content = text_preprocessing(content, tokenizer)
        content = ' '.join(content)+key
        # 한국어 형태로 분리. output = '단어1 단어2 단어3 단어4'

        return key, content

    except:
        pass


def func1(t):
    key = t[0]
    values = t[1].split(" ")
    return [(key, value) for value in values]


def func2(t):
    key = t[0]
    url = t[1]
    return True if 'http' in url else False


# url을 분리시켜줌. key = 입력한 단어, value = [url 목록]
urls = urls.flatMap(lambda x: func1(x))
# http인지 확인하고 거름 -> key = 입력한 단어, value = [url 목록]
urls = urls.filter(lambda x: func2(x))
# t[0] key, t[1] value -> key = 입력한 단어, value = [각 url 단어 목록 string]
contents = urls.map(lambda url: get_contents(url))

producer = KafkaProducer(bootstrap_servers=BROKER_LIST,
                         key_serializer=str.encode, value_serializer=str.encode)


def push_to_topics(data, topic=PRODUCER_TOPIC):

    data = data.collect()
    if not data:
        return

    else:
        for t in data:
            producer.send(topic, key=t[0], value=t[1])
            time.sleep(3)
            producer.flush()


contents.foreachRDD(lambda x: push_to_topics(x))
contents.pprint()

ssc.start()
ssc.awaitTermination()
# %%
