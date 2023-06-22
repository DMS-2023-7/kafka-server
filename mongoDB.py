import json
import requests
from flask import Flask, request, jsonify
from flask import *
from kafka import KafkaProducer, KafkaConsumer
from json import dumps,loads
import sys
import time
from flask_cors import CORS
import datetime
import subprocess
import os
from werkzeug.utils import secure_filename

import pymongo
from pymongo import MongoClient
from pymongo.mongo_client import MongoClient

def batch_Get():
    # MongoDB에 연결
    connStr = "mongodb+srv://cloudIOT2023:IOT2023@cluster0.fmzsuhg.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(connStr)

    db = client['dms']
    coll = db["chat"]

    # MongoDB 값 쿼리
    docs = coll.find()
    for doc in docs:
        # send가 0일 경우 발송하지 않은 메세지
        # send가 1일 경우 발송한 메세지
        if doc['send'] == 0:
            # producer에 채팅 작성자, 작성시간, 작성 내용 flush
            producer = KafkaProducer(bootstrap_servers='3.135.130.17:9092', value_serializer=lambda x: dumps(x).encode('utf-8'))
            test_string = {"writer": doc['writer'], "timestamp": doc['timestamp'], "content": doc['content']}
            producer.send('test-topic', value=test_string)
            producer.flush()

            # consumer에 채팅 작성자, 작성시간, 작성 내용 받아오기
            consumer = KafkaConsumer('test-topic', bootstrap_servers='3.135.130.17:9092')
            json_ans = json.dumps(test_string)
            print(f'json_ans: {json_ans}')
            url = 'http://18.221.31.141:5000/consumer'

            try:
                # 요청 데이터
                data = json_ans
                # API 요청 보내기
                response = requests.post(url, json=data)
                # 응답 처리
                if response.status_code == 200:
                    # API 응답을 이용한 작업 수행
                    result = response.json()
                    # print(jsonify(result)) #NoneType으로 잡힘
                else:
                    print('API 요청이 실패하였습니다.')
            except requests.exceptions.RequestException as e:
                # 예외 처리
                print('API 요청 중 오류가 발생하였습니다: ' + str(e))

            # 채팅 메세지 발송 이후 send를 1으로 업데이트하여 발송한 메세지로 표시
            query = {'_id': doc['_id']}
            new_values = {'$set': {'send': 1}}
            coll.update_many(query, new_values)


batch_Get()