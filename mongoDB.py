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

connStr = "mongodb+srv://cloudIOT2023:IOT2023@cluster0.fmzsuhg.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connStr)

db = client['dms']
coll = db["chat"]

# document = {"name" : "name2", "age" : 65}
# coll.insert_one(document)

docs = coll.find()
for doc in docs:
    # print(type(doc))
    # print(type(doc['send']))
    if doc['send'] == 0:
        producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: dumps(x).encode('utf-8'))
        test_string = {"writer": doc['writer'], "timestamp": doc['timestamp'], "content": doc['content']}
        producer.send('test-topic', value=test_string)
        producer.flush()
        # print(test_string)

        consumer = KafkaConsumer('test-topic', bootstrap_servers='localhost:9092')
        json_ans = json.dumps(test_string)
        print(f'json_ans: {json_ans}')
        url = 'http://localhost:5000/consumer'

        try:
            data = json_ans
            # 요청 데이터
            response = requests.post(url, json=data)
            # API 요청 보내기

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

        # 발송 이후 업데이트
        query = {'_id': doc['_id']}
        new_values = {'$set': {'send': 1}}
        coll.update_many(query, new_values)
