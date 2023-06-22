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

# MongoDB에 연결
connStr = "mongodb+srv://cloudIOT2023:IOT2023@cluster0.fmzsuhg.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connStr)
db = client['dms']
coll = db["chat"]

app = Flask(__name__)
CORS(app)

# 배치 처리 후 보내는 시간
start_time = 21
end_time = 9

# 09~22시 사이에는 배치 레이어 작업 => 시간 체크하는 함수
# check_time(): time is 09~22 => return True
def check_time():
    time_now = datetime.datetime.now().hour
    print(time_now)
    if time_now < end_time or time_now > start_time:
        print("False")
        return False
    else:
        print("True")
        return True


# producer가 topic에 msg 전송
def producerSend(writer, timestamp, content):
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: dumps(x).encode('utf-8'))
    test_string={"writer": writer, "timestamp": str(timestamp), "content": content}
    producer.send('test-topic', value=test_string)
    producer.flush()
    result = consumer()
    print("###############", result)


# consumer가 topic에서 msg 수신
def consumerGet():
    consumer = KafkaConsumer('test-topic', bootstrap_servers='localhost:9092')
    for message in consumer:
        value = message.value
        d = json.loads(value.decode('utf-8'))
        string = {"writer": d.get("writer", "Nothing"),"content": d.get("content", "Nothing"), "timestamp": d.get("timestamp", "Nothing")}
        return string
        

# index route - 사용할 일 없음
@app.route("/")
def index():
    print("홈 들어옴")
    return "home"


# UI에서 msg 받아와 producerSend 메소드 실행
@app.route("/msg_send",methods=['POST'])
def producer_test():
    params = request.get_json()
    writer = params['writer']
    timestamp = params['timestamp']
    content = params['content']

    # 09~22시 사이에 요청 보냄
    if check_time():
        producerSend(writer, timestamp, content)
        return "ok"
    else:
        test_string = {"writer": writer, "timestamp": str(timestamp), "content": content, "send":0}
        coll.insert_one(test_string)
        return "batch"


@app.route("/img_send", methods=['POST'])
def img_send():
    image = request.files["image"]

    # 이미지를 저장할 경로 설정
    image_path = "./images/"
    filename = secure_filename(image.filename)
    save_path = os.path.join(image_path, filename)

    try:
        # 이미지 저장
        image.save(save_path)
        print(save_path)
        subprocess.run(["python3","./image_test.py", save_path])

        return jsonify({"message": "이미지가 성공적으로 업로드되었습니다."}), 200
    except Exception as e:
        print(e)
        return jsonify({"error": str(e)}), 500


def consumer():
    ans = consumerGet()
    # 형식 ans = '{"writer": "zeun", "timestamp": "06/09 15:43", "content": "h2hh2"}'
    json_ans = json.dumps(ans)
    print(json_ans)
    # UI 서버에 api 통해 json_ans 전달
    url = 'http://localhost:5000/consumer'

    # 09~22시 사이에 요청 보냄
    if check_time():
        try:
            # 요청 데이터
            data = json_ans

            # API 요청 보내기
            response = requests.post(url, json=data)

            # 응답 처리
            if response.status_code == 200:
                # API 응답을 이용한 작업 수행
                result = response.json()

                return jsonify(result)
            else:
                return 'API 요청이 실패하였습니다.'

        except requests.exceptions.RequestException as e:
            # 예외 처리
            return 'API 요청 중 오류가 발생하였습니다: ' + str(e)


app.run(port=8989, host='localhost', debug=True)
