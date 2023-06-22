import subprocess
from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import loads
from json import dumps
import boto3
import cv2
import os
import argparse

# s3 셋팅
AWS_ACCESS_KEY_ID ="AKIAQME3RJYR4PXK5XWQ"
AWS_SECRET_ACCESS_KEY = "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"
AWS_DEFAULT_REGION = "ap-northeast-2"
bucket_name = '2023-dms-kafka-image'
s3 = boto3.client('s3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_DEFAULT_REGION
                )


def producer_img_send(path):
    # kafka producer 셋팅
    producer = KafkaProducer(bootstrap_servers='3.135.130.17:9092', value_serializer=lambda x: dumps(x).encode('utf-8'))

    img_string={"path":path}
    print("### path",img_string)
    producer.send('img-topic',value=img_string)
    producer.flush()

    # test용
    # s3에 사진 올리기
    file_name = path
    name = path.replace("./images/", "")
    print(name)
    key = 'test/'+name+'.jpg'
    s3.upload_file(file_name,bucket_name, key)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("arg1", help="이미지 url")
    args = parser.parse_args()

    path = args.arg1
    producer_img_send(path)