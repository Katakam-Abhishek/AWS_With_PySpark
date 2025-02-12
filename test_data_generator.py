import os
import random
import string

from typing import List, Any

try:
    import boto3
    import json
except:
    os.system('pip install boto3')
    os.system('pip install json')
finally:
    import boto3
    import json


"""
1. List all JSON files from S3 bucket with specific prefix
2. Read content of JSON file.
3. Iterate over JSON messages
4. Use put record function to push messages to kinsis data stream.
"""


# Initialize S3 client
s3 = boto3.client('s3')

# Initialize Kinesis client
kinesis = boto3.client('kinesis')

bucket_name: str = 'learningtrainingbucket-1' #s3://learningtrainingbucket-1/sample_data/retail-data-store-mock-data.json
prefix: str = 'sample_data'
stram_name: str = 'retail-dataset-source-stream'


def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

#List all JSON files from s3 bucket with a specific prefix
def list_files(bucket_name: str, prefix: str) -> List[str]:
    response  = s3.list_objects_v2(Bucket = bucket_name, Prefix =  prefix)
    # print(f'S3 objects listing objects {response}')
    """response = S3 objects listing objects {'ResponseMetadata': {'RequestId': '49TDMG50WE2EH1B0', 'HostId': 'k1111Yk5zU+n/cW/gnnRYywxpyZp+sxCzUnDGLvceECwbdeRuaDqjMC2xXDkpOx1Uec7/idaE2Q=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'k1111Yk5zU+n/cW/gnnRYywxpyZp+sxCzUnDGLvceECwbdeRuaDqjMC2xXDkpOx1Uec7/idaE2Q=', 'x-amz-request-id': '49TDMG50WE2EH1B0', 'date': 'Wed, 11 Sep 2024 16:53:39 GMT', 'x-amz-bucket-region': 'us-east-1', 'content-type': 'application/xml', 'transfer-encoding': 'chunked', 'server': 'AmazonS3'}, 'RetryAttempts': 0}, 'IsTruncated': False, 
                'Contents': [{'Key': 'sample_data/', 'LastModified': datetime.datetime(2024, 9, 11, 16, 15, 56, tzinfo=tzutc()), 'ETag': '"d41d8cd98f00b204e9800998ecf8427e"', 'Size': 0, 'StorageClass': 'STANDARD'}, {'Key': 'sample_data/retail-data-store-mock-data.json', 'LastModified': datetime.datetime(2024, 9, 11, 16, 18, 29, tzinfo=tzutc()), 'ETag': '"9d382c17ea44282177f50815c5a1a30c"', 'Size': 9537, 'StorageClass': 'STANDARD'}], 'Name': 'learningtrainingbucket-1', 'Prefix': 'sample_data', 'MaxKeys': 1000, 'EncodingType': 'url', 'KeyCount': 2}"""
    
    files:list = []
    for _ in response['Contents']:
        if _['Key'].endswith('json'):
            files.append(_['Key'])

    return files


#Read content of JSON file
def read_file(bucket: str, file_key: str) -> Any:
    response = s3.get_object(Bucket = bucket_name, Key = file_key)
    return json.loads(response['Body'].read())

def put_record_to_kinesis(stream_name, data):
    # response = kinesis.put_record(StreamName = stream_name, Data = json.dumps(data), PartitionKey = 'default')
    response = kinesis.put_record(StreamName = stream_name, Data = json.dumps(data), PartitionKey = generate_random_string(10))
    # print(f'Successfully put record to Kinesis. Sequence Number: {response["SequenceNumber"]}')
    print(response)


if __name__ == '__main__':
    for _ in list_files(bucket_name, prefix):
        data = read_file(bucket_name, file_key = _)
        # print(f'data read from s3 object: {data}')
        for message in data:
            put_record_to_kinesis(stram_name, message)
