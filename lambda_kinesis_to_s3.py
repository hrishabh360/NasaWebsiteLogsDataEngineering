import json
import boto3
import base64
import uuid
from datetime import datetime

s3 = boto3.client('s3')
bucket_name = 'nasa-kinesis-output-logs'  # ✅ Confirm this is exactly your bucket name

def lambda_handler(event, context):
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        print("Raw Payload:", payload)  # ✅ Log raw input
        
        try:
            data = json.loads(payload)
            print("Parsed JSON:", data)
        except json.JSONDecodeError as e:
            print("JSON Decode Error:", e)
            continue

        log_key = f"parsed-logs/{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_{uuid.uuid4()}.json"
        print(f"Writing to S3 → Bucket: {bucket_name}, Key: {log_key}")

        try:
            s3.put_object(
                Bucket=bucket_name,
                Key=log_key,
                Body=json.dumps(data),
                ContentType='application/json'
            )
            print("✅ Upload successful")
        except Exception as e:
            print("❌ S3 upload failed:", e)

    return {
        'statusCode': 200,
        'body': json.dumps('Finished processing records.')
    }