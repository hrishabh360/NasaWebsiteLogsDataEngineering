import json
import boto3
import base64
import uuid
from datetime import datetime

s3 = boto3.client('s3') 
bucket_name = 'nasa-kinesis-output-logs'  # 🔁 Replace with your actual bucket name

def lambda_handler(event, context):
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue

        # Optional: create a unique file name per log
        log_key = f"parsed-logs/{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_{uuid.uuid4()}.json"

        # Save each record as a JSON file in S3
        s3.put_object(
            Bucket=bucket_name,
            Key=log_key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully processed records.')
    }
