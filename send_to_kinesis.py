import boto3
import json
import re
import time
import random

# Load AWS credentials from local JSON file
with open("aws_credentials.json") as cred_file:
    creds = json.load(cred_file)

# Config
LOG_FILE = r"access_log_Jul95"  # 🔁 Replace with your actual file path
STREAM_NAME = "website-traffic-stream"               # 🔁 Make sure this matches your Kinesis stream
REGION = creds["region"]

# Create Kinesis client using manual credentials
kinesis = boto3.client(
    "kinesis",
    region_name=REGION,
    aws_access_key_id=creds["aws_access_key_id"],   
    aws_secret_access_key=creds["aws_secret_access_key"],
    aws_session_token=creds["aws_session_token"]
)

# Log line regex pattern
pattern = re.compile(
    r'(?P<ip>\S+) - - \[(?P<timestamp>[^\]]+)\] '
    r'"(?P<method>\S+)\s(?P<url>\S+)\s(?P<protocol>[^"]+)" '
    r'(?P<status_code>\d{3}) (?P<bytes>\S+)'
)

# Stream log entries to Kinesis
with open(LOG_FILE, "r") as f:
    for line in f:
        match = pattern.match(line)
        if match:
            record = match.groupdict()
            record["bytes"] = int(record["bytes"]) if record["bytes"].isdigit() else 0
            record["status_code"] = int(record["status_code"])

            # Send to Kinesis
            kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(record),
                PartitionKey=str(random.randint(1, 100))
            )

            print(f"Sent to Kinesis: {record['url']}")
            time.sleep(0.1)  # simulate real-time traffic delay
