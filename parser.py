import re
import json

log_file = "access_log_Jul95"

# Regex pattern to parse the log line
log_pattern = re.compile(
    r'(?P<ip>\S+) - - \[(?P<timestamp>[^\]]+)\] '
    r'"(?P<method>\S+)\s(?P<url>\S+)\s(?P<protocol>[^"]+)" '
    r'(?P<status_code>\d{3}) (?P<bytes>\S+)'
)

# Read and parse log lines
with open(log_file, 'r') as f:
    for i, line in enumerate(f):
        match = log_pattern.match(line)
        if match:
            log_json = match.groupdict()
            # Handle '-' in bytes as 0
            log_json['bytes'] = int(log_json['bytes']) if log_json['bytes'].isdigit() else 0
            log_json['status_code'] = int(log_json['status_code'])
            print(json.dumps(log_json))
        if i == 10:  # Print only first 10 lines for now
            break
