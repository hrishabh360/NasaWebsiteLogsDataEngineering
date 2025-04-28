SELECT *
FROM nasa_logs_nasa_kinesis_output_logs LIMIT 10;

-- 1. Most Requested URLs
SELECT url
	,COUNT(*) AS hits
FROM nasa_logs_nasa_kinesis_output_logs
GROUP BY url
ORDER BY hits DESC LIMIT 10;

-- 2. HTTP Status Code Distribution
SELECT status_code
	,COUNT(*) AS count
FROM nasa_logs_nasa_kinesis_output_logs
GROUP BY status_code
ORDER BY count DESC;

-- 3. Traffic Over Time (Hourly Breakdown)
SELECT substr(TIMESTAMP, 1, 14) AS hour_block
	,COUNT(*) AS requests
FROM nasa_logs_nasa_kinesis_output_logs
GROUP BY substr(TIMESTAMP, 1, 14)
ORDER BY hour_block;

-- 4. Top IP Addresses (most active users) 
SELECT ip
	,COUNT(*) AS request_count
FROM nasa_logs_nasa_kinesis_output_logs
GROUP BY ip
ORDER BY request_count DESC LIMIT 10;