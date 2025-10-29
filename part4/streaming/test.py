import lambda_function

event = {
    "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49668375182324373015039941841055396228491141070410743810",
                "data": "ewogICAgICAgICJyaWRlIjogewogICAgICAgICAgICAiUFVMb2NhdGlvbklEIjogMTMwLAogICAgICAgICAgICAiRE9Mb2NhdGlvbklEIjogMjA1LAogICAgICAgICAgICAidHJpcF9kaXN0YW5jZSI6IDMuNjYKICAgICAgICB9LCAKICAgICAgICAicmlkZV9pZCI6IDE1NgogICAgfQ==",
                "approximateArrivalTimestamp": 1761483880.941
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49668375182324373015039941841055396228491141070410743810",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::566450754297:role/lambda-kinesis-role",
            "awsRegion": "ap-south-1",
            "eventSourceARN": "arn:aws:kinesis:ap-south-1:566450754297:stream/ride_updates"
        }
    ]
}

result = lambda_function.lambda_handler(event, None)
print(result)