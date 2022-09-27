import boto3


class awsHandler:

    def __init__(self):
        self.client = boto3.client('s3')


    def upload_json_to_s3(self, obj, bucket, file_name):
        response = self.client.put_object(
            Body=obj,
            Bucket=bucket,
            Key=file_name
        )

        return response