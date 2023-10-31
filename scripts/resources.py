from dagster import resource, Field
import boto3


class Boto3Connector(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key, endpoint_url):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url

    # def get_session(self):

    #     session = boto3.session.Session()
    #     return session

    def get_client(self):
        session = boto3.session.Session()

        s3_client = session.client(
            service_name='s3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            endpoint_url=self.endpoint_url,
        )
        return s3_client
