import boto3
import io
from botocore.exceptions import ClientError


def get_ssm_secret(name: str, region="us-east-2") -> str:
    """Get Secrets Manager Secret"""

    client = boto3.client(service_name="secretsmanager", region_name=region)

    secret = client.get_secret_value(**{'SecretId': name})
    if secret.get('SecretString'):
        return secret['SecretString']
    elif secret.get('SecretBinary'):
        return secret['SecretBinary']
    else:
        return "Not found"
