import json
import logging
import os

import boto3

from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, format='%(message)s')


"""
Move latest RDS snapshot to latest folder in S3 and start glue crawler job.
"""
def handler(event, context):
    date = datetime.today().strftime('%Y-%m-%d')
    logger.info(f'Date: {date}')

    bucket_name = os.environ['S3_BUCKET_NAME']
    old_folder = f'siservices-prod-db/dt-siservices-prod-db-{date}/'
    new_folder = 'latest/'

    print (old_folder)

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    print (bucket)

    logger.info('Purge latest folder!')
    bucket.objects.filter(Prefix=new_folder).delete()

    logger.info('Copy RDS snapshot data over to latest folder!')
    for obj in bucket.objects.filter(Prefix=old_folder):
        old_source = {'Bucket': bucket_name, 'Key': obj.key}
        new_key = obj.key.replace(old_folder, new_folder, 1)
        s3.Object(bucket_name, new_key).copy_from(CopySource=old_source)

    logger.info('Start Glue Crawler job!')
    glue_client = boto3.client('glue', region_name='us-east-1')
    response = glue_client.start_crawler(Name=os.environ['GLUE_CRAWLER_NAME'])
    logger.info(json.dumps(response))
