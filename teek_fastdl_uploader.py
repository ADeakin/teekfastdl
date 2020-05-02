import boto3
import glob
import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger('teekfastdl_uploader')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler('teekfastdl_uploader.log', maxBytes=200000, backupCount=10)
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler())

ACCESS_KEY = ''
SECRET_KEY = ''
SERVICE_ENDPOINT = 'https://s3.us-east-2.wasabisys.com'
BUCKET = 'teekfastdl'
LOCAL_FASTDL = '/home/anthony/Workspace/teekfastdl/fastdl/' #you need the trailing slash

def upload_to_aws(s3, bucket, local_file, s3_file):
    
    #mirror local file structure
    if s3_file is None:
        s3_file = local_file

    try:
        logger.info('Uploading {0} to s3 as {1}'.format(local_file, s3_file))
        s3.upload_file(local_file, bucket, s3_file, ExtraArgs={'ACL':'public-read'})
        logger.info('Successfully uploaded {0}'.format(s3_file))
    except Exception as e:
        logger.error('Failed to upload {0} to s3 as {1}'.format(local_file, s3_file))
        logger.error(e,exc_info=True)

try:
    s3 = boto3.client('s3', endpoint_url=SERVICE_ENDPOINT, aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)

    bucket = s3.list_objects_v2(Bucket=BUCKET)
    bucket_filelist = []

    if bucket.get('Contents') is not None:
        for item in bucket.get('Contents'):
            bucket_filelist.append(item.get('Key'))

    fastdl = glob.iglob(LOCAL_FASTDL + '**/*.*', recursive=True)

    if fastdl is not None:
        for filename in fastdl:
            filename_noprefix = filename[len(LOCAL_FASTDL):]
            if filename_noprefix not in bucket_filelist:
                upload_to_aws(s3,BUCKET,filename,filename_noprefix)
    else:
        logger.info('{0} not does not exist or is empty'.format(LOCAL_FASTDL))

except Exception as e:
    logger.error('Error reading or comparing files')
    logger.error(e,exc_info=True)