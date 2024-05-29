import boto3, datetime, pandas as pd, os, configparser
from typing import List, Dict

def get_all_objects(s3_client, working_bucket: str, prefix: str, less_than_date: datetime):
    # get all objects earlier than a specific date
    objects = []
    to_delete = []
    to_download = []
    paginator = s3_client.get_paginator('list_object_versions')
    for result in paginator.paginate(Bucket=working_bucket, Prefix=prefix):
        for version in result.get('Versions', []):
            if (version['LastModified']).replace(tzinfo = None) < less_than_date:
                objects.append(version)
                if version['IsLatest']: 
                    to_download.append({'Key': version['Key'], 'VersionId': version['VersionId']})
                to_delete.append({'Key': version['Key'], 'VersionId': version['VersionId']})

        for delete_marker in result.get('DeleteMarkers', []):
            if (delete_marker['LastModified']).replace(tzinfo = None) < less_than_date:
                objects.append(delete_marker)
                to_delete.append({'Key': delete_marker['Key'], 'VersionId': delete_marker['VersionId']})

    return to_download, to_delete

    # pd.DataFrame(objects).to_csv(r'C:\Users\chen_\Downloads\s3_objects.csv', index=False) # for debug, print out to_delete to check

def delete_objects(s3_client, working_bucket:str, key_version_to_delete: List[Dict]) -> Dict:
    # delete those object return from above
    batch_size = 1000 # enforce batch size of 1000 to keep within aws api limits
    for i in range(0, len(key_version_to_delete), batch_size):
        batch = key_version_to_delete[i:i+batch_size]
        response = s3_client.delete_objects(Bucket=working_bucket, Delete={'Objects': batch})

    return response

def download_objects(s3_client, working_bucket: str, local_dir: str, key_version_to_download: List[Dict]) -> Dict:
    for i in range(0, len(key_version_to_download)):
        key = key_version_to_download[i]['Key']
        version = key_version_to_download[i]['VersionId']
        filepath = os.path.dirname(os.path.join(local_dir, key))
        filename = os.path.basename(key)
        if not os.path.exists(filepath): os.makedirs(filepath)
        s3_client.download_file(working_bucket, 
                                key, 
                                os.path.join(filepath, filename), 
                                ExtraArgs={'VersionId': version})

if __name__ == "__main__":
    secrets = configparser.ConfigParser()
    secrets.read('secrets_DO_NOT_DEPOLY.ini')

    session = boto3.Session(
        aws_access_key_id=secrets['AWS']['AWS_KEY'],
        aws_secret_access_key=secrets['AWS']['AWS_SECRET']
    )
    s3_client = session.client('s3')
    # bucket = session.resource('s3').Bucket(working_bucket)
    # resp = s3_client.list_object_versions(Bucket=working_bucket)

    working_bucket = 'dsta'
    prefix= '<your-prefix>' #'News/DailyEvents/Archived'
    ls_date = datetime.datetime(1990, 1, 1) # only files earlier than this date will be returned

    to_download, to_delete = get_all_objects(s3_client, working_bucket, prefix, ls_date)
    download_objects(s3_client, working_bucket, '/tmp', to_download)
    delete_objects(s3_client, working_bucket, to_delete)
pass