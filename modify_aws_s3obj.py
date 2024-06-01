import boto3, datetime, os, configparser, pandas as pd
from typing import List, Dict, Tuple
from tqdm import tqdm

def get_all_objects(s3_client, working_bucket: str, prefix: str,
                    less_than_date: datetime,
                    export_returns_to_folder: str = '') -> Tuple[List[Dict], List[Dict]]:
    '''
    Function to get all objects earlier than a specific date. Generates a to_download list consisting 
    of $Latest object and to_delete list containing all objects earlier than less_than_date.

    Args:
        s3_client (Any):
        working_bucket (str): s3 bucket
        prefix (str): prefix to filter bucket to
        less_than_dat (datetime): only process objects earlier than this date
        export_returns_to_folder (str): Optional. export list of objects collected to folderpath
    
    Returns:
        Tuple[List[Dict], List[Dict]]: list of objects to download, list of objects to delete; both list
        contains the object key ('Key') and version ('VersionId').
    '''
    objects = []
    to_delete = []
    to_download = []
    paginator = s3_client.get_paginator('list_object_versions')
    for result in tqdm(paginator.paginate(Bucket=working_bucket, Prefix=prefix), 'Getting objects from pages'):
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

    # for debug, print out objects to check
    if len(export_returns_to_folder) > 0: 
        pd.DataFrame(objects).to_csv(os.path.join(export_returns_to_folder, 's3objects.csv'))
    return to_download, to_delete

def delete_objects(s3_client, working_bucket:str, key_version_to_delete: List[Dict]):
    '''
    Function to delete s3 objects. Requests are sent in batch of 1000 using aws api.

    Args:
        s3_client (Any):
        key_version_to_delete (List[Dict]): List of Dict[Key, VersionId]
    
    Returns: None
    '''
    batch_size = 1000 # enforce batch size of 1000 to keep within aws api limits
    for i in tqdm(range(0, len(key_version_to_delete), batch_size), 'Deleting objects'):
        batch = key_version_to_delete[i:i+batch_size]
        response = s3_client.delete_objects(Bucket=working_bucket, Delete={'Objects': batch})

def download_objects(s3_client, working_bucket: str, local_dir: str, key_version_to_download: List[Dict]):
    '''
    Function to download s3 objects. Requests are sent one by one using aws api. Objects are downloaded
    to local_dir into the same folder structure represented by object prefix.

    Args:
        s3_client (Any):
        local_dir (str): local directory to download s3 objects to
        key_version_to_download (List[Dict]): List of Dict[Key, VersionId]
    
    Returns: None
    '''
    for i in tqdm(range(0, len(key_version_to_download)), 'Downloading files'):
        s3file = key_version_to_download[i]['Key']
        version = key_version_to_download[i]['VersionId']
        filepath = os.path.dirname(os.path.join(local_dir, s3file))
        filename = os.path.basename(s3file)
        if not os.path.exists(filepath): os.makedirs(filepath)
        s3_client.download_file(working_bucket, 
                                s3file, 
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
    prefix= '<your-prefix>' #'News/DailyEvents/Archived/'
    ls_date = datetime.datetime(2024, 6, 2) # only files earlier than this date will be returned

    to_download, to_delete = get_all_objects(s3_client, working_bucket, prefix, ls_date)
    # download_objects(s3_client, working_bucket, '/tmp', to_download)
    # delete_objects(s3_client, working_bucket, to_delete)
pass