import requests, json, time
from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory

def store_file_meta_data(path, bucket_name, file_name, raw_file_path, size, pipeline, job_name, status, generate_id = "undefined"):
    _logger = SrvLoggerFactory('main').get_logger()
    my_url = ConfigClass.data_ops_host
    payload  = {
        "path": path,
        "bucket_name": bucket_name,
        "file_name": file_name,
        "raw_file_path": raw_file_path,
        "size": size,
        "process_pipeline": pipeline,
        "job_name": job_name,
        "status": status,
        "generate_id": generate_id
    }
    _logger.debug("Saving Meta: " + str(payload))
    res = requests.post(
            url=my_url + "/v1/files/processed",
            json=payload
    )
    _logger.info('Meta Saved: ' + file_name + "  result: " + res.text)
    if res.status_code != 200:
        raise Exception(res.text)
    return res.json()

def store_file_meta_data_raw(path, bucket_name, file_name, raw_file_path, size, pipeline, job_name, status):
    _logger = SrvLoggerFactory('main').get_logger()
    # create entity in atlas
    post_data = {
        'referredEntities': {},
        'entity': {
            'typeName': 'nfs_file',
            'attributes': {
                'owner': bucket_name,
                'modifiedTime': 0,
                'replicatedTo': None,
                'userDescription': None,
                'isFile': False,
                'numberOfReplicas': 0,
                'replicatedFrom': None,
                'qualifiedName': file_name,
                'displayName': None,
                'description': None,
                'extendedAttributes': None,
                'nameServiceId': None,
                'path': file_name,
                'posixPermissions': None,
                'createTime': time.time(),
                'fileSize': size,
                'clusterName': None,
                'name': file_name,
                'isSymlink': False,
                'group': None,
                'updateBy': pipeline,
                'bucketName': bucket_name,
                'fileName': file_name,
                'generateID': 'undefined'
            },
            'isIncomplete': False,
            'status': 'ACTIVE',
            'createdBy': pipeline,
            'version': 0,
            'relationshipAttributes': {
                'schema': [],
                'inputToProcesses': [],
                'meanings': [],
                'outputFromProcesses': []
            },
            'customAttributes': {
                'generateID': 'undefined'
            },
            'labels': ['pipelinegenerate']
        }
    }
    res = requests.post(ConfigClass.service_cateloguing + '/v1/entity',
                        json=post_data, headers={'content-type': 'application/json'})
    if res.status_code != 200:
        raise Exception(res.text)
    return res.json()