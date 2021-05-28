from os import DirEntry
import requests
import json
import time
from config import ConfigClass
from services import file_meta
from services.file_meta import file_data_mgr
from services.logger_services.logger_factory_service import SrvLoggerFactory
from services.file_meta.file_data_mgr import SrvFileDataMgr


def store_file_meta_data_v2(uploader, output_file_name, output_path, file_size, desc, namespace,
                            project_name, labels, generate_id="undefined", operator=None,
                            from_parents=None, process_pipeline=None, parent_folder_geid=None):
    file_data_mgr = SrvFileDataMgr()
    return file_data_mgr.create(
        uploader,
        output_file_name,
        output_path,
        file_size,
        desc,
        namespace,
        project_name,
        labels,
        generate_id,
        operator,
        from_parents,
        process_pipeline,
        parent_folder_geid=parent_folder_geid)


def archive_file_data(path, file_name, trash_path, trash_file_name, operator, project_code, file_name_suffix, updated_file_name):
    _logger = SrvLoggerFactory('main').get_logger()
    file_data_mgr = SrvFileDataMgr()
    trash_geid = file_data_mgr.fetch_guid()
    archive_res = file_data_mgr.archive(
        path, file_name, trash_path, trash_file_name, operator, file_name_suffix, trash_geid, _logger)
    archive_neo4j_res = file_data_mgr.archive_in_neo4j(
        path, file_name, project_code, updated_file_name)
    _logger.debug("archive_neo4j_res: " + str(archive_neo4j_res))
    create_trash_node_res = file_data_mgr.create_trash_node_in_neo4j(
        path+"/"+updated_file_name, trash_path+"/"+trash_file_name, trash_geid)
    _logger.debug("create_trash_node_res: " + str(create_trash_node_res))
    return archive_res


def store_file_meta_data(output_full_path, bucket_name, file_name, raw_file_path,
                         size, pipeline, job_name, status, generate_id="undefined", uploader=None):
    _logger = SrvLoggerFactory('main').get_logger()
    my_url = ConfigClass.DATA_OPS_GR
    payload = {
        "path": output_full_path,
        "bucket_name": bucket_name,
        "file_name": file_name,
        "raw_file_path": raw_file_path,
        "size": size,
        "process_pipeline": pipeline,
        "job_name": job_name,
        "status": status,
        "generate_id": generate_id,
    }
    if uploader:
        payload['owner'] = uploader
    _logger.debug("Saving Meta: " + str(payload))
    res = requests.post(
        url=my_url + "files/processed",
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
    res = requests.post(ConfigClass.CATALOGUING_SERVICE + 'entity',
                        json=post_data, headers={'content-type': 'application/json'})
    if res.status_code != 200:
        raise Exception(res.text)
    return res.json()


def add_copied_with_approval(_logger, input_full_path, project_code):
    # only can be used to transfer data to VRE CORE
    # neo4j version -----------------------------------------------------------------------------------
    file_data_mgr = SrvFileDataMgr()
    res_add_neo4j = file_data_mgr.add_approval_copy_for_neo4j(
        input_full_path, project_code)
    if not res_add_neo4j == "Succeed":
        _logger.error("add_approval_tag in neo4j failed" + str(res_add_neo4j))
    # atlas version v1 ---------------------------------------------------------------------------------
    _logger.debug(
        '[add_copied_with_approval] input_full_path: ' + input_full_path)
    url = ConfigClass.CATALOGUING_SERVICE + "entity/basic"
    payload = {
        "excludeDeletedEntities": True,
        "includeSubClassifications": False,
        "includeSubTypes": False,
        "includeClassificationAttributes": False,
        "entityFilters": {
            "condition": "AND",
            "criterion": [
                {
                    "attributeName": "name",
                    "attributeValue": input_full_path,
                    "operator": "eq"
                }
            ]
        },
        "tagFilters": None,
        "attributes": [
            "owner",
            "downloader",
            "fileName"
        ],
        "limit": 10,
        "offset": "0",
        "sortBy": "createTime",
        "sortOrder": "DESCENDING",
        "typeName": "nfs_file",
        "classification": None,
        "termName": None
    }
    response = requests.post(
        url=url,
        json=payload
    )
    _logger.debug(str(payload))
    data = None
    if response.status_code == 200 and response.json().get('result').get('entities', None):
        data = response.json()
    else:
        payload["typeName"] = "nfs_file_processed"
        response = requests.post(
            url=url,
            json=payload
        )
        if response.status_code == 200:
            data = response.json()
    if data:
        _logger.debug(
            '[add_copied_with_approval]data[result] ' + str(data['result']))
        _logger.debug('[add_copied_with_approval] entity found: ' +
                      str(data['result'].get('entities', None)))
        entities = data['result'].get('entities', None)
        if entities and len(entities) > 0:
            entity = entities[0]
            _logger.debug('add_copied_with_approval entity: ' + str(entity))
            guid = entity['guid']
            labels = entity.get('labels', [])
            labels.append(ConfigClass.copied_with_approval)
            url_create_labels = ConfigClass.DATA_OPS_GR + "data/tags"
            create_label_payload = {
                "guid": guid,
                "tag": ConfigClass.copied_with_approval,
                "taglist": labels
            }
            _logger.debug(
                'add_copied_with_approval create_label_payload: ' + str(create_label_payload))
            res_add_label = requests.post(
                url=url_create_labels,
                json=create_label_payload
            )
            _logger.debug('add_copied_with_approval res status: ' +
                          str(res_add_label.status_code))
            if res_add_label.status_code != 200:
                _logger.error(res_add_label.text)
                return res_add_label.text
            return res_add_label.json()
    else:
        _logger.error("[add_copied_with_approval] no parent entity found")
    return response.text


def get_resource_bygeid(geid):
    '''
    if not exist return None
    '''
    url = ConfigClass.NEO4J_SERVICE_V2 + "nodes/query"
    payload_file = {
        "page": 0,
        "page_size": 1,
        "partial": False,
        "order_by": "global_entity_id",
        "order_type": "desc",
        "query": {
            "global_entity_id": geid,
            "labels": ['File']
        }
    }
    payload_folder = {
        "page": 0,
        "page_size": 1,
        "partial": False,
        "order_by": "global_entity_id",
        "order_type": "desc",
        "query": {
            "global_entity_id": geid,
            "labels": ['Folder']
        }
    }
    payload_project = {
        "page": 0,
        "page_size": 1,
        "partial": False,
        "order_by": "global_entity_id",
        "order_type": "desc",
        "query": {
            "global_entity_id": geid,
            "labels": ['Dataset']
        }
    }
    response_file = requests.post(url, json=payload_file)
    if response_file.status_code == 200:
        result = response_file.json()['result']
        if len(result) > 0:
            return result[0]
    response_folder = requests.post(url, json=payload_folder)
    if response_folder.status_code == 200:
        result = response_folder.json()['result']
        if len(result) > 0:
            return result[0]
    response_project = requests.post(url, json=payload_project)
    if response_project.status_code == 200:
        result = response_project.json()['result']
        if len(result) > 0:
            return result[0]
    raise Exception('Not found resource: ' + geid)


def get_connected_nodes(geid, direction: str = "both"):
    '''
    return a list of nodes
    '''
    if direction == 'both':
        params = {
            "direction": "input"
        }
        url = ConfigClass.NEO4J_SERVICE + "relations/connected/{}".format(geid)
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception('Internal error for neo4j service, \
                when get_connected, geid: ' + str(geid))
        connected_nodes = response.json()['result']
        params = {
            "direction": "output"
        }
        url = ConfigClass.NEO4J_SERVICE + "relations/connected/{}".format(geid)
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception('Internal error for neo4j service, \
                when get_connected, geid: ' + str(geid))
        return connected_nodes + response.json()['result']
    params = {
        "direction": direction
    }
    url = ConfigClass.NEO4J_SERVICE + "relations/connected/{}".format(geid)
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception('Internal error for neo4j service, \
            when get_connected, geid: ' + str(geid))
    connected_nodes = response.json()['result']
    return connected_nodes


def create_folder_node(zone, geid, name, level, project_code, uploader,
                       relative_path, tags=[], parent_geid="", parent_name="", extra_labels=[], extra_attrs={}):
    url = ConfigClass.ENTITY_INFO_SERVICE + "folders"
    payload = {
        "global_entity_id": geid,
        "folder_name": name,
        "folder_level": level,
        "folder_parent_geid": parent_geid,
        "folder_parent_name": parent_name,
        "uploader": uploader,
        "folder_relative_path": relative_path,
        "zone": zone,  # "greenroom | vrecore"
        "project_code": project_code,
        "folder_tags": tags,
        "extra_labels": extra_labels,
        "extra_attrs": extra_attrs
    }
    response = requests.post(url, json=payload)
    if response.status_code != 200:
        raise Exception('Create folder node failed: ' + str(payload))
    return response.json()['result']


def http_update_node(primary_label, neo4j_id, update_json):
    # update neo4j node
    update_url = ConfigClass.NEO4J_SERVICE + \
        "nodes/{}/node/{}".format(primary_label, neo4j_id)
    # update_json = {
    #     "archived": True,
    #     "name": updated_file_name,
    #     "full_path": path + "/" + updated_file_name
    # }
    res = requests.put(url=update_url, json=update_json)
    return res


def fetch_geid():
    # fetch global entity id
    entity_id_url = ConfigClass.UTILITY_SERVICE + "utility/id"
    respon_entity_id_fetched = requests.get(entity_id_url)
    if respon_entity_id_fetched.status_code == 200:
        pass
    else:
        raise Exception('Entity id fetch failed: ' + entity_id_url +
                        ": " + str(respon_entity_id_fetched.text))
    geid = respon_entity_id_fetched.json()['result']
    return geid


def event_payload_input_geid(zone, project_code, relative_path, name):
    url = ConfigClass.NEO4J_SERVICE_V2 + "nodes/query"
    payload = {
        "page": 0,
        "page_size": 1,
        "partial": False,
        "order_by": "global_entity_id",
        "order_type": "desc",
        "query": {
            "folder_relative_path": relative_path,
            "name": name,
            "project_code": project_code,
            "labels": [zone, 'Folder']
        }
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        result = response.json()['result']
        if len(result) > 0:
            return result[0]
    return None
