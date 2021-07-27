from posixpath import join
from config import ConfigClass
import os
import datetime
from copy import copy
from services.file_meta.file_data_mgr import http_query_node
from utils.meta_data_operations import get_folder_node_bypath, store_file_meta_data_v2, add_copied_with_approval, get_resource_bygeid, \
    get_connected_nodes, fetch_geid, create_folder_node, http_update_node
from utils.lineage_operations import create_lineage_v3
from utils.file_opertion_status import update_file_operation_logs, update_file_operation_status_v2, EDataActionType, \
    EActionState
from models.enum_and_events import EPipelineName
from models.folder import FolderMgr
import requests
import time
from models.minio_client import Minio_Client


def on_data_transfer_succeed(_logger, annotations):
    if ConfigClass.debug_mode:
        _logger.info("debug mode, skipped")
        return
    _logger.info("_on_data_transfer_succeed Event triggered")
    _logger.info(annotations)
    input_geid = annotations.get('event_payload_input_geid', None)
    if not input_geid:
        _logger.error(
            "[Fatal] None event_payload_input_geid: " + str(annotations))
        raise Exception('[Fatal] None event_payload_input_geid')
    # get source neo4j node
    source_node = get_resource_bygeid(input_geid)
    if not source_node:
        _logger.error(
            "[Fatal] Source node not found for: " + str(input_geid))
        raise Exception(
            '[Fatal] Source node not found for: ' + str(input_geid))
    _logger.debug(
        "_on_data_transfer_succeed annotations: " + str(annotations))
    labels = source_node['labels']
    resource_type = get_resource_type(labels)
    _logger.info("resource_type: " + str(resource_type))
    # create meta
    try:
        if resource_type == 'File':
            on_single_file_transferred(_logger, annotations, source_node)
        if resource_type == 'Folder':
            # folder no longer supported, folder copy will be decomposed into multiple file copies
            pass
    except Exception as e:
        _logger.error("Error when creating metadata: " + str(e))
        if ConfigClass.env == 'test':
            raise
        raise Exception("[Internal] Error when creating metadata: " + str(e))

def on_data_transfer_failed(_logger, annotations):
    input_geid = annotations.get('event_payload_input_geid', None)
    if not input_geid:
        _logger.error(
            "[Fatal] None event_payload_input_geid: " + str(annotations))
        raise Exception('[Fatal] None event_payload_input_geid')
    session_id = annotations.get('event_payload_session_id', 'default_session')
    job_id = annotations.get('event_payload_job_id', 'default_job')
    # get source neo4j node
    source_node = get_resource_bygeid(input_geid)
    if not source_node:
        _logger.error(
            "[Fatal] Source node not found for: " + str(input_geid))
        raise Exception(
            '[Fatal] Source node not found for: ' + str(input_geid))
    _logger.debug(
        "on_data_transfer_failed annotations: " + str(annotations))
    labels = source_node['labels']
    resource_type = get_resource_type(labels)
    _logger.info("resource_type: " + str(resource_type))
    zone = 'vrecore'
    res_update_status = update_file_operation_status_v2(session_id, job_id, zone,
        EActionState.TERMINATED.name,
        payload={"message": "pipeline failed."})

def on_single_file_transferred(_logger, annotations, source_node):
    '''
    when transferred a single file
    '''
    # default is None
    destination_geid = annotations.get('event_payload_destination_geid')
    destination = None
    # get destination
    if destination_geid and destination_geid != 'None':
        destination = get_resource_bygeid(destination_geid)
    input_full_path = annotations["input_path"]
    output_full_path = annotations["output_path"]
    output_file_name = os.path.basename(output_full_path)
    output_path = os.path.dirname(output_full_path)
    project_code = annotations["project"]
    bukcet_name = "core-" + project_code
    # get project
    project_response = http_query_node('Container', {"code": project_code})
    project_info = project_response.json()[0]
    generate_id = annotations.get("generate_id", "undefined")
    uploader = annotations.get("uploader")
    session_id = annotations.get('event_payload_session_id', 'default_session')
    job_id = annotations.get('event_payload_job_id', 'default_job')
    operator = annotations.get('event_payload_operator', 'admin')
    # generate meta information
    unix_process_time = datetime.datetime.utcnow().timestamp()
    labels = source_node['labels']
    zone = 'vrecore'
    # get task payloads
    versioning = ''
    url = ConfigClass.DATA_OPS_UT + "tasks"
    task_response = requests.get(
        url,
        params={
            "session_id": "*",
            "job_id": job_id
        }
    )
    my_task = task_response.json()['result'][0]
    versioning = my_task['payload'].get('versioning', '')
    source_folder = my_task['payload'].get('source_folder')
    if versioning == '':
        minio_cli = Minio_Client(ConfigClass)
        object_stat = minio_cli.client.stat_object(bukcet_name, output_full_path)
        _logger.debug("Minio versioning=======================" + object_stat.version_id)
        versioning = object_stat.version_id
    # Saving folder metadata
    created_folders_cache = []
    folder_mgr = FolderMgr(
                created_folders_cache,
                project_info["global_entity_id"],
                project_code,
                output_path,
                [],
                zone)
    folder_mgr.create(uploader)
    last_folder_node = folder_mgr.last_node
    # Saving file metadata
    # v2 API
    from_parents = {
        "global_entity_id": source_node['global_entity_id'],
        "original_geid": source_node['global_entity_id']
    }
    file_node_stored = store_file_meta_data_v2(
        uploader,
        output_file_name,
        output_path,
        source_node.get('file_size', 0),
        'processed by data_transfer',
        zone,
        project_code,
        [tag for tag in source_node.get('tags', []) if tag != ConfigClass.copied_with_approval],
        generate_id,
        operator,
        from_parents=from_parents,
        process_pipeline=EPipelineName.data_transfer.name,
        parent_folder_geid=last_folder_node.global_entity_id,
        original_geid=source_node['global_entity_id'],
        bucket=bukcet_name,
        object_path=output_full_path,
        version_id=versioning)
    copy_zippreview(
        source_node['global_entity_id'],
        file_node_stored['global_entity_id'])
    # update extra attibutes
    update_json = {}
    for k, v in source_node.items():
        if not k in file_node_stored:
            update_json[k] = v
    updated_file_node_stored = http_update_node(
        "File", file_node_stored['id'], update_json=update_json)
    res = updated_file_node_stored.json()[0]
    refresh_node(file_node_stored, res)
    _logger.debug('Saved meta v2')
    create_lineage_v3(
        source_node['global_entity_id'],
        file_node_stored['global_entity_id'],
        project_code,
        EPipelineName.data_transfer.name,
        'data_transfer Processed',
        unix_process_time)
    _logger.debug('Created Lineage v2')
    # add sys tags
    res_add_copied_with_approval = add_copied_with_approval(
        _logger, 'File', source_node['global_entity_id'], True)
    if source_folder:
        res_source_folder_add_copied_with_approval = add_copied_with_approval(
            _logger, 'Folder', source_folder['global_entity_id'], True)
    res_update_status = update_file_operation_status_v2(session_id, job_id, zone,
        EActionState.SUCCEED.name)
    _logger.debug('res_update_status: ' +
                  str(res_update_status.status_code))
    res_update_audit_logs = update_file_operation_logs(
        uploader,
        operator,
        os.path.join('Greenroom', input_full_path),
        os.path.join('VRECore', output_full_path),
        source_node.get('file_size', 0),
        project_code,
        generate_id
    )
    _logger.debug('res_update_audit_logs: ' +
                  str(res_update_audit_logs.status_code))

def copy_zippreview(old_geid, new_geid):
    url = ConfigClass.DATA_OPS_GR + "archive"
    get_params = {
        "file_geid": old_geid
    }
    response_get = requests.get(url=url, params=get_params)
    if response_get.status_code == 404:
        return
    if response_get.status_code != 200:
        raise Exception(response_get.text)
    json_response_get = response_get.json()
    archive_preview = json_response_get['result']
    post_url = ConfigClass.DATA_OPS_GR + "archive"
    post_json = {
        "file_geid": new_geid,
        "archive_preview": archive_preview
    }
    post_response = requests.post(url=post_url, json=post_json)
    if post_response.status_code != 200:
        raise Exception(post_response.text)

def get_resource_type(labels: list):
    '''
    Get resource type by neo4j labels
    '''
    resources = ['File', 'TrashFile', 'Folder']
    for label in labels:
        if label in resources:
            return label
    return None


def refresh_node(target: dict, new: dict):
    for k, v in new.items():
        target[k] = v