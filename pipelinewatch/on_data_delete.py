import imp
import os
import datetime
import time
import requests
from config import ConfigClass
from services.file_meta import file_data_mgr
from utils.meta_data_operations import store_file_meta_data_v2, get_resource_bygeid, \
    get_connected_nodes, fetch_geid, create_folder_node, archive_file_data, \
    http_update_node, get_folder_node_bypath_without_zone
from utils.lineage_operations import create_lineage_v2
from utils.file_opertion_status import update_file_operation_logs, update_file_operation_status, EDataActionType
from services.file_meta.file_data_mgr import SrvFileDataMgr
from services.file_meta.vr_folders_mgr import SrvVRMgr
from models.enum_and_events import EPipelineName

trashfile_label = 'TrashFile'
trashbin_label = 'TranshBin'


def on_data_delete_succeed(_logger, annotations):
    # return
    _logger.info("on_data_delete_succeed Event triggered")
    _logger.info(annotations)
    input_geid = annotations.get('event_payload_input_geid', None)
    if not input_geid:
        _logger.error(
            "[Fatal] None event_payload_input_geid: " + str(annotations))
        raise Exception('[Fatal] None event_payload_input_geid.')
    # get source neo4j node
    source_node = get_resource_bygeid(input_geid)
    if not source_node:
        _logger.error(
            "[Fatal] Source node not found for: " + str(input_geid))
        raise Exception(
            '[Fatal] Source node not found for: ' + str(input_geid))
    _logger.debug(
        "on_data_delete_succeed annotations: " + str(annotations))
    labels = source_node['labels']
    resource_type = get_resource_type(labels)
    _logger.info("resource_type: " + str(resource_type))
    # create meta
    try:
        if resource_type == 'File':
            on_single_file_deleted(_logger, annotations, source_node)
        if resource_type == 'Folder':
            on_folder_deleted(_logger, annotations, source_node)
    except Exception as e:
        if ConfigClass.env == 'test':
            raise
        raise Exception("[Internal] Error when creating metadata: " + str(e))


def on_folder_deleted(_logger, annotations, source_node):
    '''
    when deleted a folder
    '''
    def is_parent(folder, connected):
        return folder['folder_relative_path'] \
            == os.path.join(connected['folder_relative_path'], connected['name'])
    source_geid = source_node['global_entity_id']
    session_id = annotations.get('event_payload_session_id', 'default_session')
    job_id = annotations.get('event_payload_job_id', 'default_job')
    input_full_path = annotations["event_payload_input_path"]
    input_folder_name = os.path.basename(input_full_path)
    output_full_path = annotations["event_payload_output_path"]
    output_folder_name = os.path.basename(output_full_path)
    output_root_path = os.path.dirname(output_full_path)
    project_code = annotations["event_payload_project"]
    uploader = annotations.get('event_payload_uploader', '')
    operator = annotations.get('event_payload_operator', '')
    zone = get_zone(source_node["labels"])
    zone_param = {
        "VRECore": "vrecore",
        "Greenroom": "greenroom"
    }.get(zone)
    # generate meta information
    unix_process_time = datetime.datetime.utcnow().timestamp()
    # get connected
    connected_nodes = get_connected_nodes(source_geid)
    # folder node creation
    fnodes_created = []

    def wrapper_create_trashbin_folder(to_deactive_node, parent_gotten=None):
        # create mirror folder nodes
        new_geid = fetch_geid()
        extra_attrs = {'in_trashbin': True, 'archived': True}
        if source_node['global_entity_id'] == to_deactive_node['global_entity_id']:
            extra_attrs['is_trashbin_root'] = True
        fnode_created = create_folder_node(
            zone=zone_param,
            geid=new_geid,
            name=to_deactive_node['name'],
            level=to_deactive_node['folder_level'],
            project_code=project_code,
            uploader=to_deactive_node['uploader'],
            relative_path=to_deactive_node['folder_relative_path'],
            tags=to_deactive_node['tags'],
            parent_geid=parent_gotten['global_entity_id'] if parent_gotten else "",
            parent_name=parent_gotten['name'] if parent_gotten else "",
            extra_labels=[trashfile_label],
            extra_attrs=extra_attrs
        )
        fnode_created['original_geid'] = to_deactive_node['global_entity_id']
        fnode_created['original_relative_path'] = to_deactive_node['folder_relative_path']
        fnode_created['original_name'] = to_deactive_node['name']
        fnodes_created.append(fnode_created)
        return fnode_created
    fnodes_connected = [
        node for node in connected_nodes if 'Folder' in node['labels']]
    fnodes_children = [node for node in fnodes_connected
                       if node['folder_level'] > source_node['folder_level'] and
                       node.get('archived') != True]
    # mark to_deactivate_folder_nodes as archived(can be restore)
    # update folder name and relative path of the original fnodes
    update_json = {
        'archived': True,
        'name': output_folder_name
    }
    updated_source_node = http_update_node(
        "Folder", source_node['id'], update_json=update_json)
    if updated_source_node.status_code != 200:
        raise Exception("updated_source_ndoe error: " + updated_source_node.text
                        + "----- payload: " + str(update_json))
    res = updated_source_node.json()[0]
    refresh_node(source_node, res)
    # update relative path
    for fnode in fnodes_children:
        # caculate relative path
        destination_relative_path = get_relative_folder_path(fnode)
        update_json = {
            'archived': True,
            'folder_relative_path': destination_relative_path
        }
        updated_fnode = http_update_node(
            "Folder", fnode['id'], update_json=update_json)
        res: dict = updated_fnode.json()[0]
        refresh_node(fnode, res)
    # find parent_node_original, create mirror node in TRASH zone
    to_deactivate_folder_nodes = fnodes_children + [source_node]
    to_deactivate_folder_nodes.sort(key=lambda f: f['folder_level'])
    for fnode in to_deactivate_folder_nodes:
        # find parent_node_original
        fnode_original_parent = [
            connected_node for connected_node in to_deactivate_folder_nodes
            if is_parent(fnode, connected_node)]
        fnode_original_parent = fnode_original_parent[0] if len(
            fnode_original_parent) > 0 else None
        parent_gotten = None
        if fnode_original_parent:
            parent_gotten = get_folder_node_bypath_without_zone(
                fnode_original_parent['project_code'], fnode_original_parent['folder_relative_path'],
                fnode_original_parent['name'], [trashfile_label])
        wrapper_create_trashbin_folder(fnode, parent_gotten)
    # file node creation
    file_nodes = [node for node in connected_nodes if 'File' in node['labels']
                  and node.get('archived') != True]
    for file_node in file_nodes:
        parent_folder_node = None
        for folder_node in fnodes_created:
            if folder_node['original_geid'] == file_node['parent_folder_geid']:
                parent_folder_node = folder_node
                relative_folder_full_path = os.path.join(
                    folder_node['folder_relative_path'], folder_node['name'])
                original_relative_folder_full_path = os.path.join(
                    folder_node['original_relative_path'], folder_node['original_name'])
        if parent_folder_node:
            _logger.debug('Found child file, creating meta for: ' +
                          file_node['full_path'])
            file_path = {
                'VRECore': os.path.join(
                    "/", ConfigClass.VRE_ROOT_PATH, project_code, original_relative_folder_full_path),
                'Greenroom': os.path.join(
                    "/", ConfigClass.NFS_ROOT_PATH, project_code, 'raw', original_relative_folder_full_path)
            }.get(zone, None)
            file_input_full_path = os.path.join(file_path, file_node['name'])
            file_old_input_full_path = file_node['full_path']
            update_json = {
                'archived': True,
                'full_path': file_input_full_path,
                'path': file_path
            }
            updated_node = http_update_node(
                "File", file_node['id'], update_json=update_json)
            res: dict = updated_node.json()[0]
            refresh_node(file_node, res)
            file_output_full_path = os.path.join(
                output_full_path, get_relative_folder_path_to_source_for_file(file_node, source_node), file_node['name'])
            from_parents = {
                "full_path": file_input_full_path,
            }
            # create trash file node
            trash_node_stored = store_file_meta_data_v2(
                file_node['uploader'],
                file_node['name'],
                os.path.dirname(file_output_full_path),
                file_node['file_size'],
                'processed by data_delete',
                zone_param,
                project_code,
                file_node.get('tags', []),
                file_node['generate_id'],
                operator,
                from_parents=from_parents,
                process_pipeline=EPipelineName.data_delete.name,
                parent_folder_geid=parent_folder_node['global_entity_id']
            )
            update_json = {
                'archived': True,
                'labels': [zone, trashfile_label],
                'in_trashbin': True
            }
            # update extra attibutes
            for k, v in file_node.items():
                if not k in trash_node_stored and not update_json.get(k):
                    update_json[k] = v
            updated_node = http_update_node(
                "File", trash_node_stored['id'], update_json=update_json)
            res: dict = updated_node.json()[0]
            refresh_node(trash_node_stored, res)
            # archive data in atlas
            file_data_mgr = SrvFileDataMgr()
            file_data_mgr.archive(
                os.path.dirname(file_old_input_full_path),
                file_node['name'],
                os.path.dirname(trash_node_stored['full_path']),
                trash_node_stored['name'],
                operator,
                '',
                trash_node_stored['global_entity_id'],
                _logger,
                os.path.dirname(file_input_full_path))
            create_lineage_v2(
                file_input_full_path,
                file_output_full_path,
                project_code,
                EPipelineName.data_delete.name,
                'data_delete Processed',
                datetime.datetime.utcnow().timestamp())
            _logger.debug('Created Lineage v2')

            # updated_file_name = os.path.basename(file_output_full_path)
            # myfilename, file_extension = os.path.splitext(updated_file_name)
            # update_file_name_suffix = myfilename.split("_")[1]
            # archive_res = archive_file_data(file_node['full_path'], file_node['name'], file_output_full_path,
            #                                 os.path.basename(file_output_full_path), operator, project_code, update_file_name_suffix, updated_file_name)

            # Update file entity in elastic search
            es_payload = {
                "global_entity_id": file_node["global_entity_id"],
                "updated_fields": {
                    "name": file_node['name'],
                    "path": os.path.dirname(file_output_full_path),
                    "full_path": file_output_full_path,
                    "archived": True,
                    "process_pipeline": "data_delete",
                    "time_lastmodified": time.time()
                }
            }
            _logger.info(f"es delete file payload: {es_payload}")
            es_res = requests.put(
                ConfigClass.PROVENANCE_SERVICE + 'entity/file', json=es_payload)
            _logger.info(f"es delete file response: {es_res.text}")

            res_update_audit_logs = update_file_operation_logs(
                uploader,
                operator,
                file_input_full_path,
                file_output_full_path,
                0,
                project_code,
                "undefined",
                EDataActionType.data_delete.name
            )
            _logger.debug('res_update_audit_logs: ' +
                          str(res_update_audit_logs.status_code))
    res_update_status = update_file_operation_status(session_id, job_id, EDataActionType.data_delete.name,
                                                     project_code, operator, input_full_path, zone_param)
    _logger.debug('res_update_status: ' +
                  str(res_update_status.status_code))
    _logger.info('Deletion succeed.')


def on_single_file_deleted(_logger, annotations, source_node):
    '''
    when deleted a folder
    '''
    input_full_path = annotations["event_payload_input_path"]
    input_file_name = os.path.basename(input_full_path)
    input_path = os.path.dirname(input_full_path)
    output_full_path = annotations["event_payload_output_path"]
    output_file_name = os.path.basename(output_full_path)
    output_path = os.path.dirname(output_full_path)
    project_code = annotations["event_payload_project"]
    session_id = annotations.get(
        'event_payload_session_id', 'default_session')
    job_id = annotations.get('event_payload_job_id', 'default_job')
    operator = annotations.get('event_payload_operator', '')
    uploader = annotations.get('event_payload_uploader', '')
    generate_id = annotations.get('event_payload_generate_id', '')
    unix_process_time = datetime.datetime.utcnow().timestamp()
    myfilename, file_extension = os.path.splitext(output_file_name)
    update_file_name_suffix = myfilename.split("_")[1]
    updated_file_name = output_file_name
    updated_input_path = input_path + "/" + updated_file_name
    zone = "greenroom" if ConfigClass.data_lake in output_full_path else 'vrecore'
    _logger.debug(
        "_on_file_delete_succeed annotations: " + str(annotations))
    # archive virtual files
    vr_mgr = SrvVRMgr()
    res_delete_vr_files_by_full_path = vr_mgr.delete_vr_files_by_full_path(
        input_full_path)
    _logger.debug("delete_vr_files_by_full_path res: " +
                  str(res_delete_vr_files_by_full_path))
    archive_res = archive_file_data(input_path, input_file_name, output_path,
                                    output_file_name, operator, project_code, update_file_name_suffix, updated_file_name)
    create_lineage_v2(
        updated_input_path,
        output_full_path,
        project_code,
        EPipelineName.data_delete.name,
        'data_delete Processed',
        unix_process_time)
    res_update_status = update_file_operation_status(session_id, job_id, EDataActionType.data_delete.name,
                                                     project_code, operator, input_full_path, zone)
    _logger.debug('res_update_status: ' +
                  str(res_update_status.status_code))
    res_update_audit_logs = update_file_operation_logs(
        uploader,
        operator,
        input_full_path,
        output_full_path,
        source_node.get('file_size', 0),
        project_code,
        generate_id,
        EDataActionType.data_delete.name
    )
    _logger.debug('res_update_audit_logs: ' +
                  str(res_update_audit_logs.status_code))
    _logger.debug('archive_res: ' + str(archive_res))
    _logger.info('Archived: ' + input_full_path + '--------------' +
                 updated_input_path + '--------------' + output_full_path)


def get_resource_type(labels: list):
    '''
    Get resource type by neo4j labels
    '''
    resources = ['File', 'TrashFile', 'Folder']
    for label in labels:
        if label in resources:
            return label
    return None


def get_zone(labels: list):
    '''
    Get resource type by neo4j labels
    '''
    zones = ['Greenroom', 'VRECore']
    for label in labels:
        if label in zones:
            return label
    return None


def get_relative_folder_path(current_node):
    '''
    must update source node first
    '''
    # caculate relative path
    input_nodes = get_connected_nodes(
        current_node['global_entity_id'], "input")
    input_nodes = [
        node for node in input_nodes if 'Folder' in node['labels']]
    input_nodes.sort(key=lambda f: f['folder_level'])
    folder_name_list = [node['name'] for node in input_nodes]
    destination_relative_path = os.sep.join(folder_name_list)
    destination_relative_path = destination_relative_path[:-1] \
        if destination_relative_path.endswith(os.sep) else destination_relative_path
    return destination_relative_path

def get_relative_folder_path_to_source_for_file(current_node, source_node):
    '''
    must update source node first
    '''
    path_relative_to_source_path = ''
    input_nodes = get_connected_nodes(
    current_node['global_entity_id'], "input")
    input_nodes = [
        node for node in input_nodes if 'Folder' in node['labels']]
    input_nodes.sort(key=lambda f: f['folder_level'])
    found_source_node = [
        node for node in input_nodes if node['global_entity_id'] == source_node['global_entity_id']]
    found_source_node = found_source_node[0] if len(
        found_source_node) > 0 else None
    # child nodes
    source_index = input_nodes.index(found_source_node)
    folder_name_list = [node['name']
        for node in input_nodes[source_index + 1:]]
    path_relative_to_source_path = os.sep.join(folder_name_list)
    return path_relative_to_source_path

def refresh_node(target: dict, new: dict):
    for k, v in new.items():
        target[k] = v
