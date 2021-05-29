from posixpath import join
from config import ConfigClass
import os
import datetime
from copy import copy
from services.file_meta.file_data_mgr import http_query_node
from utils.meta_data_operations import get_folder_node_bypath, store_file_meta_data_v2, add_copied_with_approval, get_resource_bygeid, \
    get_connected_nodes, fetch_geid, create_folder_node, http_update_node
from utils.lineage_operations import create_lineage_v2
from utils.file_opertion_status import update_file_operation_logs, update_file_operation_status, EDataActionType
from models.enum_and_events import EPipelineName


def on_data_transfer_succeed(_logger, annotations):
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
            on_folder_transferred(_logger, annotations, source_node)
    except Exception as e:
        _logger.error("Error when creating metadata: " + str(e))
        if ConfigClass.env == 'test':
            raise
        raise Exception("[Internal] Error when creating metadata: " + str(e))


def on_folder_transferred(_logger, annotations, source_node):
    '''
    when transferred a folder
    '''
    def is_parent(folder, connected):
        return folder['folder_relative_path'] \
            == os.path.join(connected['folder_relative_path'], connected['name'])
    # default is None
    destination_geid = annotations['event_payload_destination_geid']
    destination = None
    source_geid = source_node['global_entity_id']
    input_full_path = annotations["input_path"]
    output_full_path = annotations["output_path"]
    output_folder_name = os.path.basename(
        output_full_path)  # test_copy_rename_21
    source_node['output_folder_name'] = output_folder_name
    project_code = annotations["project"]
    uploader = annotations.get("uploader")
    session_id = annotations.get('event_payload_session_id', 'default_session')
    job_id = annotations.get('event_payload_job_id', 'default_job')
    operator = annotations.get('event_payload_operator', 'admin')
    # generate meta information
    unix_process_time = datetime.datetime.utcnow().timestamp()
    # get connected
    connected_nodes = get_connected_nodes(source_geid)
    # folder node creation
    fnodes_created = []
    # get destination
    if destination_geid and destination_geid != 'None':
        destination = get_resource_bygeid(destination_geid)

    def wrapper_create_mirror_folder(fnode_to_mirror: dict, start_level=0, parent_node=None):
        # create mirror folder nodes
        new_geid = fetch_geid()
        new_folder_level = fnode_to_mirror['folder_level'] - \
            source_node['folder_level'] + start_level  # m-n+o
        fnode_created = create_folder_node(
            zone="vrecore",
            geid=new_geid,
            name=fnode_to_mirror['output_folder_name'] if fnode_to_mirror.get(
                'output_folder_name') else fnode_to_mirror['name'],
            level=new_folder_level,
            project_code=project_code,
            uploader=fnode_to_mirror['uploader'],
            relative_path=fnode_to_mirror["new_relative_path"],
            tags=fnode_to_mirror['tags'],
            parent_geid=parent_node['global_entity_id'] if parent_node else "",
            parent_name=parent_node['name'] if parent_node else ""
        )
        fnode_created['original_geid'] = fnode_to_mirror['global_entity_id']
        fnode_created['original_relative_path'] = fnode_to_mirror['folder_relative_path']
        fnode_created['original_name'] = fnode_to_mirror['name']
        # # update extra attibutes
        # update_json = {}
        # for k, v in fnode_to_mirror.items():
        #     if not k in fnode_created:
        #         update_json[k] = v
        # updated_fnode_created = http_update_node(
        #     "Folder", fnode_created['id'], update_json=update_json)
        # res = updated_fnode_created.json()[0]
        # refresh_node(fnode_created, res)
        fnodes_created.append(fnode_created)
        return fnode_created
    fnodes_connected = [
        node for node in connected_nodes if 'Folder' in node['labels']]
    fnodes_children = [node for node in fnodes_connected
                       if node['folder_level'] > source_node['folder_level'] and node.get('archived') != True]
    # update source node relative path and parent
    source_node['new_relative_path'] = os.path.join(
        destination['folder_relative_path'], destination['name']) \
        if destination and 'Folder' in destination['labels'] else ''
    # update parent folder node (use destination folder node)
    source_node['parent_node'] = destination if destination and 'Folder' in destination['labels'] else None
    start_level = source_node['parent_node']['folder_level'] + 1 if source_node.get(
        'parent_node') else 0
    # update child fnodes relative path
    for node in fnodes_children:
        def get_new_relative_path(current_node):
            root_destination_path = os.path.join(
                destination['folder_relative_path'], destination['name'], output_folder_name) \
                if destination and 'Folder' in destination['labels'] else output_folder_name
            input_nodes = get_connected_nodes(
                current_node['global_entity_id'], "input")
            input_nodes = [
                node for node in input_nodes if 'Folder' in node['labels']]
            input_nodes.sort(key=lambda f: f['folder_level'])
            found_source_node = [
                node for node in input_nodes if node['global_entity_id'] == source_node['global_entity_id']]
            found_source_node = found_source_node[0] if len(
                found_source_node) > 0 else None
            path_relative_to_source_path = ''
            if found_source_node:
                # child nodes
                source_index = input_nodes.index(found_source_node)
                folder_name_list = [node['name']
                                    for node in input_nodes[source_index + 1:]]
                path_relative_to_source_path = os.sep.join(folder_name_list)
            destination_relative_full_path: str = os.path.join(
                root_destination_path, path_relative_to_source_path)
            destination_relative_full_path = destination_relative_full_path[:-1] \
                if destination_relative_full_path.endswith(os.sep) else destination_relative_full_path
            return destination_relative_full_path
        node["new_relative_path"] = get_new_relative_path(node)
        _logger.debug("new_relative_path: " + node["new_relative_path"])
    fnodes_to_mirror = fnodes_children + [source_node]
    fnodes_to_mirror.sort(key=lambda f: f['folder_level'])
    for fnode_to_mirror in fnodes_to_mirror:
        # find parent
        fnode_original_parent = [
            connected_node for connected_node in fnodes_to_mirror
            if is_parent(fnode_to_mirror, connected_node)]
        fnode_original_parent = fnode_original_parent[0] if len(
            fnode_original_parent) > 0 else None
        # get parent folder node in VRECore
        parent_gotten = None
        if fnode_original_parent:
            parent_folder_name = fnode_original_parent['output_folder_name'] if fnode_original_parent.get(
                'output_folder_name') else fnode_original_parent['name']
            _logger.debug({
                "param1": "VRECore",
                "param2": fnode_original_parent['project_code'],
                "param3": fnode_original_parent['new_relative_path'],
                "param4": parent_folder_name
            })
            parent_gotten = get_folder_node_bypath(
                "VRECore", fnode_original_parent['project_code'], fnode_original_parent['new_relative_path'], parent_folder_name)
            _logger.debug("parent_gotten: {}".format(str(parent_gotten)))
        wrapper_create_mirror_folder(
            fnode_to_mirror,
            start_level=start_level,
            parent_node=fnode_to_mirror.get('parent_node', parent_gotten))
        tags = fnode_to_mirror.get('tags', [])
        tags.append(ConfigClass.copied_with_approval)
        tags = list(set(tags))
        # add copy with approval
        http_update_node('Folder', fnode_to_mirror['id'], {'tags': tags})
    # file node creation
    file_nodes = [node for node in connected_nodes if 'File' in node['labels'] and node.get('archived') != True]
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
            file_input_full_path = os.path.join(
                "/", ConfigClass.NFS_ROOT_PATH, project_code, 'raw', original_relative_folder_full_path, file_node['name'])
            file_output_full_path = os.path.join(
                "/", ConfigClass.VRE_ROOT_PATH, project_code, relative_folder_full_path, file_node['name'])
            from_parents = {
                "full_path": file_input_full_path,
                "original_geid": file_node['global_entity_id']
            }
            file_node_stored = store_file_meta_data_v2(
                file_node['uploader'],
                file_node['name'],
                os.path.dirname(file_output_full_path),
                file_node.get('file_size', 0),
                'processed by data_transfer',
                'vrecore',
                project_code,
                file_node.get('tags', []),
                file_node['generate_id'],
                operator,
                from_parents=from_parents,
                process_pipeline=EPipelineName.data_transfer.name,
                parent_folder_geid=parent_folder_node['global_entity_id']
            )
            _logger.debug('Saved meta v2: ' + file_output_full_path)
            # update extra attibutes
            update_json = {}
            for k, v in file_node.items():
                if not k in file_node_stored:
                    update_json[k] = v
            updated_node = http_update_node(
                "File", file_node_stored['id'], update_json=update_json)
            res = updated_node.json()[0]
            refresh_node(file_node_stored, res)
            create_lineage_v2(
                file_input_full_path,
                file_output_full_path,
                project_code,
                EPipelineName.data_transfer.name,
                'Transfer job: {}'.format(job_id),
                datetime.datetime.utcnow().timestamp())
            _logger.debug('Created Lineage v2: ' + '--------------{}------------{}-----------------'.format(
                file_input_full_path, file_output_full_path))
            add_copied_with_approval(
                _logger, file_input_full_path, project_code)
            res_update_audit_logs = update_file_operation_logs(
                uploader,
                operator,
                file_input_full_path,
                file_output_full_path,
                0,
                project_code,
                "undefined"
            )
            _logger.debug('res_update_audit_logs: ' +
                          str(res_update_audit_logs.status_code))
    res_update_status = update_file_operation_status(session_id, job_id, EDataActionType.data_transfer.name,
                                                     project_code, operator, input_full_path, "vrecore")
    _logger.debug('res_update_status: ' +
                  str(res_update_status.status_code))
    _logger.info('Transfer succeed.')


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
    generate_id = annotations.get("generate_id", "undefined")
    uploader = annotations.get("uploader")
    session_id = annotations.get('event_payload_session_id', 'default_session')
    job_id = annotations.get('event_payload_job_id', 'default_job')
    operator = annotations.get('event_payload_operator', 'admin')
    # generate meta information
    unix_process_time = datetime.datetime.utcnow().timestamp()
    labels = source_node['labels']
    zone = 'vrecore'
    # Saving metadata
    # v2 API
    from_parents = {
        "full_path": input_full_path,
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
        source_node.get('tags', []),
        generate_id,
        operator,
        from_parents=from_parents,
        process_pipeline=EPipelineName.data_transfer.name,
        parent_folder_geid=destination_geid if destination and 'Folder' in destination['labels'] else None)
    # update extra attibutes
    update_json = {}
    for k, v in source_node.items():
        if not k in file_node_stored:
            update_json[k] = v
    updated_file_node_stored = http_update_node(
        "File", file_node_stored['id'], update_json=update_json)
    res = updated_file_node_stored.json()[0]
    refresh_node(source_node, res)
    _logger.debug('Saved meta v2')
    create_lineage_v2(
        input_full_path,
        output_full_path,
        project_code,
        EPipelineName.data_transfer.name,
        'data_transfer Processed',
        unix_process_time)
    _logger.debug('Created Lineage v2')
    add_copied_with_approval(
        _logger, input_full_path, project_code)
    res_update_status = update_file_operation_status(session_id, job_id, EDataActionType.data_transfer.name,
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
        generate_id
    )
    _logger.debug('res_update_audit_logs: ' +
                  str(res_update_audit_logs.status_code))


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
