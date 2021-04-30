import requests, os
from enum import Enum
from config import ConfigClass

def get_frontend_zone(my_disk_namespace: str):
    '''
    disk namespace to path
    '''
    return {
        "greenroom": "Green Room",
        "vre": "VRE Core",
        "vrecore": "VRE Core"
    }.get(my_disk_namespace, None)

def update_file_operation_status(session_id, job_id, action_type, project_code, operator, source, zone):
    '''
    Endpoint
    http://10.3.7.234:5063/v1/file/actions/status
    '''
    url = ConfigClass.data_ops_host + '/v1/file/actions/status'
    payload = {
        "session_id": session_id,
        "job_id": job_id,
        "source": source,
        "action": action_type,
        "target_status": 'succeed',
        "project_code": project_code,
        "operator": operator,
        "progress": "100",
        "payload": {
            "zone": zone,
            "frontend_zone": get_frontend_zone(zone)
        }
    }
    res_update_status = requests.post(
        url,
        json=payload
    )
    return res_update_status

def update_file_operation_logs(owner, operator, input_file_path, output_file_path,
    file_size, project_code, generate_id, operation_type="data_transfer", extra=None):
    '''
    Endpoint
    /v1/file/actions/logs
    '''
    url = ConfigClass.data_ops_host + '/v1/file/actions/logs'
    payload = {
        "operation_type": operation_type,
        "owner": owner,
        "operator": operator,
        "input_file_path": input_file_path,
        "output_file_path": output_file_path,
        "file_size": file_size,
        "project_code": project_code,
        "generate_id": generate_id
    }
    res_update_file_operation_logs = requests.post(
        url,
        json=payload
    )
    # new audit log api
    url_audit_log = ConfigClass.PROVENANCE_SERVICE + '/v1/audit-logs'
    payload_audit_log = {
        "action": operation_type,
        "operator": operator,
        "target": input_file_path,
        "outcome": output_file_path,
        "resource": "file",
        "display_name": os.path.basename(input_file_path),
        "project_code": project_code,
        "extra": extra if extra else {}
    }
    res_audit_logs = requests.post(
        url_audit_log,
        json=payload_audit_log
    )
    return res_update_file_operation_logs

class EDataActionType(Enum):
    data_transfer = 0
    data_delete = 200