import requests
from enum import Enum
from config import ConfigClass

def update_file_operation_status(session_id, job_id, action_type, project_code, operator, source):
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
        "progress": "100"
    }
    res_update_status = requests.post(
        url,
        json=payload
    )
    return res_update_status

def update_file_operation_logs(owner, operator, input_file_path, output_file_path, file_size, project_code, generate_id):
    '''
    Endpoint
    /v1/file/actions/logs
    '''
    url = ConfigClass.data_ops_host + '/v1/file/actions/logs'
    payload = {
        "operation_type": "data_transfer",
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
    return res_update_file_operation_logs

class EDataActionType(Enum):
    data_transfer = 0