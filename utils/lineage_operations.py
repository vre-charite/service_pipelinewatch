from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory
import requests

def create_lineage_v2(inputFullPath, outputFullPath, projectCode, pipelineName, description, create_time):
    '''
    create lineage
    payload = {
        "inputFullPath": inputFullPath,
        "outputFullPath": outputFullPath,
        "projectCode": projectCode,
        "pipelineName": pipelineName,
        "description": description,
    }
    '''
    _logger = SrvLoggerFactory('stream_watcher').get_logger()
    my_url = ConfigClass.service_cateloguing
    payload = {
        "inputFullPath": inputFullPath,
        "outputFullPath": outputFullPath,
        "projectCode": projectCode,
        "pipelineName": pipelineName,
        "description": description,
        'process_timestamp': create_time
    }
    _logger.debug("Creating Lineage V2: " + str(payload))
    res = requests.post(
            url=my_url + '/v2/lineage',
            json=payload
    )
    if res.status_code == 200:
        _logger.info('Lineage Created: ' + inputFullPath + ':to:' + outputFullPath)
        return res.json()
    else:
        _logger.error(res.text)
        raise(Exception(res.text))

def create_lineage(inputFullPath, outputFullPath, projectCode, pipelineName, description, create_time):
    '''
    create lineage
    payload = {
        "inputFullPath": inputFullPath,
        "outputFullPath": outputFullPath,
        "projectCode": projectCode,
        "pipelineName": pipelineName,
        "description": description,
    }
    '''
    _logger = SrvLoggerFactory('stream_watcher').get_logger()
    my_url = ConfigClass.service_cateloguing
    payload = {
        "inputFullPath": inputFullPath,
        "outputFullPath": outputFullPath,
        "projectCode": projectCode,
        "pipelineName": pipelineName,
        "description": description,
        'process_timestamp': create_time
    }
    _logger.debug("Creating Lineage: " + str(payload))
    res = requests.post(
            url=my_url + '/v1/lineage',
            json=payload
    )
    if res.status_code == 200:
        _logger.info('Lineage Created: ' + inputFullPath + ':to:' + outputFullPath)
        return res.json()
    else:
        _logger.error(res.text)
        raise(Exception(res.text))