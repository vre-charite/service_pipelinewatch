# Copyright 2022 Indoc Research
# 
# Licensed under the EUPL, Version 1.2 or – as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
# 
# https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
# 
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
# 

from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory
import requests

def create_lineage_v3(input_geid, output_geid, projectCode, pipelineName, description, create_time):
    '''
    create lineage
    payload = {
        "input_geid": "string",
        "output_geid": "string",
        "project_code": "string",
        "pipeline_name": "string",
        "description": "string"
    }
    '''
    _logger = SrvLoggerFactory('stream_watcher').get_logger()
    my_url = ConfigClass.PROVENANCE_SERVICE
    payload = {
        "input_geid": input_geid,
        "output_geid": output_geid,
        "project_code": projectCode,
        "pipeline_name": pipelineName,
        "description": description
    }
    _logger.debug("Creating Lineage V3: " + str(payload))
    res = requests.post(
            url=my_url + 'lineage',
            json=payload
    )
    if res.status_code == 200:
        _logger.info('Lineage Created: ' + input_geid + ':to:' + output_geid)
        return res.json()
    else:
        _logger.error(res.text)
        raise(Exception(res.text))


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
    my_url = ConfigClass.CATALOGUING_SERVICE_V2
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
            url=my_url + 'lineage',
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
    my_url = ConfigClass.CATALOGUING_SERVICE_V1
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
            url=my_url + 'lineage',
            json=payload
    )
    if res.status_code == 200:
        _logger.info('Lineage Created: ' + inputFullPath + ':to:' + outputFullPath)
        return res.json()
    else:
        _logger.error(res.text)
        raise(Exception(res.text))
