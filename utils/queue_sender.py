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

import requests
import json
from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory

def send_to_queue(path_name):
    _logger = SrvLoggerFactory('stream_watcher').get_logger()
    url = ConfigClass.QUEUE_SERVICE + "send_message"
    my_data = path_name
    _logger.info("Sending Message To Queue: " + path_name)
    res = requests.post(
        url=url,
        data=my_data,
        headers={'Content-type': 'text/plain; charset=utf-8'}
    )
    _logger.info(res.text)
    return json.loads(res.text)
