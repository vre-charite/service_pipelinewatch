import requests
import json
from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory

def send_to_queue(path_name):
    _logger = SrvLoggerFactory('stream_watcher').get_logger()
    url = ConfigClass.service_queue_send_msg_url
    my_data = path_name
    _logger.info("Sending Message To Queue: " + path_name)
    res = requests.post(
        url=url,
        data=my_data,
        headers={'Content-type': 'text/plain; charset=utf-8'}
    )
    _logger.info(res.text)
    return json.loads(res.text)
