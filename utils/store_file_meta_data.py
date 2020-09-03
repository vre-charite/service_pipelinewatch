import requests
from config import ConfigClass
import json
from services.logger_services.logger_factory_service import SrvLoggerFactory

def store_file_meta_data(path, bucket_name, file_name, raw_file_path, size, pipeline, job_name, status):
    _logger = SrvLoggerFactory('stream_watcher').get_logger()
    my_url = ConfigClass.data_ops_host
    payload  = {
        "path": path,
        "bucket_name": bucket_name,
        "file_name": file_name,
        "raw_file_path": raw_file_path,
        "size": size,
        "pipeline": pipeline,
        "job_name": job_name,
        "status": status
    }
    _logger.info("Saving Meta: " + str(payload))
    res = requests.post(
            url=my_url + "/v1/files/processed",
            json=payload
    )
    _logger.info('Meta Saved: ' + file_name + "  result: " + res.text)
    return json.loads(res.text)