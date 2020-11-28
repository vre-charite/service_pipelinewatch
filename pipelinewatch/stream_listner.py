from kubernetes import watch
from utils.meta_data_operations import store_file_meta_data
from utils.lineage_operations import create_lineage
from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory
import os, datetime
from enum import Enum
import requests

class StreamWatcher:
    def __init__(self, batch_api):
        self.name = "k8s_job_watch"
        self.watcher = watch.Watch()
        self.batch_api = batch_api
        self._logger = SrvLoggerFactory(self.name).get_logger()
        self.__logger_debug = SrvLoggerFactory(self.name + "_debug").get_logger()
    def _watch_callback(self, job):
        job_name = job.metadata.name
        pipeline = job.metadata.labels['pipeline']
        def job_filter(job):
            active_pods = job.status.active
            return (active_pods == 0 or active_pods == None)
        if job_filter(job):
            self.__logger_debug.debug('ended job_name: ' + job_name + " pipeline: " + pipeline)
            if pipeline == EPipelineName.dicom_edit.name:
                my_final_status = 'failed' if job.status.failed else 'succeeded'
                self.__logger_debug.debug(job_name + ": " + my_final_status)
                if my_final_status == 'succeeded':
                    annotations = job.spec.template.metadata.annotations
                    self._on_dicom_eidt_succeed(pipeline, job_name, annotations)
                    self.__delete_job(job_name)
                else:
                    self._logger.warning("Terminating creating metadata")
            elif pipeline == EPipelineName.data_transfer.name:
                my_final_status = 'failed' if job.status.failed else 'succeeded'
                self.__logger_debug.debug(job_name + ": " + my_final_status)
                if my_final_status == 'succeeded':
                    annotations = job.spec.template.metadata.annotations
                    self._on_data_transfer_succeed(job_name, annotations)
                    self.__delete_job(job_name)
                else:
                    self._logger.warning("Terminating creating metadata")
        else:
            self._logger.info(job_name + " runnning...")
    def _on_dicom_eidt_succeed(self, pipeline, job_name, annotations):
        self.__logger_debug.debug('_on_dicom_eidt_succeed Triggered----' + datetime.datetime.now().isoformat())
        input_path = annotations['input_file']
        output_path = annotations['output_path']
        decoded_input_path = input_path.split('/')
        bucket_name = decoded_input_path[3]
        raw_file_name = decoded_input_path[5]
        split_raw_file_name = os.path.splitext(raw_file_name)
        file_name = split_raw_file_name[0] + '_edited' + split_raw_file_name[1]
        file_path = output_path + "/" + file_name
        generate_id = "undefined"
        unix_process_time = datetime.datetime.utcnow().timestamp()
        if ConfigClass.generate_project_process_file_folder in file_path:
            generate_id = file_name[0:8]
        processed_file_size = 0
        try:
            processed_file_size = os.path.getsize(file_path)
        except Exception as e:
            self._logger.warning("Error when getting file size")
            self._logger.warning(str(e))
            self._logger.warning("Terminating creating metadata")
        if processed_file_size > 0:
            if self.__check_valid_creation(file_path):
                self.__logger_debug.debug("action triggered: " + datetime.datetime.now().isoformat())
                store_file_meta_data(
                    file_path,
                    bucket_name,
                    file_name,
                    input_path,
                    processed_file_size,
                    pipeline,
                    job_name,
                    "succeeded",
                    generate_id
                )
                create_lineage(
                    input_path,
                    file_path,
                    bucket_name,
                    pipeline,
                    'Pipeline Processed',
                    unix_process_time
                )
            else:
                self._logger.debug('Skip Lineage Creation, redundant messages')
    def _on_data_transfer_succeed(self, job_name, annotations):
        self._logger.info(annotations)
        input_full_path = annotations["input_path"]
        output_full_path = annotations["output_path"]
        output_file_name = os.path.basename(output_full_path)
        project_code = annotations["project"] if annotations.get("project") else ConfigClass.tvb_project_code
        unix_process_time = datetime.datetime.utcnow().timestamp()
        processed_file_size = 0
        try:
            processed_file_size = os.path.getsize(output_full_path)
        except Exception as e:
            self._logger.error("Error when getting file size")
            self._logger.error(str(e))
            self._logger.error("Terminating creating metadata")
        if self.__check_valid_creation(output_full_path):
            store_file_meta_data(
                output_full_path,
                project_code,
                output_file_name,
                input_full_path,
                processed_file_size,
                EPipelineName.data_transfer.name,
                job_name,
                "succeeded"
            )
            self._logger.debug('Saved meta')
            create_lineage(
                input_full_path,
                output_full_path,
                project_code,
                EPipelineName.data_transfer.name,
                'K8s Processed',
                unix_process_time
            )
            self._logger.debug('Created Lineage')
        else:
            self._logger.debug('Skip Lineage Creation, redundant messages')
    def __delete_job(self , job_name):
        try:
            dele_res = self.batch_api.delete_namespaced_job(
                job_name, ConfigClass.k8s_namespace, propagation_policy="Foreground")
            self._logger.info(dele_res)
            self._logger.info("Deleted job: " + job_name)
        except Exception as e:
            self._logger.error(str(e))
    def __check_valid_creation(self, processed_full_path):
        url = ConfigClass.service_cateloguing + "/v1/entity/basic"
        payload = {
            "excludeDeletedEntities": True,
            "includeSubClassifications": False,
            "includeSubTypes": False,
            "includeClassificationAttributes": False,
            "entityFilters": {
                "condition": "AND",
                "criterion": [
                    {
                        "attributeName": "name",
                        "attributeValue": processed_full_path,
                        "operator": "eq"
                    }
                ]
            },
            "tagFilters": None,
            "attributes": [
                "owner",
                "downloader",
                "fileName"
            ],
            "limit": 10,
            "offset": "0",
            "sortBy": "createTime",
            "sortOrder": "DESCENDING",
            "typeName": "nfs_file_processed",
            "classification": None,
            "termName": None
        }
        response = requests.post(
            url=url,
            json=payload
        )
        if response.status_code == 200:
            data = response.json()
            self.__logger_debug.debug( "get entity: " + str(data))
            self._logger.debug('[check_valid_creation_lineage] entity found: ' + str(data['result'].get('entities', None)))
            if data['result'].get('entities', None):
                self._logger.debug('[check_valid_creation_lineage] invalid redundant creation')
                return False
        self._logger.debug('[check_valid_creation_lineage] good to go')
        return True
    def __get_stream(self):
        stream = self.watcher.stream(self.batch_api.list_namespaced_job, ConfigClass.k8s_namespace)
        return stream
    def run(self):
        self._logger.info('Start Pipeline Job Stream Watching')
        stream = self.__get_stream()
        for event in stream:
            self.__logger_debug.debug(str(event))
            event_type = event['type']
            job = event['object']
            finalizers = event['object'].metadata.finalizers
            self._logger.info('Event Triggered: ' + event_type + " - " + job.metadata.name)
            if event_type == PipelineJobEvent.MODIFIED.name and not finalizers:
                self._watch_callback(job)
            else:
                if event_type == PipelineJobEvent.MODIFIED.name and finalizers:
                    self._logger.info("Pre-Deleting.......................")
                self._logger.info("Ingnored Event, Skip Watch Callback.")
            self._logger.info("Event Process Done.")

class PipelineJobEvent(Enum):
    ADDED = 0,
    MODIFIED = 1,
    DELETED = 2

class EPipelineName(Enum):
    dicom_edit = 0,
    data_transfer = 1,