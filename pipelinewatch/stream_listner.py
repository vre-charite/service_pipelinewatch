from kubernetes import watch
from utils.meta_data_operations import store_file_meta_data, \
    add_copied_with_approval, store_file_meta_data_v2, archive_file_data
from utils.lineage_operations import create_lineage, create_lineage_v2
from utils.file_opertion_status import update_file_operation_status, EDataActionType, update_file_operation_logs
from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory
from services.file_meta.vr_folders_mgr import SrvVRMgr
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
        try:
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
                elif pipeline == EPipelineName.data_delete.name:
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    self.__logger_debug.debug(job_name + ": " + my_final_status)
                    if my_final_status == 'succeeded':
                        annotations = job.spec.template.metadata.annotations
                        self._on_file_delete_succeed(pipeline, job_name, annotations)
                        self.__delete_job(job_name)
                    else:
                        self._logger.warning("Terminating creating metadata")
                else:
                    self._logger.warning("Unknow pipeline job: " + pipeline)
            else:
                self._logger.info(job_name + " runnning...")
        except Exception as expe:
            self._logger.error("Internal Error: " + str(expe))
    def _on_dicom_eidt_succeed(self, pipeline, job_name, annotations):
        self.__logger_debug.debug('_on_dicom_eidt_succeed Triggered----' + datetime.datetime.now().isoformat())
        input_path = annotations['input_file']
        output_path = annotations['output_path']
        uploader = annotations.get("uploader", 'admin')
        decoded_input_path = input_path.split('/')
        bucket_name = decoded_input_path[3]
        raw_file_name = decoded_input_path[5]
        split_raw_file_name = os.path.splitext(raw_file_name)
        file_name = split_raw_file_name[0] + '_edited' + split_raw_file_name[1]
        file_path = output_path + "/" + file_name
        generate_id = "undefined"
        unix_process_time = datetime.datetime.utcnow().timestamp()
        zone = "greenroom"
        if ConfigClass.generate_project_process_file_folder in file_path:
            generate_id = annotations.get("event_payload_generate_id")
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
                # v2 API
                from_parents = {
                    "full_path": input_path,
                }
                store_file_meta_data_v2(
                    uploader,
                    file_name,
                    output_path,
                    processed_file_size,
                    'processed by dicom_edit',
                    zone,
                    bucket_name,
                    [],
                    generate_id,
                    'auto_trigger',
                    from_parents=from_parents,
                    process_pipeline=pipeline)
                create_lineage_v2(
                    input_path,
                    file_path,
                    bucket_name,
                    pipeline,
                    'dicom_edit Processed',
                    unix_process_time)
            else:
                self._logger.debug('Skip Lineage Creation, redundant messages')
    def _on_data_transfer_succeed(self, job_name, annotations):
        self._logger.info("_on_data_transfer_succeed Event triggered")
        self._logger.info(annotations)
        input_full_path = annotations["input_path"]
        output_full_path = annotations["output_path"]
        output_file_name = os.path.basename(output_full_path)
        output_path = os.path.dirname(output_full_path)
        project_code = annotations["project"] if annotations.get("project") else ConfigClass.tvb_project_code
        generate_id = annotations.get("generate_id", "undefined")
        uploader = annotations.get("uploader")
        session_id = annotations.get('event_payload_session_id', 'default_session')
        job_id = annotations.get('event_payload_job_id', 'default_job')
        operator = annotations.get('event_payload_operator', 'admin')
        operation_type = annotations.get('event_payload_operation_type', 0) 
        self._logger.debug("_on_data_transfer_succeed generate_id: " + generate_id)
        self._logger.debug("_on_data_transfer_succeed annotations: " + str(annotations))
        unix_process_time = datetime.datetime.utcnow().timestamp()
        processed_file_size = 0
        zone = "greenroom" if ConfigClass.data_lake in output_full_path else 'vrecore'
        try:
            processed_file_size = os.path.getsize(output_full_path)
        except Exception as e:
            self._logger.error("Error when getting file size")
            self._logger.error(str(e))
            self._logger.error("Terminating creating metadata")
        try:
            ## Saving metadata
            if self.__check_valid_creation(output_full_path):
                # v2 API
                from_parents = {
                    "full_path": input_full_path,
                }
                store_file_meta_data_v2(
                    uploader,
                    output_file_name,
                    output_path,
                    processed_file_size,
                    'processed by data_transfer',
                    zone,
                    project_code,
                    [],
                    generate_id,
                    operator,
                    from_parents=from_parents,
                    process_pipeline=EPipelineName.data_transfer.name)
                self._logger.debug('Saved meta v2')
                create_lineage_v2(
                    input_full_path,
                    output_full_path,
                    project_code,
                    EPipelineName.data_transfer.name,
                    'data_transfer Processed',
                    unix_process_time)
                self._logger.debug('Created Lineage v2')
                if str(operation_type) == '1': ## type copy to vre core raw
                    add_copied_with_approval(self._logger, input_full_path, project_code)
                res_update_status = update_file_operation_status(session_id, job_id, EDataActionType.data_transfer.name,
                    project_code, operator, input_full_path, zone)
                self._logger.debug('res_update_status: ' + str(res_update_status.status_code))
                res_update_audit_logs = update_file_operation_logs(
                    uploader,
                    operator,
                    input_full_path,
                    output_full_path,
                    processed_file_size,
                    project_code,
                    generate_id
                )
                self._logger.debug('res_update_audit_logs: ' + str(res_update_audit_logs.status_code))
            else:
                self._logger.debug('Skip Lineage Creation, redundant messages')
        except Exception as e:
            self._logger.error("Error when creating metadata: " + str(e))
    def _on_file_delete_succeed(self, pipeline, job_name, annotations):
        self._logger.info("_on_file_delete_succeed Event triggered")
        input_full_path = annotations["event_payload_input_path"]
        input_file_name = os.path.basename(input_full_path)
        input_path = os.path.dirname(input_full_path)
        output_full_path = annotations["event_payload_output_path"]
        output_file_name = os.path.basename(output_full_path)
        output_path = os.path.dirname(output_full_path)
        project_code = annotations["event_payload_project"]
        session_id = annotations.get('event_payload_session_id', 'default_session')
        job_id = annotations.get('event_payload_job_id', 'default_job')
        operator = annotations.get('event_payload_operator', '')
        uploader = annotations.get('event_payload_uploader', '')
        generate_id = annotations.get('event_payload_generate_id', '')
        unix_process_time = datetime.datetime.utcnow().timestamp()
        processed_file_size = 0
        myfilename, file_extension = os.path.splitext(output_file_name)
        update_file_name_suffix = myfilename.split("_")[1]
        updated_file_name = output_file_name
        updated_input_path = input_path + "/" + updated_file_name
        zone = "greenroom" if ConfigClass.data_lake in output_full_path else 'vrecore'
        self._logger.debug("_on_file_delete_succeed annotations: " + str(annotations))
        try:
            processed_file_size = os.path.getsize(output_full_path)
        except Exception as e:
            self._logger.error("Error when getting file size")
            self._logger.error(str(e))
            self._logger.error("Terminating creating metadata")
        ## archive virtual files
        vr_mgr = SrvVRMgr()
        res_delete_vr_files_by_full_path = vr_mgr.delete_vr_files_by_full_path(input_full_path)
        self._logger.debug("delete_vr_files_by_full_path res: " + str(res_delete_vr_files_by_full_path))
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
        self._logger.debug('res_update_status: ' + str(res_update_status.status_code))
        res_update_audit_logs = update_file_operation_logs(
            uploader,
            operator,
            input_full_path,
            output_full_path,
            processed_file_size,
            project_code,
            generate_id,
            EDataActionType.data_delete.name
        )
        self._logger.debug('res_update_audit_logs: ' + str(res_update_audit_logs.status_code))
        self._logger.debug('archive_res: ' + str(archive_res))
        self._logger.info('Archived: ' + input_full_path + '--------------' + updated_input_path + '--------------' + output_full_path)
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
            # self.__logger_debug.debug(str(event))
            event_type = event['type']
            job = event['object']
            finalizers = event['object'].metadata.finalizers
            # self._logger.debug('Event Triggered: ' + event_type + " - " + job.metadata.name)
            if event_type == PipelineJobEvent.MODIFIED.name and not finalizers:
                self._watch_callback(job)
            else:
                if event_type == PipelineJobEvent.MODIFIED.name and finalizers:
                    pass
                    # self._logger.debug("Pre-Deleting.......................")
                # self._logger.debug("Ingnored Event, Skip Watch Callback.")
            # self._logger.debug("Event Process Done.")

class PipelineJobEvent(Enum):
    ADDED = 0,
    MODIFIED = 1,
    DELETED = 2

class EPipelineName(Enum):
    dicom_edit = 0,
    data_transfer = 1,
    data_delete = 200
