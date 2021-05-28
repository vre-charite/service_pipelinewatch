from kubernetes import watch
from utils.meta_data_operations import \
    store_file_meta_data_v2
from utils.lineage_operations import create_lineage_v2
from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory
import os
import datetime
from models.enum_and_events import PipelineJobEvent, EPipelineName
from .on_data_transfer import on_data_transfer_succeed
from .on_data_delete import on_data_delete_succeed
import requests


class StreamWatcher:
    def __init__(self, batch_api):
        self.name = "k8s_job_watch"
        self.watcher = watch.Watch()
        self.batch_api = batch_api
        self._logger = SrvLoggerFactory(self.name).get_logger()
        self.__logger_debug = SrvLoggerFactory(
            self.name + "_debug").get_logger()

    def _watch_callback(self, job):
        try:
            job_name = job.metadata.name
            pipeline = job.metadata.labels['pipeline']

            def job_filter(job):
                active_pods = job.status.active
                return (active_pods == 0 or active_pods == None)
            if job_filter(job):
                self.__logger_debug.debug(
                    'ended job_name: ' + job_name + " pipeline: " + pipeline)
                if pipeline == EPipelineName.dicom_edit.name:
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    self.__logger_debug.debug(
                        job_name + ": " + my_final_status)
                    if my_final_status == 'succeeded':
                        annotations = job.spec.template.metadata.annotations
                        self._on_dicom_eidt_succeed(
                            pipeline, job_name, annotations)
                        self.__delete_job(job_name)
                    else:
                        self._logger.warning("Terminating creating metadata")
                elif pipeline == EPipelineName.data_transfer.name:
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    self.__logger_debug.debug(
                        job_name + ": " + my_final_status)
                    if my_final_status == 'succeeded':
                        annotations = job.spec.template.metadata.annotations
                        on_data_transfer_succeed(self._logger, annotations)
                        self.__delete_job(job_name)
                    else:
                        self._logger.warning("Terminating creating metadata")
                elif pipeline == EPipelineName.data_delete.name:
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    self.__logger_debug.debug(
                        job_name + ": " + my_final_status)
                    if my_final_status == 'succeeded':
                        annotations = job.spec.template.metadata.annotations
                        on_data_delete_succeed(self._logger, annotations)
                        self.__delete_job(job_name)
                    else:
                        self._logger.warning("Terminating creating metadata")
                else:
                    self._logger.warning("Unknow pipeline job: " + pipeline)
            else:
                self._logger.info(job_name + " runnning...")
        except Exception as expe:
            self._logger.error("Internal Error: " + str(expe))
            if ConfigClass.env == 'test':
                raise

    def _on_dicom_eidt_succeed(self, pipeline, job_name, annotations):
        self.__logger_debug.debug(
            '_on_dicom_eidt_succeed Triggered----' + datetime.datetime.now().isoformat())
        input_path = annotations['input_file']

        try:
            # Get parent folder
            payload = {
                "label": "own",
                "start_label": "Folder",
                "end_label": "File",
                "end_params": {
                    "full_path": input_path,
                }
            }
            response = requests.post(
                ConfigClass.NEO4J_SERVICE + "relations/query", json=payload)
            if response.json():
                parent_folder_geid = response.json(
                )[0]["start_node"]["global_entity_id"]
            else:
                parent_folder_geid = None
        except Exception as e:
            self.__logger_debug.debug('Error getting parent_folder: ' + str(e))

        output_path = annotations['output_path']
        uploader = annotations.get("uploader", 'admin')
        decoded_input_path = input_path.split('/')
        bucket_name = decoded_input_path[3]
        raw_file_name = decoded_input_path[-1]
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
            self.__logger_debug.debug(
                "action triggered: " + datetime.datetime.now().isoformat())
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
                process_pipeline=pipeline,
                parent_folder_geid=parent_folder_geid
            )
            create_lineage_v2(
                input_path,
                file_path,
                bucket_name,
                pipeline,
                'dicom_edit Processed',
                unix_process_time)

    def __delete_job(self, job_name):
        try:
            dele_res = self.batch_api.delete_namespaced_job(
                job_name, ConfigClass.k8s_namespace, propagation_policy="Foreground")
            self._logger.info(dele_res)
            self._logger.info("Deleted job: " + job_name)
        except Exception as e:
            self._logger.error(str(e))

    def __get_stream(self):
        stream = self.watcher.stream(
            self.batch_api.list_namespaced_job, ConfigClass.k8s_namespace)
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
