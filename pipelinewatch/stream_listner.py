from kubernetes import watch
from utils.store_file_meta_data import store_file_meta_data
from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory
import os
from enum import Enum

class StreamWatcher:
    def __init__(self, batch_api):
        self.watcher = watch.Watch()
        self.batch_api = batch_api
        self._logger = SrvLoggerFactory('stream_watcher').get_logger()
    def _watch_callback(self, job):
        job_name = job.metadata.name
        pipeline = job.metadata.labels['pipeline']
        def job_filter(job):
            active_pods = job.status.active
            return (active_pods == 0 or active_pods == None)
        if job_filter(job):
            my_final_status = 'failed' if job.status.failed else 'succeeded'
            self._logger.info(job_name + ": " + my_final_status)
            annotations = job.spec.template.metadata.annotations
            input_path = annotations['input_file']
            output_path = annotations['output_path']
            decoded_input_path = input_path.split('/')
            bucket_name = decoded_input_path[3]
            raw_file_name = decoded_input_path[5]
            split_raw_file_name = os.path.splitext(raw_file_name)
            file_name = split_raw_file_name[0] + '_edited' + split_raw_file_name[1]
            file_path = output_path + "/" + file_name
            processed_file_size = 0
            if my_final_status == 'succeeded':
                try:
                    processed_file_size = os.path.getsize(file_path)
                except Exception as e:
                    self._logger.warning("Error when getting file size")
                    self._logger.warning(str(e))
                    self._logger.warning("Terminating creating metadata")
                if processed_file_size > 0:
                    self._logger.info("pipeline: " + pipeline)
                    self._logger.info("input_path: " + input_path)
                    self._logger.info("output_path: " + output_path)
                    self._logger.info("file_name: " + file_name)
                    self._logger.info("file size: " + str(processed_file_size))
                    store_file_meta_data(
                        file_path,
                        bucket_name,
                        file_name,
                        input_path,
                        processed_file_size,
                        pipeline,
                        job_name,
                        my_final_status
                    )
                try:
                    dele_res = self.batch_api.delete_namespaced_job(job_name, ConfigClass.k8s_namespace, propagation_policy="Foreground")
                    self._logger.info(dele_res)
                    self._logger.info("Deleted job: " + job_name)
                except Exception as e:
                    self._logger.error(str(e))
            else:
                self._logger.warning("Terminating creating metadata")
        else:
            self._logger.info(job_name + " runnning...")
    def _get_stream(self):
        stream = self.watcher.stream(self.batch_api.list_namespaced_job, ConfigClass.k8s_namespace)
        return stream
    def run(self):
        self._logger.info('Start Pipeline Job Stream Watching')
        stream = self._get_stream()
        for event in stream:
            # self._logger.info(event)
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
            self._logger.info("-----------------------------------------")

class PipelineJobEvent(Enum):
    ADDED = 0,
    MODIFIED = 1,
    DELETED = 2