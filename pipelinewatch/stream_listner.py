from kubernetes import watch
from utils.meta_data_operations import \
    store_file_meta_data_v2
from utils.lineage_operations import create_lineage_v3
from utils.archive import generate_zip_preview, save_preview
from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory
import os
import datetime
from models.enum_and_events import PipelineJobEvent, EPipelineName
from .on_data_transfer_v2 import on_data_transfer_succeed, on_data_transfer_failed
from .on_data_delete_v2 import on_data_delete_succeed, on_data_delete_failed
import requests
from models.minio_client import Minio_Client
from utils.meta_data_operations import location_decoder
from utils.file_opertion_status import unlock_resource


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
                        # self._on_dicom_edit_succeed(
                        #     pipeline, job_name, annotations)
                        self.__delete_job(job_name)
                    else:
                        self._logger.warning("Terminating creating metadata")
                elif pipeline == EPipelineName.data_transfer.name:
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    self.__logger_debug.debug(
                        job_name + ": " + my_final_status)
                    annotations = job.spec.template.metadata.annotations
                    input_full_path = annotations["input_path"]
                    output_full_path = annotations["output_path"]
                    project_code = annotations["project"]
                    gr_bucket = "gr-" + project_code
                    core_bucket = "core-" + project_code
                    input_key = os.path.join(gr_bucket, input_full_path)
                    output_key = os.path.join(core_bucket, output_full_path)
                    if my_final_status == 'succeeded':
                        on_data_transfer_succeed(self._logger, annotations)
                        self.__delete_job(job_name)
                    else:
                        on_data_transfer_failed(self._logger, annotations)
                        self._logger.warning("Terminating creating metadata")
                    # unlock resource
                    unlock_resource(input_key)
                    unlock_resource(output_key)
                elif pipeline == EPipelineName.data_delete.name:
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    self.__logger_debug.debug(
                        job_name + ": " + my_final_status)
                    annotations = job.spec.template.metadata.annotations
                    location = annotations["event_payload_input_path"]
                    ingestion_type, ingestion_host, ingestion_path = location_decoder(location)
                    if my_final_status == 'succeeded':
                        on_data_delete_succeed(self._logger, annotations)
                        self.__delete_job(job_name)
                    else:
                        on_data_delete_failed(self._logger, annotations)
                        self._logger.warning("Terminating creating metadata")
                    unlock_resource(ingestion_path)
                else:
                    self._logger.warning("Unknow pipeline job: " + pipeline)
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    if my_final_status == 'succeeded':
                        self.__delete_job(job_name)
            else:
                self._logger.info(job_name + " runnning...")
        except Exception as expe:
            self._logger.error("Internal Error: " + str(expe))
            if ConfigClass.env == 'test':
                raise

    def _on_dicom_edit_succeed(self, pipeline, job_name, annotations):
        self.__logger_debug.debug(
            '_on_dicom_edit_succeed Triggered----' + datetime.datetime.now().isoformat())
        input_location = annotations['input_file']

        try:
            # Get parent folder
            payload = {
                "label": "own",
                "start_label": "Folder",
                "end_label": "File",
                "end_params": {
                    "location": input_location,
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

        try:
            # Get input file node
            payload = {
                "location": input_location
            }
            response = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/File/query", json=payload)
            parent_node = response.json()[0]
            self.__logger_debug.debug(f'Got parent node {parent_node}')
        except Exception as e:
            error_msg = f"Error getting parent_node on dicom pipeline: {str(e)}"
            self.__logger_debug.debug(error_msg)
            raise Exception(error_msg)

        output_path = self.parse_minio_location(annotations['output_path'])["path"]
        uploader = annotations.get("uploader", 'admin')
        file_data = self.parse_minio_location(input_location)
        raw_file_name = file_data["path"].split('/')[-1]
        split_raw_file_name = os.path.splitext(raw_file_name)
        file_name = split_raw_file_name[0] + '_edited' + split_raw_file_name[1]
        file_path = output_path + "/" + file_name
        unix_process_time = datetime.datetime.utcnow().timestamp()
        zone = "greenroom"

        nfs_output_path = '/'.join(output_path.split("/")[6:])
        nfs_output_path = ConfigClass.NFS_ROOT_PATH + nfs_output_path

        generate_id = annotations.get("event_payload_generate_id", "undefined")
        processed_file_size = 0
        mc = Minio_Client(ConfigClass)
        try:
            result = mc.client.stat_object(file_data["bucket"], output_path + "/" + file_name)
            processed_file_size = result.size
            version_id = result.version_id
        except Exception as e:
            self._logger.warning("Error when getting file size")
            self._logger.warning(str(e))
            self._logger.warning("Terminating creating metadata")


        if processed_file_size > 0:
            self.__logger_debug.debug(
                "action triggered: " + datetime.datetime.now().isoformat())
            # v2 API
            from_parents = {
                "global_entity_id": parent_node["global_entity_id"],
            }

            file_type = os.path.splitext(file_name)[1]
            nfs_file_path = ConfigClass.MINIO_TMP_PATH + output_path + "/" + file_name
            if file_type == ".zip":
                zip_preview = generate_zip_preview(
                    nfs_file_path, 
                    file_data["bucket"], 
                    output_path + "/" + file_name, 
                    self._logger
                )
            created_node = store_file_meta_data_v2(
                uploader,
                file_name,
                nfs_output_path,
                processed_file_size,
                'processed by dicom_edit',
                zone,
                parent_node["project_code"],
                [],
                generate_id,
                'auto_trigger',
                from_parents=from_parents,
                process_pipeline=pipeline,
                parent_folder_geid=parent_folder_geid,
                object_path=output_path + "/" + file_name,
                bucket=file_data["bucket"],
                version_id=version_id
            )

            if file_type == ".zip":
                save_preview(zip_preview, created_node["global_entity_id"], self._logger)
                os.remove(nfs_file_path)

            create_lineage_v3(
                parent_node["global_entity_id"],
                created_node["global_entity_id"],
                parent_node["project_code"],
                pipeline,
                'dicom_edit Processed',
                unix_process_time
            )

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

    def parse_minio_location(self, path):
        # parse from format minio://http://10.3.7.220/gr-generate/admin/generate_folder/ABC-1234_OIP.WH4UEecUNFLkLRAy3cbgQQHaEK.jpg
        path = path.replace("minio://", "").replace("http://", "").split("/")
        bucket = path[1]
        path = '/'.join(path[2:])
        return {"bucket": bucket, "path": path}

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
