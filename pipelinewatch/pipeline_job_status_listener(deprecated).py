from utils.k8s_client_factory import get_k8s_batchapi
from utils.loopper import loop_start
from utils.store_file_meta_data import store_file_meta_data
from config import ConfigClass
import os
import logging

class PipelineJobStatusListener:
    def __init__(self, batch_api):
        self.batch_api = batch_api
        self._logger = logging.getLogger()
    def callback_fetch_pipeline_status(self, jobs):
        def job_filter(job):
            active_pods = job.status.active
            return active_pods == 0 or active_pods == None
        finished_pipeline_jobs = [job for job in jobs if job_filter(job) ]
        num_of_jobs = len(finished_pipeline_jobs)
        self._logger.info("Found " + str(num_of_jobs) + " Finished Pipeline Jobs")
        if num_of_jobs > 0:
            for job in finished_pipeline_jobs:
                job_name = job.metadata.name
                pipeline = job.metadata.labels['pipeline']
                my_final_status = 'failed' if job.status.failed else 'succeeded'
                self._logger.info(job_name + ": " + my_final_status)
                container = job.spec.template.spec.containers[0]
                args = container.args
                input_path = args[1]
                output_path = args[3]
                decoded_input_path = input_path.split('/')
                bucket_name = decoded_input_path[3]
                raw_file_name = decoded_input_path[5]
                split_raw_file_name = os.path.splitext(raw_file_name)
                file_name = split_raw_file_name[0] + '_edited' + split_raw_file_name[1]
                # file_path = output_path + "/" + file_name
                file_path = output_path
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
                        dele_res = self.batch_api.delete_namespaced_job(job_name, ConfigClass.k8s_namespace)
                        print(dele_res)
                        self._logger.info("Deleted job: " + job_name)
                        self._logger.info(" ")
                    except Exception as e:
                        self._logger.error(str(e))
                else:
                    self._logger.warning("Terminating creating metadata")
    def run(self):
        def read_k8s_jobs():
            fetched = self.batch_api.list_namespaced_job(ConfigClass.k8s_namespace)
            jobs = fetched.items
            self.callback_fetch_pipeline_status(jobs)
        self._logger.info("Start Watching Pipeline Jobs")
        loop_start(read_k8s_jobs, ConfigClass.pipeline_job_peek_interval)