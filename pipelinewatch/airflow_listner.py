import requests, time, os, json, datetime
from services.logger_services.logger_factory_service import SrvLoggerFactory
from services.data_providers.redis import SrvRedisSingleton, ERedisChannels
from config import ConfigClass
from utils.loopper import loop_start
from utils.lineage_operations import create_lineage
from utils.meta_data_operations import store_file_meta_data
from multiprocessing import Process
from enum import Enum

class AirFlowStreamWatch():
    def __init__(self):
        self.name = "air_flow_watch"
        self.__logger = SrvLoggerFactory(self.name).get_logger()

    def run(self):
        self.__logger.info('Start AirFlow Stream Watch')
        stream = self.__get_stream()
        while True:
            msg = stream.get_message()
            if msg and msg['type'] == 'message':
                on_success_payload = json.loads(msg['data'].decode('utf-8'))
                self.__logger.debug('event watched')
                def to_execute(payload):
                    self.__logger.info('Start track status: ' + payload['dag_id'])
                    is_continue = True
                    while is_continue:
                        dag_info = self.get_dag_info(payload['dag_id'])
                        if dag_info and dag_info[0]['state'] == 'success':
                            ## if pipeline succeed, run callback, save meta, exit watch process
                            self.__logger.debug('Airflow Event JOB-Succeed Triggered')
                            self.__on_success_callback(payload)
                            is_continue = False
                            time.sleep(ConfigClass.airflow_job_peek_interval)
                            self.__logger.info('End track: ' + payload['dag_id'] + ' Job Succeed')
                        elif dag_info and dag_info[0]['state'] == 'failed':
                            is_continue = False
                            self.__logger.info('End track: ' + payload['dag_id'] + ' Job Failed')
                        else:
                            self.__logger.debug('Continue track: ' + payload['dag_id'] + ' Job Running')
                __process = Process(target=to_execute, args=(on_success_payload, ))
                __process.start()

    def __get_stream(self):
        my_redis = SrvRedisSingleton()
        subscriber = my_redis.subscriber(ERedisChannels.pipeline_process_start.name)
        return subscriber

    def get_dag_info(self, dag_id):
        list_url = '{}/api/experimental/dags/{}/dag_runs'.format(ConfigClass.service_airflow_url, dag_id)
        res_fetched = requests.get(list_url)
        return res_fetched.json() if res_fetched.status_code == 200 else None

    def __on_success_callback(self, on_success_payload):
        dag_id = on_success_payload["dag_id"]
        sub_payload = on_success_payload['payload']
        process_pipeline = sub_payload['process_pipeline']
        create_time = sub_payload['create_time']
        # iso_process_time = datetime.datetime.utcfromtimestamp(float(create_time)).isoformat()
        iso_process_time = datetime.datetime.utcnow().isoformat()
        self.__logger.debug('iso_process_time: ' + iso_process_time)
        ## transfer pipeline process
        if process_pipeline == EPipelineType.data_transfer.name:
            input_full_path = sub_payload["input_path"]
            output_full_path = sub_payload["output_path"]
            output_file_name = os.path.basename(output_full_path)
            project_code = sub_payload["project"]
            processed_file_size = 0
            try:
                processed_file_size = os.path.getsize(output_full_path)
            except Exception as e:
                self.__logger.error("Error when getting file size")
                self.__logger.error(str(e))
                self.__logger.error("Terminating creating metadata")
            store_file_meta_data(
                output_full_path,
                project_code,
                output_file_name,
                input_full_path,
                processed_file_size,
                process_pipeline,
                dag_id,
                "success"
            )
            self.__logger.debug('Saved meta')
            create_lineage(
                input_full_path,
                output_full_path,
                project_code,
                process_pipeline,
                'Airflow Processed',
                iso_process_time
            )
            self.__logger.debug('Created Lineage')

class AirflowWatch():
    def __init__(self):
        self.__logger = SrvLoggerFactory('air_flow_watch').get_logger()

    def run(self):
        loop_start(self.get_latest_runs, ConfigClass.airflow_job_peek_interval)

    def get_latest_runs(self):
        print('loop start')
        my_url = ConfigClass.service_airflow_url + '/api/experimental/latest_runs'
        res_fetched = requests.get(my_url)
        if res_fetched.status_code == 200:
            dags = res_fetched.json()['items']
            for dag in dags:
                self._watch_callback(dag)

    def get_dag_info(self, dag_id):
        list_url = '{}/api/experimental/dags/{}/dag_runs'.format(ConfigClass.service_airflow_url, dag_id)
        res_fetched = requests.get(list_url)
        return res_fetched.json()

    def _watch_callback(self, dag):
        dag_id = dag['dag_id']
        list_url = '{}/api/experimental/dags/{}/dag_runs'.format(ConfigClass.service_airflow_url, dag_id)
        res_fetched = requests.get(list_url)
        self.__logger.debug(res_fetched.json())

class AirTaskWatch():

    def __init__(self, dag_id, task_id):
        self.dag_id = dag_id
        self.task_id = task_id
        self.__logger = SrvLoggerFactory('air_flow_watch').get_logger()
        self.endpoint_get_task = ConfigClass.service_airflow_url + "/api/experimental/dags/{}/tasks/{}".format(dag_id, task_id)

    def fetch_status(self):
        fetched = requests.get(self.endpoint_get_task)
        if fetched.status_code == 200:
            fetched_json = fetched.json()
            annotations = fetched_json['annotations']
            task_id = fetched_json['task_id']
            cmds = fetched_json['cmds']
            self.__logger.debug(annotations)
            input_full_path = annotations["input_full_path"]
            output_full_path = annotations["output_full_path"]
            output_file_name = os.path.basename(output_full_path)
            process_pipeline = annotations["process_pipeline"]
            project_code = annotations["project_code"]
            processed_file_size = 0
            try:
                processed_file_size = os.path.getsize(output_full_path)
            except Exception as e:
                self.__logger.error("Error when getting file size")
                self.__logger.error(str(e))
                self.__logger.error("Terminating creating metadata")
            store_file_meta_data(
                output_full_path,
                project_code,
                output_file_name,
                input_full_path,
                processed_file_size,
                process_pipeline,
                task_id,
                task_id
            )
            create_lineage(
                input_full_path,
                output_full_path,
                project_code,
                process_pipeline,
                'Airflow Processed File'
            )
        else:
            return False
        return True

    def run(self):
        def to_execute():
            is_continue = True
            while is_continue:
                is_continue = self.test_callback()
                time.sleep(ConfigClass.airflow_job_peek_interval)
        __process = Process(target=to_execute)
        __process.start()

    def test_callback(self):
        fetched = requests.get(self.endpoint_get_task)
        print(fetched.json()['cmds'])
        return True

def batch_task_watch(dag_id, task_ids):
    for task_id in task_ids:
        task_wathcer = AirTaskWatch(dag_id, task_id)
        task_wathcer.run()

class EPipelineType(Enum):
    data_transfer = 0