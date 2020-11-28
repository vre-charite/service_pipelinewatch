import os

class ConfigClass(object):

    env = os.environ.get('env')

    #greenroom queue
    gm_queue_endpoint = 'message-bus-greenroom.greenroom'
    gm_username = 'greenroom'
    gm_password = 'indoc101'

    # data_lake
    data_lake = "/data/vre-storage"
    tvb_project_code = "tvbcloud"

    #queue service url
    service_queue_send_msg_url = "http://queue-producer.greenroom:6060/v1/send_message"

    #data ops url
    data_ops_host = "http://dataops-gr.greenroom:5063"
    # data_ops_host = "http://10.3.7.234:5063"

    #service_cateloguing
    service_cateloguing = "http://cataloguing.utility:5064"
    # service_cateloguing = "http://10.3.7.237:5064"

    #k8s_namespace
    k8s_namespace = "greenroom"

    #pipeline_job_peek_interval
    pipeline_job_peek_interval = 60

    #airflow
    service_airflow_url = "http://10.3.7.235:8080"
    airflow_job_peek_interval = 5

    #generate project
    generate_project_process_file_folder = "/generate/processed/"

    # Redis Service
    REDIS_HOST = "redis-master.utility"
    # REDIS_HOST = "10.3.7.233"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = {
        'staging': '8EH6QmEYJN',
        'charite': 'o2x7vGQx6m'
    }.get(env, "5wCCMMC1Lk")