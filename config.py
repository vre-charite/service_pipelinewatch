import os


class ConfigClass(object):

    env = os.environ.get('env', 'test')

    # greenroom queue
    gm_queue_endpoint = 'message-bus-greenroom.greenroom'
    gm_username = 'greenroom'
    gm_password = 'indoc101'

    # data_lake
    data_lake = "/data/vre-storage"
    tvb_project_code = "tvbcloud"
    # disk mounts
    NFS_ROOT_PATH = "/data/vre-storage"
    VRE_ROOT_PATH = "/vre-data"

    QUEUE_SERVICE = "http://queue-producer.greenroom:6060/v1/"
    DATA_OPS_GR = "http://dataops-gr.greenroom:5063/v1/"
    DATA_OPS_GR_V2 = "http://dataops-gr.greenroom:5063/v2/"
    DATA_OPS_UT = "http://dataops-ut.utility:5063/v1/"
    DATA_OPS_UT_V2 = "http://dataops-ut.utility:5063/v2/"
    NEO4J_SERVICE = "http://neo4j.utility:5062/v1/neo4j/"
    NEO4J_SERVICE_V2 = "http://neo4j.utility:5062/v2/neo4j/"
    CATALOGUING_SERVICE = "http://cataloguing.utility:5064/v1/"
    CATALOGUING_SERVICE_V2 = "http://cataloguing.utility:5064/v2/"
    UTILITY_SERVICE = "http://common.utility:5062/v1/"
    ENTITY_INFO_SERVICE = "http://entityinfo.utility:5066/v1/"
    PROVENANCE_SERVICE = "http://provenance.utility:5077/v1/"

    # k8s_namespace
    k8s_namespace = "greenroom"

    # pipeline_job_peek_interval
    pipeline_job_peek_interval = 60

    # airflow
    service_airflow_url = "http://10.3.7.235:8080"
    airflow_job_peek_interval = 5

    # generate project
    generate_project_process_file_folder = "/generate/processed/"

    # Redis Service
    REDIS_HOST = "redis-master.utility"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = {
        'staging': '8EH6QmEYJN',
        'charite': 'o2x7vGQx6m'
    }.get(env, "5wCCMMC1Lk")

    # system tags
    copied_with_approval = 'copied-to-core'

    if env == 'test':
        REDIS_HOST = "10.3.7.233"
        QUEUE_SERVICE = "http://queue-producer.greenroom:6060/v1/"
        DATA_OPS_GR = "http://10.3.7.234:5063/v1/"
        DATA_OPS_GR_V2 = "http://10.3.7.234:5063/v2/"
        DATA_OPS_UT = "http://10.3.7.239:5063/v1/"
        DATA_OPS_UT_V2 = "http://10.3.7.239:5063/v2/"
        NEO4J_SERVICE = "http://10.3.7.216:5062/v1/neo4j/"
        NEO4J_SERVICE_V2 = "http://10.3.7.216:5062/v2/neo4j/"
        CATALOGUING_SERVICE = "http://10.3.7.237:5064/v1/"
        CATALOGUING_SERVICE_V2 = "http://10.3.7.237:5064/v2/"
        UTILITY_SERVICE = "http://10.3.7.222:5062/v1/"
        ENTITY_INFO_SERVICE = "http://10.3.7.228:5066/v1/"
        PROVENANCE_SERVICE = "http://10.3.7.202:5077/v1/"

    debug_mode = False

    # minio config
    MINIO_OPENID_CLIENT = "react-app"
    MINIO_ENDPOINT = "minio.minio:9000"
    MINIO_HTTPS = False
    KEYCLOAK_URL = "http://keycloak.utility:8080"
    MINIO_ACCESS_KEY = "indoc-minio"
    MINIO_SECRET_KEY = "Trillian42!"
    MINIO_TMP_PATH = "/data/vre-storage/tmp/"

    if env == "test":
        # minio config
        MINIO_ENDPOINT = "10.3.7.220"
        MINIO_HTTPS = False
        KEYCLOAK_URL = "http://10.3.7.220" # for local test ONLY


    # MINIO_OPENID_CLIENT = "react-app"
    # if env == "staging":
    #     # MINIO_ENDPOINT = "10.3.7.240:80"
    #     MINIO_ENDPOINT = "minio.minio:9000"
    #     MINIO_HTTPS = False
    #     KEYCLOAK_URL = "http://10.3.7.240:80"
    #     MINIO_TEST_PASS = "IndocStaging2021!"
    # elif env == "test":
    #     MINIO_ENDPOINT = "10.3.7.220"
    #     MINIO_HTTPS = False
    #     KEYCLOAK_URL = "http://keycloak.utility:8080"
    #     KEYCLOAK_URL = "http://10.3.7.220"
    #     MINIO_TEST_PASS = "admin"

    # else:
    #     MINIO_ENDPOINT = "10.3.7.220"
    #     MINIO_HTTPS = False
    #     KEYCLOAK_URL = "http://keycloak.utility:8080"
    #     # KEYCLOAK_URL = "http://10.3.7.220" # for local test ONLY
    #     MINIO_TEST_PASS = "admin"
