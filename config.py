import os
import requests
from requests.models import HTTPError
from pydantic import BaseSettings, Extra
from typing import Dict, Set, List, Any
from functools import lru_cache

SRV_NAMESPACE = os.environ.get("APP_NAME", "service_pipelinewatch")
CONFIG_CENTER_ENABLED = os.environ.get("CONFIG_CENTER_ENABLED", "false")
CONFIG_CENTER_BASE_URL = os.environ.get("CONFIG_CENTER_BASE_URL", "NOT_SET")

def load_vault_settings(settings: BaseSettings) -> Dict[str, Any]:
    if CONFIG_CENTER_ENABLED == "false":
        return {}
    else:
        return vault_factory(CONFIG_CENTER_BASE_URL)

def vault_factory(config_center) -> dict:
    url = f"{config_center}/v1/utility/config/{SRV_NAMESPACE}"
    config_center_respon = requests.get(url)
    if config_center_respon.status_code != 200:
        raise HTTPError(config_center_respon.text)
    return config_center_respon.json()['result']


class Settings(BaseSettings):
    port: int = 6063
    host: str = "0.0.0.0"
    env: str = "test"
    namespace: str = ""
    
    # greenroom queue
    gm_queue_endpoint: str
    gm_username: str
    gm_password: str

    # data_lake
    data_lake: str = "/data/vre-storage"
    tvb_project_code: str = "tvbcloud"
    # disk mounts
    NFS_ROOT_PATH: str = "/data/vre-storage"
    VRE_ROOT_PATH: str = "/vre-data"

    QUEUE_SERVICE: str
    DATA_OPS_GR: str
    DATA_OPS_UTIL: str
    NEO4J_SERVICE: str
    CATALOGUING_SERVICE: str
    UTILITY_SERVICE: str
    ENTITYINFO_SERVICE: str
    PROVENANCE_SERVICE: str

    # k8s_namespace
    k8s_namespace: str = "greenroom"

    # pipeline_job_peek_interval
    pipeline_job_peek_interval: int = 60

    # airflow
    airflow_job_peek_interval: int = 5

    # generate project
    generate_project_process_file_folder: str = "/generate/processed/"

    # Redis Service
    REDIS_HOST: str
    REDIS_PORT: str
    REDIS_DB: str
    REDIS_PASSWORD: str

    # system tags
    copied_with_approval: str = "copied-to-core"

    debug_mode: bool = False

    # minio config
    MINIO_OPENID_CLIENT: str
    MINIO_ENDPOINT: str
    MINIO_HTTPS: str
    KEYCLOAK_URL: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_TMP_PATH: str = "/data/vre-storage/tmp/"
    
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        extra = Extra.allow

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                load_vault_settings,
                env_settings,
                init_settings,
                file_secret_settings,
            )
    

@lru_cache(1)
def get_settings():
    settings =  Settings()
    return settings

class ConfigClass(object):
    settings = get_settings()

    version = "0.1.0"
    env = settings.env
    disk_namespace = settings.namespace
    
    # greenroom queue
    gm_queue_endpoint = settings.gm_queue_endpoint
    gm_username = settings.gm_username
    gm_password = settings.gm_password

    # data_lake
    data_lake = settings.data_lake
    tvb_project_code = settings.tvb_project_code
    # disk mounts
    NFS_ROOT_PATH = settings.NFS_ROOT_PATH
    VRE_ROOT_PATH = settings.VRE_ROOT_PATH

    QUEUE_SERVICE = settings.QUEUE_SERVICE + "/v1/"
    DATA_OPS_GR = settings.DATA_OPS_GR + "/v1/"
    DATA_OPS_GR_V2 = settings.DATA_OPS_GR + "/v2/"
    DATA_OPS_UT = settings.DATA_OPS_UTIL + "/v1/"
    DATA_OPS_UT_V2 = settings.DATA_OPS_UTIL + "/v2/"
    NEO4J_SERVICE = settings.NEO4J_SERVICE + "/v1/neo4j/"
    NEO4J_SERVICE_V2 = settings.NEO4J_SERVICE + "/v2/neo4j/"
    CATALOGUING_SERVICE = settings.CATALOGUING_SERVICE + "/v1/"
    CATALOGUING_SERVICE_V2 = settings.CATALOGUING_SERVICE + "/v2/"
    UTILITY_SERVICE = settings.UTILITY_SERVICE + "/v1/"
    ENTITY_INFO_SERVICE = settings.ENTITYINFO_SERVICE + "/v1/"
    PROVENANCE_SERVICE = settings.PROVENANCE_SERVICE + "/v1/"

    # k8s_namespace
    k8s_namespace = settings.k8s_namespace

    # pipeline_job_peek_interval
    pipeline_job_peek_interval = settings.pipeline_job_peek_interval

    # airflow
    airflow_job_peek_interval = settings.airflow_job_peek_interval

    # generate project
    generate_project_process_file_folder = settings.generate_project_process_file_folder

    # Redis Service
    REDIS_HOST = settings.REDIS_HOST
    REDIS_PORT = int(settings.REDIS_PORT)
    REDIS_DB = int(settings.REDIS_DB)
    REDIS_PASSWORD = settings.REDIS_PASSWORD

    # system tags
    copied_with_approval = settings.copied_with_approval

    debug_mode = settings.debug_mode

    # minio config
    MINIO_OPENID_CLIENT = settings.MINIO_OPENID_CLIENT
    MINIO_ENDPOINT = settings.MINIO_ENDPOINT
    MINIO_HTTPS = settings.MINIO_HTTPS == "TRUE"
    KEYCLOAK_URL = settings.KEYCLOAK_URL
    MINIO_ACCESS_KEY = settings.MINIO_ACCESS_KEY
    MINIO_SECRET_KEY = settings.MINIO_SECRET_KEY
    MINIO_TMP_PATH = settings.MINIO_TMP_PATH
