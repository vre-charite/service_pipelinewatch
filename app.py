from config import ConfigClass
from utils.k8s_client_factory import k8s_init, get_k8s_batchapi
import os
import logging
import threading
from pipelinewatch.stream_listner import StreamWatcher

def main():
    # pipeline watcher thread
    ## init k8s
    k8s_configurations = k8s_init()
    batch_api_instance = get_k8s_batchapi(k8s_configurations)
    ## start watching k8s pipeline jobs
    my_job_watcher = StreamWatcher(batch_api_instance)
    my_job_watcher.run()

if __name__ == '__main__':
    main()
