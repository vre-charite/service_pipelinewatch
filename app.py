# Copyright 2022 Indoc Research
# 
# Licensed under the EUPL, Version 1.2 or – as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
# 
# https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
# 
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
# 

from config import ConfigClass, SRV_NAMESPACE
from utils.k8s_client_factory import k8s_init, get_k8s_batchapi
import os, logging, threading
from pipelinewatch.stream_listner import StreamWatcher
# from pipelinewatch.airflow_listner import AirFlowStreamWatch
from multiprocessing import Process
from services.logger_services.logger_factory_service import SrvLoggerFactory


__logger = SrvLoggerFactory('main_thread').get_logger()

def main():
    # watcher run
    watcher_processes = []
    # register watcher
    watcher_processes.append(wrap_stream_watch_process)
    # watcher_processes.append(AirFlowStreamWatch().run)
    # start wathcers
    def run_process(watcher_p):
        watcher_p()
    for watcher_p in watcher_processes:
        p = Process(target=run_process, args=(watcher_p,))
        p.start()
        p.join()
    

def wrap_stream_watch_process():
    ## init k8s
    k8s_configurations = k8s_init()
    batch_api_instance = get_k8s_batchapi(k8s_configurations)
    stream_watch = StreamWatcher(batch_api_instance)
    while True:
        stream_watch.run()
    __logger.debug('wrap_stream_watch_process down')


if __name__ == '__main__':
    main()
