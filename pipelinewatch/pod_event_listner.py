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

from kubernetes import watch
# from utils.store_file_meta_data import store_file_meta_data
from utils.lineage_operations import create_lineage
from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory
import os
from enum import Enum

class PodEventWatcher:
    def __init__(self, core_api):
        self.watcher = watch.Watch()
        self.core_api = core_api
        self._logger = SrvLoggerFactory('pod_job_watcher').get_logger()
    def _watch_callback(self, event):
        self._logger.info("Event: %s %s" % (event['type'], event['object'].metadata.name))
        return
    def _get_stream(self):
        stream = self.watcher.stream(self.core_api.list_pod_for_all_namespaces)
        return stream
    def run(self):
        self._logger.info('Start Pod Events Stream Watching')
        stream = self._get_stream()
        for event in stream:
            self._watch_callback(event)

class PodEvent(Enum):
    ADDED = 0,
    MODIFIED = 1,
    DELETED = 2
