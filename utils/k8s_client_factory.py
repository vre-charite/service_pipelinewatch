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

from kubernetes import client, config

def k8s_init():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    return client.Configuration()

def get_k8s_batchapi(configuration):
    return client.BatchV1Api(client.ApiClient(configuration))

def get_k8s_coreapi(configuration):
    return client.CoreV1Api()