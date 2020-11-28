from kubernetes import watch
from utils.store_file_meta_data import store_file_meta_data
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


'''
from utils.k8s_client_factory import k8s_init, get_k8s_coreapi
from pipelinewatch.pod_event_listner import PodEventWatcher

k8s_configurations = k8s_init()
core_api_instance = get_k8s_coreapi(k8s_configurations)

watcher = PodEventWatcher(core_api_instance)
watcher.run()
'''

'''
{
"asctime": "2020-10-23 15:31:25,743",
"namespace": "pipelinewatch",
"sub_name": "pod_job_watcher",
"level": "INFO",
"message": null,
"type": "MODIFIED",
"object": "{'api_version': 'v1',\n 'kind': 'Pod',\n 'metadata': {'annotations': {'cni.projectcalico.org/podIP': '192.168.115.224/32',\n                              'cni.projectcalico.org/podIPs': '192.168.115.224/32'},\n              'cluster_name': None,\n              'creation_timestamp': datetime.datetime(2020, 10, 19, 14, 9, 16, tzinfo=tzutc()),\n              'deletion_grace_period_seconds': None,\n              'deletion_timestamp': None,\n              'finalizers': None,\n              'generate_name': None,\n              'generation': None,\n              'initializers': None,\n              'labels': {'airflow_version': '1.10.10',\n                         'app': 'podtest',\n                         'kubernetes_pod_operator': 'True'},\n              'managed_fields': [{'api_version': 'v1',\n                                  'fields': None,\n                                  'manager': 'OpenAPI-Generator',\n                                  'operation': 'Update',\n                                  'time': datetime.datetime(2020, 10, 19, 14, 9, 16, tzinfo=tzutc())},\n                                 {'api_version': 'v1',\n                                  'fields': None,\n                                  'manager': 'calico',\n                                  'operation': 'Update',\n                                  'time': datetime.datetime(2020, 10, 19, 14, 9, 21, tzinfo=tzutc())},\n                                 {'api_version': 'v1',\n                                  'fields': None,\n                                  'manager': 'kubelet',\n                                  'operation': 'Update',\n                                  'time': datetime.datetime(2020, 10, 23, 19, 31, 26, tzinfo=tzutc())}],\n              'name': 'airflow-test-pod-b71a2cc5',\n              'namespace': 'greenroom',\n              'owner_references': None,\n              'resource_version': '17997718',\n              'self_link': '/api/v1/namespaces/greenroom/pods/airflow-test-pod-b71a2cc5',\n              'uid': 'cc8bd943-04b2-47f6-ba57-2af63564f0cb'},\n 'spec': {'active_deadline_seconds': None,\n          'affinity': {'node_affinity': None,\n                       'pod_affinity': None,\n                       'pod_anti_affinity': None},\n          'automount_service_account_token': None,\n          'containers': [{'args': ['sleep', '30'],\n                          'command': ['bash', '-cx'],\n                          'env': None,\n                          'env_from': None,\n                          'image': 'filecopy:latest',\n                          'image_pull_policy': 'IfNotPresent',\n                          'lifecycle': None,\n                          'liveness_probe': None,\n                          'name': 'base',\n                          'ports': None,\n                          'readiness_probe': None,\n                          'resources': {'limits': None, 'requests': None},\n                          'security_context': None,\n                          'stdin': None,\n                          'stdin_once': None,\n                          'termination_message_path': '/dev/termination-log',\n                          'termination_message_policy': 'File',\n                          'tty': None,\n                          'volume_devices': None,\n                          'volume_mounts': [{'mount_path': '/var/run/secrets/kubernetes.io/serviceaccount',\n                                             'mount_propagation': None,\n                                             'name': 'default-token-76rrx',\n                                             'read_only': True,\n                                             'sub_path': None,\n                                             'sub_path_expr': None}],\n                          'working_dir': None}],\n          'dns_config': None,\n          'dns_policy': 'ClusterFirst',\n          'enable_service_links': True,\n          'host_aliases': None,\n          'host_ipc': None,\n          'host_network': None,\n          'host_pid': None,\n          'hostname': None,\n          'image_pull_secrets': [{'name': 'registrypullsecret'}],\n          'init_containers': None,\n          'node_name': 'ivysaur-9.ldap.indocresearch.org',\n          'node_selector': None,\n          'preemption_policy': None,\n          'priority': 0,\n          'priority_class_name': None,\n          'readiness_gates': None,\n          'restart_policy': 'Never',\n          'runtime_class_name': None,\n          'scheduler_name': 'default-scheduler',\n          'security_context': {'fs_group': None,\n                               'run_as_group': None,\n                               'run_as_non_root': None,\n                               'run_as_user': None,\n                               'se_linux_options': None,\n                               'supplemental_groups': None,\n                               'sysctls': None,\n                               'windows_options': None},\n          'service_account': 'default',\n          'service_account_name': 'default',\n          'share_process_namespace': None,\n          'subdomain': None,\n          'termination_grace_period_seconds': 30,\n          'tolerations': [{'effect': 'NoExecute',\n                           'key': 'node.kubernetes.io/not-ready',\n                           'operator': 'Exists',\n                           'toleration_seconds': 300,\n                           'value': None},\n                          {'effect': 'NoExecute',\n                           'key': 'node.kubernetes.io/unreachable',\n                           'operator': 'Exists',\n                           'toleration_seconds': 300,\n                           'value': None}],\n          'volumes': [{'aws_elastic_block_store': None,\n                       'azure_disk': None,\n                       'azure_file': None,\n                       'cephfs': None,\n                       'cinder': None,\n                       'config_map': None,\n                       'csi': None,\n                       'downward_api': None,\n                       'empty_dir': None,\n                       'fc': None,\n                       'flex_volume': None,\n                       'flocker': None,\n                       'gce_persistent_disk': None,\n                       'git_repo': None,\n                       'glusterfs': None,\n                       'host_path': None,\n                       'iscsi': None,\n                       'name': 'default-token-76rrx',\n                       'nfs': None,\n                       'persistent_volume_claim': None,\n                       'photon_persistent_disk': None,\n                       'portworx_volume': None,\n                       'projected': None,\n                       'quobyte': None,\n                       'rbd': None,\n                       'scale_io': None,\n                       'secret': {'default_mode': 420,\n                                  'items': None,\n                                  'optional': None,\n                                  'secret_name': 'default-token-76rrx'},\n                       'storageos': None,\n                       'vsphere_volume': None}]},\n 'status': {'conditions': [{'last_probe_time': None,\n                            'last_transition_time': datetime.datetime(2020, 10, 19, 14, 9, 16, tzinfo=tzutc()),\n                            'message': None,\n                            'reason': None,\n                            'status': 'True',\n                            'type': 'Initialized'},\n                           {'last_probe_time': None,\n                            'last_transition_time': datetime.datetime(2020, 10, 19, 14, 9, 16, tzinfo=tzutc()),\n                            'message': 'containers with unready status: [base]',\n                            'reason': 'ContainersNotReady',\n                            'status': 'False',\n                            'type': 'Ready'},\n                           {'last_probe_time': None,\n                            'last_transition_time': datetime.datetime(2020, 10, 19, 14, 9, 16, tzinfo=tzutc()),\n                            'message': 'containers with unready status: [base]',\n                            'reason': 'ContainersNotReady',\n                            'status': 'False',\n                            'type': 'ContainersReady'},\n                           {'last_probe_time': None,\n                            'last_transition_time': datetime.datetime(2020, 10, 19, 14, 9, 16, tzinfo=tzutc()),\n                            'message': None,\n                            'reason': None,\n                            'status': 'True',\n                            'type': 'PodScheduled'}],\n            'container_statuses': [{'container_id': None,\n                                    'image': 'filecopy:latest',\n                                    'image_id': '',\n                                    'last_state': {'running': None,\n                                                   'terminated': None,\n                                                   'waiting': None},\n                                    'name': 'base',\n                                    'ready': False,\n                                    'restart_count': 0,\n                                    'state': {'running': None,\n                                              'terminated': None,\n                                              'waiting': {'message': 'Back-off '\n                                                                     'pulling '\n                                                                     'image '\n                                                                     '\"filecopy:latest\"',\n                                                          'reason': 'ImagePullBackOff'}}}],\n            'host_ip': '10.3.7.9',\n            'init_container_statuses': None,\n            'message': None,\n            'nominated_node_name': None,\n            'phase': 'Pending',\n            'pod_ip': '192.168.115.224',\n            'qos_class': 'BestEffort',\n            'reason': None,\n            'start_time': datetime.datetime(2020, 10, 19, 14, 9, 16, tzinfo=tzutc())}}",
"raw_object": {
    "kind": "Pod",
    "apiVersion": "v1",
    "metadata": {
        "name": "airflow-test-pod-b71a2cc5",
        "namespace": "greenroom",
        "selfLink": "/api/v1/namespaces/greenroom/pods/airflow-test-pod-b71a2cc5",
        "uid": "cc8bd943-04b2-47f6-ba57-2af63564f0cb",
        "resourceVersion": "17997718",
        "creationTimestamp": "2020-10-19T14:09:16Z",
        "labels": {
            "airflow_version": "1.10.10",
            "app": "podtest",
            "kubernetes_pod_operator": "True"
        },
        "annotations": {
            "cni.projectcalico.org/podIP": "192.168.115.224/32",
            "cni.projectcalico.org/podIPs": "192.168.115.224/32"
        },
        "managedFields": [{
            "manager": "OpenAPI-Generator",
            "operation": "Update",
            "apiVersion": "v1",
            "time": "2020-10-19T14:09:16Z",
            "fieldsType": "FieldsV1",
            "fieldsV1": {
                "f:metadata": {
                    "f:labels": {
                        ".": {},
                        "f:airflow_version": {},
                        "f:app": {},
                        "f:kubernetes_pod_operator": {}
                    }
                },
                "f:spec": {
                    "f:affinity": {},
                    "f:containers": {
                        "k:{\"name\":\"base\"}": {
                            ".": {},
                            "f:args": {},
                            "f:command": {},
                            "f:image": {},
                            "f:imagePullPolicy": {},
                            "f:name": {},
                            "f:resources": {},
                            "f:terminationMessagePath": {},
                            "f:terminationMessagePolicy": {}
                        }
                    },
                    "f:dnsPolicy": {},
                    "f:enableServiceLinks": {},
                    "f:restartPolicy": {},
                    "f:schedulerName": {},
                    "f:securityContext": {},
                    "f:serviceAccount": {},
                    "f:serviceAccountName": {},
                    "f:terminationGracePeriodSeconds": {}
                }
            }
        }, {
            "manager": "calico",
            "operation": "Update",
            "apiVersion": "v1",
            "time": "2020-10-19T14:09:21Z",
            "fieldsType": "FieldsV1",
            "fieldsV1": {
                "f:metadata": {
                    "f:annotations": {
                        ".": {},
                        "f:cni.projectcalico.org/podIP": {},
                        "f:cni.projectcalico.org/podIPs": {}
                    }
                }
            }
        }, {
            "manager": "kubelet",
            "operation": "Update",
            "apiVersion": "v1",
            "time": "2020-10-23T19:31:26Z",
            "fieldsType": "FieldsV1",
            "fieldsV1": {
                "f:status": {
                    "f:conditions": {
                        "k:{\"type\":\"ContainersReady\"}": {
                            ".": {},
                            "f:lastProbeTime": {},
                            "f:lastTransitionTime": {},
                            "f:message": {},
                            "f:reason": {},
                            "f:status": {},
                            "f:type": {}
                        },
                        "k:{\"type\":\"Initialized\"}": {
                            ".": {},
                            "f:lastProbeTime": {},
                            "f:lastTransitionTime": {},
                            "f:status": {},
                            "f:type": {}
                        },
                        "k:{\"type\":\"Ready\"}": {
                            ".": {},
                            "f:lastProbeTime": {},
                            "f:lastTransitionTime": {},
                            "f:message": {},
                            "f:reason": {},
                            "f:status": {},
                            "f:type": {}
                        }
                    },
                    "f:containerStatuses": {},
                    "f:hostIP": {},
                    "f:podIP": {},
                    "f:podIPs": {
                        ".": {},
                        "k:{\"ip\":\"192.168.115.224\"}": {
                            ".": {},
                            "f:ip": {}
                        }
                    },
                    "f:startTime": {}
                }
            }
        }]
    },
    "spec": {
        "volumes": [{
            "name": "default-token-76rrx",
            "secret": {
                "secretName": "default-token-76rrx",
                "defaultMode": 420
            }
        }],
        "containers": [{
            "name": "base",
            "image": "filecopy:latest",
            "command": ["bash", "-cx"],
            "args": ["sleep", "30"],
            "resources": {},
            "volumeMounts": [{
                "name": "default-token-76rrx",
                "readOnly": true,
                "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount"
            }],
            "terminationMessagePath": "/dev/termination-log",
            "terminationMessagePolicy": "File",
            "imagePullPolicy": "IfNotPresent"
        }],
        "restartPolicy": "Never",
        "terminationGracePeriodSeconds": 30,
        "dnsPolicy": "ClusterFirst",
        "serviceAccountName": "default",
        "serviceAccount": "default",
        "nodeName": "ivysaur-9.ldap.indocresearch.org",
        "securityContext": {},
        "imagePullSecrets": [{
            "name": "registrypullsecret"
        }],
        "affinity": {},
        "schedulerName": "default-scheduler",
        "tolerations": [{
            "key": "node.kubernetes.io/not-ready",
            "operator": "Exists",
            "effect": "NoExecute",
            "tolerationSeconds": 300
        }, {
            "key": "node.kubernetes.io/unreachable",
            "operator": "Exists",
            "effect": "NoExecute",
            "tolerationSeconds": 300
        }],
        "priority": 0,
        "enableServiceLinks": true
    },
    "status": {
        "phase": "Pending",
        "conditions": [{
            "type": "Initialized",
            "status": "True",
            "lastProbeTime": null,
            "lastTransitionTime": "2020-10-19T14:09:16Z"
        }, {
            "type": "Ready",
            "status": "False",
            "lastProbeTime": null,
            "lastTransitionTime": "2020-10-19T14:09:16Z",
            "reason": "ContainersNotReady",
            "message": "containers with unready status: [base]"
        }, {
            "type": "ContainersReady",
            "status": "False",
            "lastProbeTime": null,
            "lastTransitionTime": "2020-10-19T14:09:16Z",
            "reason": "ContainersNotReady",
            "message": "containers with unready status: [base]"
        }, {
            "type": "PodScheduled",
            "status": "True",
            "lastProbeTime": null,
            "lastTransitionTime": "2020-10-19T14:09:16Z"
        }],
        "hostIP": "10.3.7.9",
        "podIP": "192.168.115.224",
        "podIPs": [{
            "ip": "192.168.115.224"
        }],
        "startTime": "2020-10-19T14:09:16Z",
        "containerStatuses": [{
            "name": "base",
            "state": {
                "waiting": {
                    "reason": "ImagePullBackOff",
                    "message": "Back-off pulling image \"filecopy:latest\""
                }
            },
            "lastState": {},
            "ready": false,
            "restartCount": 0,
            "image": "filecopy:latest",
            "imageID": "",
            "started": false
        }],
        "qosClass": "BestEffort"
    }
}
}
'''
