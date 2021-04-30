from models.service_meta_class import MetaService
import requests
from config import ConfigClass
import os

class SrvFileDataMgr(metaclass=MetaService):
    base_url = ConfigClass.data_ops_util_host
    def create(self, uploader, file_name, path, file_size, desc, namespace,
            project_code, labels, generate_id, operator=None, from_parents=None, process_pipeline=None):

        # fetch geid
        global_entity_id = self.fetch_guid()

        url = self.base_url + "/v1/filedata"
        post_json_form = {
            "global_entity_id": global_entity_id,
            "uploader": uploader,
            "file_name": file_name,
            "path": path,
            "file_size": file_size,
            "description": desc,
            "namespace": namespace,
            "project_code": project_code,
            "labels": labels,
            "generate_id": generate_id
        }
        if operator:
            post_json_form['operator'] = operator
        if from_parents:
            post_json_form['parent_query'] = from_parents
        if process_pipeline:
            post_json_form['process_pipeline'] = process_pipeline 
        res = requests.post(url = url, json=post_json_form)
        if res.status_code == 200:
            return res.json()
        else:
            return {
                "error": "create meta failed",
                "errorcode": res.status_code
            }
    
    def archive(self, path, file_name, trash_path, trash_file_name, operator, file_name_suffix, trash_geid):
        url = ConfigClass.service_cateloguing + "/v2/filedata"
        dele_json_form = {
            "path": path,
            "file_name": file_name,
            "trash_path": trash_path,
            "trash_file_name": trash_file_name,
            "operator": operator,
            "file_name_suffix": file_name_suffix,
            "trash_geid": trash_geid
        }
        res = requests.delete(url = url, json=dele_json_form)
        if res.status_code == 200:
            return res.json()
        else:
            return {
                "error": "archive meta failed",
                "errorcode": res.status_code,
                "error_msg": res.text
            }

    def archive_in_neo4j(self, path, file_name, project_code, updated_file_name):
        try:
            parent_full_path = path + "/" + file_name
            ## get project information
            get_project_url = ConfigClass.NEO4J_SERVICE + "/v1/neo4j/nodes/Dataset/query"
            get_project_json = {
                "code": project_code
            }
            res_project_gotten = requests.post(url = get_project_url, json=get_project_json)
            if res_project_gotten.status_code == 200:
                pass
            else:
                return {
                    "error": "archive meta in neo4j failed",
                    "errorcode": res_project_gotten.status_code,
                    "error_msg": res_project_gotten.text + "------------" + get_project_url,
                    "payload": str(get_project_json)
                }
            project_id = res_project_gotten.json()[0]['id']
            ## get parent node
            get_parent_url = ConfigClass.ENTITY_INFO_SERVICE + "/v1/files/{}/query".format(project_id)
            get_parent_json = {
                "page": 0,
                "page_size": 1,
                "query": {
                    "full_path": parent_full_path,
                    "labels": ["File"]
                },
                "order_by": "name",
                "order_type": "desc"
            }
            res_parent_gotten = requests.post(url = get_parent_url, json=get_parent_json)
            if res_parent_gotten.status_code == 200:
                print('updated res_parent_gotten: ', res_parent_gotten.json())
            else:
                return {
                    "error": "archive meta in neo4j failed",
                    "errorcode": res_parent_gotten.status_code,
                    "error_msg": res_parent_gotten.text + "------------" + get_parent_url,
                    "payload": str(get_parent_json)
                }
            neo4j_id = res_parent_gotten.json()['result'][0]['id']
            ## update parent file
            update_url = ConfigClass.NEO4J_SERVICE + "/v1/neo4j/nodes/File/node/{}".format(neo4j_id)
            update_json = {
                "archived": True,
                "name": updated_file_name,
                "full_path": path + "/" + updated_file_name
            }
            res = requests.put(url = update_url, json=update_json)
            if res.status_code == 200:
                print('updated archive_in_neo4j: ', res.json())
            else:
                return {
                    "error": "archive meta in neo4j failed",
                    "errorcode": res.status_code,
                    "error_msg": res.text + "-----------" + update_url,
                    "payload": str(update_json)
                }
        except Exception as e:
                return {
                    "error": "archive meta in neo4j failed",
                    "errorcode": 500,
                    "error_msg": str(e)
                }

    def add_approval_copy_for_neo4j(self, parent_full_path, project_code):
        try:
            ## get project information
            get_project_url = ConfigClass.NEO4J_SERVICE + "/v1/neo4j/nodes/Dataset/query"
            get_project_json = {
                "code": project_code
            }
            res_project_gotten = requests.post(url = get_project_url, json=get_project_json)
            if res_project_gotten.status_code == 200:
                pass
            else:
                return {
                    "error": "add_approval_copy_for_neo4j in neo4j failed",
                    "errorcode": res_project_gotten.status_code,
                    "error_msg": res_project_gotten.text + "------------" + get_project_url,
                    "payload": str(get_project_json)
                }
            project_id = res_project_gotten.json()[0]['id']
            ## get parent node
            get_parent_url = ConfigClass.ENTITY_INFO_SERVICE + "/v1/files/{}/query".format(project_id)
            get_parent_json = {
                "page": 0,
                "page_size": 1,
                "query": {
                    "full_path": parent_full_path,
                    "labels": ["File"]
                },
                "order_by": "name",
                "order_type": "desc"
            }
            res_parent_gotten = requests.post(url = get_parent_url, json=get_parent_json)
            if res_parent_gotten.status_code == 200:
                print('add_approval_copy_for_neo4j res_parent_gotten: ', res_parent_gotten.json())
            else:
                return {
                    "error": "add_approval_copy_for_neo4j in neo4j failed",
                    "errorcode": res_parent_gotten.status_code,
                    "error_msg": res_parent_gotten.text + "------------" + get_parent_url,
                    "payload": str(get_parent_json)
                }
            parent_geid = res_parent_gotten.json()['result'][0]['global_entity_id']
            file_tags = res_parent_gotten.json()['result'][0]['tags']
            if not file_tags:
                file_tags = []
            ## add approval copy tag
            add_url = ConfigClass.data_ops_host + "/v2/containers/{}/tags".format(project_id)
            file_tags.append(ConfigClass.copied_with_approval)
            add_post = {
                "taglist": file_tags,
                "geid": parent_geid,
                "internal": True
            }
            respon_add_copy_tag = requests.post(add_url, json=add_post)
            if respon_add_copy_tag.status_code == 200:
                return "Succeed"
            else:
                return {
                    "error": "add_approval_copy_for_neo4ja in neo4j failed",
                    "errorcode": respon_add_copy_tag.status_code,
                    "error_msg": respon_add_copy_tag.text + "------------" + add_url,
                    "payload": str(add_post)
                }

        except Exception as e:
                return {
                    "error": "archive meta in neo4j failed",
                    "errorcode": 500,
                    "error_msg": str(e)
                }

    def create_trash_node_in_neo4j(self, input_path, trash_path, trash_geid):
        trash_url = ConfigClass.ENTITY_INFO_SERVICE + "/v1/files/trash"
        payload = {
            "trash_full_path": trash_path,
            "full_path": input_path,
            "trash_geid": trash_geid,
        }
        res = requests.post(url=trash_url, json=payload)
        if res.status_code == 200:
            print('create_trash_node_in_neo4j: ', res.json())
        else:
            print({
                "error": "create_trash_node_in_neo4j failed",
                "errorcode": res.status_code,
                "error_msg": res.text + "-----------" + trash_url,
                "request_body": str(payload)
            })
            return {
                "error": "create_trash_node_in_neo4j failed",
                "errorcode": res.status_code,
                "error_msg": res.text + "-----------" + trash_url,
                "request_body": str(payload)
            }

    def fetch_guid(self):
         ## fetch global entity id
        entity_id_url = ConfigClass.UTILITY_SERVICE + "/v1/utility/id?entity_type=file_data"
        respon_entity_id_fetched = requests.get(entity_id_url)
        if respon_entity_id_fetched.status_code == 200:
            pass
        else:
            raise Exception('Entity id fetch failed: ' + entity_id_url + ": " + str(respon_entity_id_fetched.text))
        trash_geid = respon_entity_id_fetched.json()['result']
        return trash_geid
