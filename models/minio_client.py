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

import requests
from minio import Minio
from minio.commonconfig import Tags
from minio.credentials.providers import ClientGrantsProvider
from minio.commonconfig import REPLACE, CopySource
from config import ConfigClass


class Minio_Client():

    def __init__(self):
        # Temperary use the credential
        self.client = Minio(
            ConfigClass.MINIO_ENDPOINT, 
            access_key=ConfigClass.MINIO_ACCESS_KEY,
            secret_key=ConfigClass.MINIO_SECRET_KEY,
            secure=ConfigClass.MINIO_HTTPS)



    def _get_jwt(self):
        # first login with keycloak
        username = "admin"
        password = ConfigClass.MINIO_TEST_PASS
        payload = {
            "grant_type": "password",
            "username": username,
            "password": password,
            "client_id": ConfigClass.MINIO_OPENID_CLIENT,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        result = requests.post(
            ConfigClass.KEYCLOAK_ENDPOINT, data=payload, headers=headers)
        keycloak_access_token = result.json().get("access_token")
        return result.json()

    def get_provider(self):
        minio_http = ("https://" if ConfigClass.MINIO_HTTPS else "http://") + \
            ConfigClass.MINIO_ENDPOINT
        print(minio_http)
        provider = ClientGrantsProvider(
            self._get_jwt,
            minio_http,
        )

        return provider.retrieve()

    def copy_object(self, bucket, obj, source_bucket, source_obj):
        result = self.client.copy_object(
            bucket,
            obj,
            CopySource(source_bucket, source_obj),
        )
        return result

    def fput_object(self, bucket_name, object_name, file_path):
        result = self.client.fput_object(
            bucket_name,
            object_name,
            file_path
        )
        return result