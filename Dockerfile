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

FROM python:3.7-buster
RUN groupadd --gid 1004 deploy \
    && useradd --home-dir /home/deploy --create-home --uid 1004 \
        --gid 1004 --shell /bin/sh --skel /dev/null deploy

ARG PIP_USERNAME
ARG PIP_PASSWORD

WORKDIR /home/deploy
COPY .  ./
RUN chown -R deploy:deploy /home/deploy
USER deploy
#RUN chmod +x /home/deploy/gunicorn_starter.sh
RUN chmod +x /home/deploy/worker_k8s_job_watch.py
RUN PIP_USERNAME=$PIP_USERNAME PIP_PASSWORD=$PIP_PASSWORD pip install --no-cache-dir -r requirements.txt -r internal_requirements.txt --user
ENV PATH="/home/deploy/.local/bin:${PATH}"
# CMD ["./gunicorn_starter.sh"]
