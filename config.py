class ConfigClass(object):
    #greenroom queue
    gm_queue_endpoint = 'message-bus-greenroom.greenroom'
    gm_username = 'greenroom'
    gm_password = 'indoc101'

    # data_lake
    data_lake = "/data/vre-storage"

    #queue service url
    service_queue_send_msg_url = "http://queue-producer.greenroom:6060/v1/send_message"

    #data ops url
    data_ops_host = "http://dataops-gr.greenroom:5063"

    #k8s_namespace
    k8s_namespace = "greenroom"

    #pipeline_job_peek_interval
    pipeline_job_peek_interval = 60