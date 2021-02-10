import requests
from config import ConfigClass

def get_project_by_code(project_code):
    url = ConfigClass.service_bff + '/project/{}'.format(project_code)
    res = requests.get(url)
    if res.status_code == 200:
        return res.json()['result']
    else:
        return {
            "error": True,
            "errorcode": res.status_code,
            "msg": "failed on getting project by codes"
        }