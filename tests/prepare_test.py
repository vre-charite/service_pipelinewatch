import requests
from config import ConfigClass


class SetupTest:
    def __init__(self, log):
        self.log = log
        self.log.info("Test Start")
