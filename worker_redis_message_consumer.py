#!/usr/bin/env python3
from services.data_providers.redis import SrvRedisSingleton

def main():
    srv_redis = SrvRedisSingleton()
    sub = srv_redis.subscriber('pipelinewatch')
    for info in sub.listen():
        print(info)

if __name__ == '__main__':
    main()
