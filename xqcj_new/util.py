import requests
import redis
from scrapy.conf import settings


class HandleProxy:
    def __init__(self):
        self.proxy_provider = settings['PROXY_PROVIDER_API']
        self.redis = redis.StrictRedis(host=settings['REDIS_HOST'], port=settings['REDIS_PORT'],
                                       db=settings['REDIS_DB'],
                                       password=settings['REDIS_PASS'])
        self.block_proxy_set = settings['BLOCK_PROXY_SET']
        # self.unblocked_proxy_set = settings['UNBLOCKED_PROXY_SET_PREFIX'] + "_" + datetime.now().strftime("%Y%m%d")
        self.unblocked_proxy_set = settings['UNBLOCKED_PROXY_SET']

    def test_ajk(self, ip):
        available = False
        proxy = {
            "http": ip
        }
        # data = {
        #     "searchName": "",
        #     "searchArea": "",
        #     "searchIndustry": "",
        #     "centerPlat": "",
        #     "businessType": "招标项目",
        #     "searchTimeStart": "",
        #     "searchTimeStop": "",
        #     "timeTypeParam": "",
        #     "bulletinIssnTime": "",
        #     "bulletinIssnTimeStart": "",
        #     "bulletinIssnTimeStop": "",
        #     "pageNo": "1",
        #     "row": "15"
        # }
        try:
            # result = requests.post(url="http://www.cebpubservice.com/ctpsp_iiss/searchbusinesstypebeforedooraction/"
            #                        "getStringMethod.do", data=data, proxies=proxy, timeout=30)
            result = requests.get(url="http://www.baidu.com", proxies=proxy, timeout=10)

            if result.status_code == 200:
                available = True
                print(result.text)
            else:
                available = False
        except Exception as e:
            available = False
        return available

    def available_proxy(self):
        while True:
            result = requests.get(self.proxy_provider)
            ip = result.text
            proxy = "http://" + ip
            print("正在寻找合适的代理IP")
            if not self.redis.sismember(self.block_proxy_set, proxy) and not self.redis.zscore(self.unblocked_proxy_set,
                                                                                               proxy):
                if self.test_ajk(ip):
                    return ip


if __name__ == '__main__':
    handle = HandleProxy()
    print(handle.available_proxy())
