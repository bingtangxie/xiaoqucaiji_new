# -*- coding: utf-8 -*-
import scrapy
import re
import json
import redis
import pymongo
from xqcj_new.items import XqcjNewItem
from scrapy.conf import settings


class Tongcheng5801Spider(scrapy.Spider):
    name = 'tongcheng58_01'
    allowed_domains = ['58.com']
    start_urls = ['https://www.58.com/changecity.html']

    def __init__(self):
        super().__init__()
        redis_host = settings['REDIS_HOST']
        redis_port = settings['REDIS_PORT']
        redis_db = settings['REDIS_DB']
        redis_password = settings['REDIS_PASS']
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        redis_host = spider.settings['REDIS_HOST']
        redis_port = spider.settings['REDIS_PORT']
        redis_db = spider.settings['REDIS_DB']
        redis_password = spider.settings['REDIS_PASS']
        mongo_host = spider.settings['MONGO_HOST']
        mongo_port = spider.settings['MONGO_PORT']
        mongo_db = spider.settings['MONGO_DB']
        spider.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)
        spider.db = pymongo.MongoClient(host=mongo_host, port=mongo_port)[mongo_db]
        return spider

    def parse(self, response):
        res = response.text
        independent_city = re.search("independentCityList = (.+?)}", res, re.S).group(1) + "}"
        cities = re.search("cityList = (.+?)<\/script>", res, re.S).group(1)
        city_codes = json.loads(cities)
        independent_city_codes = json.loads(independent_city)
        city_codes.pop("其他")
        city_codes.pop("海外")
        all_keys = list(city_codes.keys())
        all_keys_length = len(all_keys)
        # for i in range(0, int(all_keys_length * 0.25)):
        #     province = all_keys[i]
        #     for city in city_codes[province]:
        #         code = city_codes[province][city].split("|")[0]
        #         url = "https://{code}.58.com/xiaoqu".format(code=code)
        #         yield scrapy.Request(url=url, callback=self.parse_district, meta={'province': province, 'city': city.strip()})

        # for i in range(int(all_keys_length * 0.25), int(all_keys_length * 0.5)):
        #     province = all_keys[i]
        #     for city in city_codes[province]:
        #         code = city_codes[province][city].split("|")[0]
        #         url = "https://{code}.58.com/xiaoqu".format(code=code)
        #         yield scrapy.Request(url=url, callback=self.parse_district, meta={'province': province, 'city': city.strip()})
        #
        # for i in range(int(all_keys_length * 0.5), int(all_keys_length * 0.75)):
        #     province = all_keys[i]
        #     for city in city_codes[province]:
        #         code = city_codes[province][city].split("|")[0]
        #         url = "https://{code}.58.com/xiaoqu".format(code=code)
        #         yield scrapy.Request(url=url, callback=self.parse_district, meta={'province': province, 'city': city.strip()})

        # for i in range(int(all_keys_length * 0.75), all_keys_length):
        #     province = all_keys[i]
        #     for city in city_codes[province]:
        #         code = city_codes[province][city].split("|")[0]
        #         url = "https://{code}.58.com/xiaoqu".format(code=code)
        #         yield scrapy.Request(url=url, callback=self.parse_district, meta={'province': province, 'city': city.strip()})

        for indep_city in independent_city_codes:
            in_code = independent_city_codes[indep_city].split("|")[0]
            in_url = "https://{code}.58.com/xiaoqu".format(code=in_code)
            yield scrapy.Request(url=in_url, callback=self.parse_district, meta={'province': indep_city.strip(), 'city': indep_city.strip()})

    def parse_district(self, response):
        province = response.meta['province']
        city = response.meta['city']
        districts = response.xpath("//dl[@class='secitem']")[0].xpath("./dd/a")
        for district in districts:
            district_name = district.xpath("./text()").extract_first()
            if district_name != "不限":
                district_code = district.xpath("./@value").extract_first()
                district_url = response.url + district_code
                yield scrapy.Request(url=district_url, callback=self.parse_street, meta={'province': province, 'city': city, 'district': district_name.strip()})

    def parse_street(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        streets = response.xpath("//div[@id='qySelectSecond']/a")
        for street in streets:
            street_name = street.xpath("./text()").extract_first()
            street_code = street.xpath("./@value").extract_first()
            street_url = re.search("(.+xiaoqu/)\d+/", response.url).group(1) + street_code
            yield scrapy.Request(url=street_url, callback=self.parse_list, meta={'province': province, 'city': city, 'district': district, 'street': street_name.strip()})

    def parse_list(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        xiaoqu_list = response.xpath("//div[@class='list-info']/h2/a")
        if xiaoqu_list:
            for xiaoqu in xiaoqu_list:
                housing_url = xiaoqu.xpath("./@href").extract_first()
                data = {"province": province, "city": city, "district": district, "street": street, "housing_url": housing_url}
                r_key = re.search("(.+)_\d+", Tongcheng5801Spider.name).group(1) + "_detail_url_hashtable"
                self.redis.hset(r_key, housing_url, json.dumps(data))
        else:
            # 返回列表为空
            pass
