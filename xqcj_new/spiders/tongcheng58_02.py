# -*- coding: utf-8 -*-
import scrapy
import redis
import pymongo
import re
import json
from scrapy.conf import settings
from xqcj_new.items import XqcjNewItem


class Tongcheng5802Spider(scrapy.Spider):
    name = 'tongcheng58_02'
    allowed_domains = ['58.com']
    # start_urls = ['http://58.com/']

    def __init__(self):
        super().__init__()
        redis_host = settings['REDIS_HOST']
        redis_port = settings['REDIS_PORT']
        redis_db = settings['REDIS_DB']
        redis_password = settings['REDIS_PASS']
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)
        # self.redis0 = redis.StrictRedis(host=redis_host, port=redis_port, db=0, password=redis_password)
        self.sum_xinfang = 0
        self.sum_ershoufang = 0

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

    def start_requests(self):
        base_key = re.search("(.+)_\d+", Tongcheng5802Spider.name).group(1)
        r_key = base_key + "_detail_url_hashtable"
        urls = self.redis.hkeys(r_key)
        for url in urls:
            detail_url = url.decode()
            if not self.redis.sismember(base_key, detail_url):
                data = json.loads(self.redis.hget(r_key, detail_url))
                data['r_key'] = r_key
                data['housing_url'] = detail_url
                yield scrapy.Request(url=detail_url, callback=self.parse_detail, meta=data)

    def parse_detail(self, response):
        r_key = response.meta['r_key']
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        housing_url = response.meta['housing_url']
        items = XqcjNewItem()
        housing_name = response.xpath("//div[@class='title-bar']/span[@class='title']/text()").extract_first()
        housing_price_raw = response.xpath("//div[@class='price-container']")
        if housing_price_raw:
            price = housing_price_raw[0].xpath("./span[@class='price']/text()").extract_first()
            unit = housing_price_raw[0].xpath("./span[@class='unit']/text()").extract_first()
            housing_price = price + " " + unit
        else:
            housing_price = ""
        info_tb = response.xpath("//table[@class='info-tb']/tr")
        data_dict = {}
        for data in info_tb:
            td_block = data.xpath("./td")
            if len(td_block) == 2:
                key = td_block[0].xpath("./text()").extract_first()
                value = td_block[1].xpath("./@title").extract_first()
                if key not in data_dict:
                    data_dict[key] = value
            if len(td_block) == 4:
                key1 = td_block[0].xpath("./text()").extract_first()
                value1 = td_block[1].xpath("./@title").extract_first()
                if key1 not in data_dict:
                    data_dict[key1] = value1
                key2 = td_block[2].xpath("./text()").extract_first()
                value2 = td_block[3].xpath("./@title").extract_first()
                if key2 not in data_dict:
                    data_dict[key2] = value2
        for label in data_dict:
            if label == "商圈区域":
                items['business_circle'] = data_dict[label]
            if label == "详细地址":
                items['housing_address'] = data_dict[label]
            if label == "建筑类别":
                items['building_type'] = data_dict[label]
            if label == "总住户数":
                items['house_total'] = data_dict[label]
            if label == "产权类别":
                items['property_type'] = data_dict[label]
            if label == "物业费用":
                items['property_fee'] = data_dict[label]
            if label == "产权年限":
                items['right_years'] = data_dict[label]
            if label == "容积率":
                items['capacity_rate'] = data_dict[label]
            if label == "建筑年代":
                items['built_year'] = data_dict[label]
            if label == "绿化率":
                items['greening_rate'] = data_dict[label]
            if label == "占地面积":
                items['area'] = data_dict[label]
            if label == "建筑面积":
                items['building_area'] = data_dict[label]
            if label == "停车位":
                items['parking_place'] = data_dict[label]
            if label == "物业公司":
                items['property_company'] = data_dict[label]
            if label == "开发商":
                items['developer'] = data_dict[label]
        items['province'] = province
        items['city'] = city
        items['district'] = district
        items['street'] = street
        items['housing_url'] = housing_url
        items['housing_name'] = housing_name
        items['housing_price'] = housing_price
        if self.redis.hexists(r_key, housing_url):
            yield items