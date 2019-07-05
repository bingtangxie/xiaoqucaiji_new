# -*- coding: utf-8 -*-
import scrapy
import redis
import pymongo
import re
import json
from fake_useragent import UserAgent
from scrapy.conf import settings
from xqcj_new.items import XqcjNewItem


class Anjuke02Spider(scrapy.Spider):
    name = 'anjuke_02'
    allowed_domains = ['anjuke.com']
    # start_urls = ['http://anjuke.com/']

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
        spider.ua = UserAgent()
        return spider

    def start_requests(self):
        base_key = re.search("(.+)_\d+", Anjuke02Spider.name).group(1)
        r_key = base_key + "_detail_url_hashtable"
        # urls = self.redis.hkeys(r_key)
        # for url in urls:
        #     detail_url = url.decode()
        #     if not self.redis.sismember(base_key, detail_url):
        #         data = json.loads(self.redis.hget(r_key, detail_url))
        #         data['r_key'] = r_key
        #         yield scrapy.Request(url=detail_url, callback=self.parse_detail, meta=data)
        urls = self.redis.smembers("anjuke")
        for url in urls:
            detail_url = url.decode()
            if not self.redis.sismember(base_key, detail_url):
                data = json.loads(self.redis.hget(r_key, detail_url))
                data['r_key'] = r_key
                yield scrapy.Request(url=detail_url, callback=self.parse_detail, meta=data)

    def parse_detail(self, response):
        search_type = response.meta['search_type']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street'].strip()
        housing_url = response.meta['housing_url']
        r_key = response.meta['r_key']
        if search_type == "xinfang":
            basic_info = response.xpath("//div[@class='basic-info']")
            housing_name = basic_info.xpath("./h1/text()").extract_first()
            housing_alias_raw = basic_info.xpath("./p")
            price = response.xpath("//dd[@class='price']/p")
            undefined_price = price[0].xpath("./i[@class='sp-price other-price']/text()").extract_first()
            zb_price = price[0].xpath("./em/text()").extract_first()
            zb_price_unit = price[0].xpath("./span/text()").extract_first()
            if undefined_price:
                housing_price = undefined_price
            else:
                housing_price = zb_price + " " + zb_price_unit
            if housing_alias_raw:
                housing_alias = housing_alias_raw.xpath("./text()").extract_first().lstrip("别名：")
            else:
                housing_alias = ""
            more_info_url = response.xpath("//div[@class='more-info']/a/@href").extract_first()
            # yield scrapy.Request(url=more_info_url, callback=self.parse_detail_info,
            #                      meta={'city': city, 'district': district, 'street': street, 'housing_url': housing_url,
            #                            'housing_detail_url': more_info_url, 'housing_name': housing_name,
            #                            'housing_alias': housing_alias, 'housing_price': housing_price,
            #                            'search_type': search_type, 'r_key': r_key})
        if search_type == "xiaoqu":
            items = XqcjNewItem()
            housing_name = response.xpath("//div[@class='comm-title']/h1/text()").extract_first().strip()
            housing_address = response.xpath("//div[@class='comm-title']/h1/span/text()").extract_first()
            housing_price = re.search("comm_midprice\":\"(\d+)\",", response.text)
            housing_info = response.xpath("//dl[@class='basic-parms-mod']")[0]
            if housing_price:
                housing_price = housing_price.group(1) + "元/㎡"
            else:
                housing_price = "暂无"
            dt = housing_info.xpath("./dt")
            dd = housing_info.xpath("./dd")
            dt_num = len(dt)
            for i in range(dt_num):
                label_name = dt[i].xpath("./text()").extract_first().strip("：")
                label_value = dd[i].xpath("./text()").extract_first()
                if label_name == "物业类型":
                    items['property_type'] = label_value
                if label_name == "物业费":
                    items['property_fee'] = label_value
                if label_name == "总建面积":
                    items['building_area'] = label_value
                if label_name == "总户数":
                    items['house_total'] = label_value
                if label_name == "建造年代":
                    items['built_year'] = label_value
                if label_name == "停车位":
                    items['parking_place'] = label_value
                if label_name == "绿化率":
                    items['greening_rate'] = label_value
                if label_name == "物业公司":
                    items['property_company'] = label_value
                if label_name == "所属商圈":
                    items['business_circle'] = label_value
                if label_name == "容  积  率":
                    items['capacity_rate'] = label_value
                if label_name == "开  发  商":
                    items['developer'] = label_value
            items['city'] = city
            items['district'] = district
            items['street'] = street
            items['housing_name'] = housing_name
            items['housing_url'] = housing_url
            items['housing_address'] = housing_address
            items['flag'] = search_type
            if self.redis.hexists(r_key, housing_url):
                yield items

    def parse_detail_info(self, response):
        items = XqcjNewItem()
        search_type = response.meta['search_type']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        r_key = response.meta['r_key']
        if search_type == "xinfang":
            housing_name = response.meta['housing_name']
            housing_alias = response.meta['housing_alias']
            housing_price = response.meta['housing_price']
            housing_url = response.meta['housing_url']
            housing_detail_url = response.meta['housing_detail_url']
            # print(city, district, street, housing_name, housing_alias, housing_price, housing_url, housing_detail_url)
            info_blocks = response.xpath("//div[@class='can-left']/div[@class='can-item']")
            for cantainer in info_blocks:
                head = cantainer.xpath("./div[@class='can-head']/h4/text()").extract_first()
                if head == "基本信息" or "小区情况":
                    can = cantainer.xpath("./div[@class='can-border']/ul[@class='list']/li")
                    for pairs in can:
                        label = pairs.xpath("./div[@class='name']/text()").extract_first()
                        if label:
                            label = label.strip()
                        value = pairs.xpath("./div[@class='des']/text()").extract_first()
                        if value:
                            value = value.strip()
                        if label == "物业类型":
                            items['property_type'] = value
                        if label == "开发商":
                            value = pairs.xpath("./div[@class='des']/a/text()").extract_first() or value
                            items['developer'] = value
                        if label == "楼盘地址":
                            items['housing_address'] = value
                        if label == "建筑类型":
                            items['building_type'] = value
                        if label == "规划户数":
                            items['house_total'] = value
                        if label == "物业管理费":
                            items['property_fee'] = value
                        if label == "物业公司":
                            value = pairs.xpath("./div[@class='des']/a/text()").extract_first() or value
                            items['property_company'] = value
                        if label == "车位数":
                            items['parking_place'] = value
                        if label == "车位比":
                            items['parking_ratio'] = value
                        if label == "绿化率":
                            items['greening_rate'] = value
                        if label == "产权年限":
                            items['right_years'] = value
                        if label == "容积率":
                            items['capacity_rate'] = value
            items['city'] = city
            items['district'] = district
            items['street'] = street
            items['housing_name'] = housing_name
            items['housing_alias'] = housing_alias
            items['housing_price'] = housing_price
            items['housing_url'] = housing_url
            items['housing_detail_url'] = housing_detail_url
            items['flag'] = search_type
            if self.redis.hexists(r_key, housing_url):
                yield items
