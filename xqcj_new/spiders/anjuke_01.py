# -*- coding: utf-8 -*-
import scrapy
import redis
import pymongo
import re
import json
from scrapy.conf import settings


class Anjuke01Spider(scrapy.Spider):
    name = 'anjuke_01'
    allowed_domains = ['anjuke.com']
    start_urls = ['https://www.anjuke.com/sy-city.html']

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
        content = response.xpath('//div[@class="letter_city"]/ul/li')
        entrance_list = []
        for block in content:
            label = block.xpath("./label/text()").extract_first()
            cities = block.xpath("./div[@class='city_list']/a")
            if label == "其他":
                pass
            else:
                for city in cities:
                    city_name = city.xpath("./text()").extract_first()
                    city_url = city.xpath("./@href").extract_first()
                    entrance_list.append({"city_url": city_url, "city": city_name})
        entrance_list_length = len(entrance_list)
        # for i in range(0, int(entrance_list_length * 0.25)):
        #     url = entrance_list[i]['city_url']
        #     city = entrance_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city, meta={"city": city})

        # for i in range(int(entrance_list_length * 0.25), int(entrance_list_length * 0.5)):
        #     url = entrance_list[i]['city_url']
        #     city = entrance_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city, meta={"city": city})

        # for i in range(int(entrance_list_length * 0.5), int(entrance_list_length * 0.75)):
        #     url = entrance_list[i]['city_url']
        #     city = entrance_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city, meta={"city": city})

        for i in range(int(entrance_list_length * 0.25), entrance_list_length):
            url = entrance_list[i]['city_url']
            city = entrance_list[i]['city']
            yield scrapy.Request(url=url, callback=self.parse_city, meta={"city": city})

    def parse_city(self, response):
        # print(response.text)
        header_nav = response.xpath("//ul[@class='L_tabsnew']/li")
        city = response.meta['city']
        if header_nav:
            for header_item in header_nav:
                label = header_item.xpath("./a/text()").extract_first().strip()
                if label == "新 房":
                    xinfang_url = header_item.xpath("./a/@href").extract_first()
                    search_type = "xinfang"
                    yield scrapy.Request(url=xinfang_url, callback=self.parse_district,
                                         meta={'city': city, 'search_type': search_type})
                if label == "二手房":
                    ershoufang_url = header_item.xpath("./a/@href").extract_first()
                    search_type = "ershoufang"
                    yield scrapy.Request(url=ershoufang_url, callback=self.parse_district,
                                         meta={'city': city, 'search_type': search_type})
        else:
            pass

    def parse_district(self, response):
        search_type = response.meta['search_type']
        city = response.meta['city']
        if search_type == "xinfang":
            district_streets = response.xpath("//div[@class='item-list area-bd']")
            districts = district_streets.xpath("./div[@class='filter']/a")
            streets_blocks = district_streets.xpath("./div[@class='filter-sub']")
            for i in range(len(districts)):
                if i <= len(streets_blocks):
                    district_name = districts[i].xpath("./text()").extract_first()
                    streets = streets_blocks[i].xpath("./a")
                    for street in streets:
                        street_name = street.xpath("./text()").extract_first()
                        street_url = street.xpath("./@href").extract_first()
                        yield scrapy.Request(url=street_url, callback=self.parse_list,
                                             meta={'city': city, 'district': district_name, 'street': street_name,
                                                   'search_type': search_type})
        if search_type == "ershoufang":
            pass

    def parse_list(self, response):
        search_type = response.meta['search_type']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        if search_type == "xinfang":
            buildings = response.xpath("//div[@class='infos']")
            for building in buildings:
                detail_url_raw = building.xpath("./a[@class='lp-name']")
                if detail_url_raw:
                    detail_url = detail_url_raw.xpath("./@href").extract_first()
                    r_key = re.search("(.+)_\d+", Anjuke01Spider.name).group(1) + "_detail_url_hashtable"
                    data = {"city": city, "district": district, "street": street, "housing_url": detail_url, "search_type": search_type}
                    self.redis.hset(r_key, detail_url, json.dumps(data))
                # if not self.redis.sismember(Anjuke01Spider.name, detail_url):
                #     yield scrapy.Request(url=detail_url, callback=self.parse_detail,
                #                          meta={'city': city, 'district': district, 'street': street,
                #                                'housing_url': detail_url, 'search_type': search_type})
                pagination = response.xpath("//div[@class='pagination']")
                next_page = pagination.xpath("./a[@class='next-page next-link']")
                if next_page:
                    next_url = next_page[0].xpath("./@href").extract_first()
                    yield scrapy.Request(url=next_url, callback=self.parse_list,
                                         meta={'city': city, 'district': district, 'street': street,
                                               'search_type': search_type})