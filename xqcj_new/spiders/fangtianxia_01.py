# -*- coding: utf-8 -*-
import scrapy
from scrapy.conf import settings
import redis
import pymongo
import re
import json
# from xqcj_new.items import XqcjNewItem


class Fangtianxia01Spider(scrapy.Spider):
    name = 'fangtianxia_01'
    allowed_domains = ['fang.com']
    start_urls = ['https://www.fang.com/SoufunFamily.htm']

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
        content = response.xpath("//table[@id='senfe']/tr")
        province = None
        entrance_list = []
        for item in content:
            ss = "".join(item.xpath("./td")[1].xpath("./strong/text()").extract())
            cities = item.xpath("./td")[2].xpath("./a")
            if ss:
                if ss == "直辖市":
                    province = ""
                elif ss == '\xa0':
                    pass
                else:
                    province = ss
            else:
                pass
            for city in cities:
                city_name = city.xpath("./text()").extract()[0]
                city_url = city.xpath("./@href").extract()[0]
                if province != "其它":
                    if city_name in ["北京", "上海", "重庆", "天津"]:
                        province = city_name
                    entrance_list.append({"province": province, "city": city_name, "city_url": city_url})
        entrance_list_length = len(entrance_list)
        # for i in range(int(entrance_list_length * 0.25)):
        #     url = entrance_list[i]['city_url']
        #     province = entrance_list[i]['province']
        #     city = entrance_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city,
        #                          meta={'province': province, 'city': city})

        # for i in range(int(entrance_list_length * 0.25), int(entrance_list_length * 0.5)):
        #     url = entrance_list[i]['city_url']
        #     province = entrance_list[i]['province']
        #     city = entrance_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city,
        #                          meta={'province': province, 'city': city})

        # for i in range(int(entrance_list_length * 0.5), int(entrance_list_length * 0.75)):
        #     url = entrance_list[i]['city_url']
        #     province = entrance_list[i]['province']
        #     city = entrance_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city,
        #                          meta={'province': province, 'city': city})

        for i in range(int(entrance_list_length * 0.75), entrance_list_length):
            url = entrance_list[i]['city_url']
            province = entrance_list[i]['province']
            city = entrance_list[i]['city']
            yield scrapy.Request(url=url, callback=self.parse_city,
                                 meta={'province': province, 'city': city})

    def parse_city(self, response):
        xinfang = response.xpath("//div[@track-id='newhouse']/div[@class='s4Box']/a")
        ershoufang = response.xpath("//a[@id='dsy_D01_24']")
        province = response.meta['province']
        city = response.meta['city']
        if xinfang:
            link_url = xinfang.xpath('./@href').extract()[0]
            search_type = 'xinfang'
            yield scrapy.Request(url=link_url, callback=self.parse_district, meta={'province': province, 'city': city, 'search_type': search_type})
        if ershoufang:
            link_url = ershoufang.xpath('./@href').extract()[0]
            search_type = 'ershoufang'
            yield scrapy.Request(url=link_url, callback=self.parse_district,
                                 meta={'province': province, 'city': city, 'search_type': search_type})

    def parse_district(self, response):
        search_type = response.meta['search_type']
        province = response.meta['province']
        city = response.meta['city']
        if search_type == 'xinfang':
            districts = response.xpath("//dd[@id='sjina_D03_05']/ul/li[@id='quyu_name']/a")
            # xinfang_house_total = response.xpath("//li[@id='sjina_C01_30']/a/span/text()").extract_first()
            # if xinfang_house_total:
            #     xinfang_city_house_total = re.search("\((\d+)\)", xinfang_house_total).group(1)
            # self.sum_xinfang = self.sum_xinfang + int(xinfang_city_house_total)
            # print(province, city, city_house_total, self.sum)
            if districts:
                for district in districts:
                    if district.xpath("./@style").extract():
                        pass
                    elif district.xpath("./@href").extract()[0] == '#no':
                        pass
                    else:
                        link_uri = district.xpath("./@href").extract()[0]
                        district_name = district.xpath("./text()").extract()[0]
                        link_url = response.urljoin(link_uri)
                        yield scrapy.Request(url=link_url, callback=self.parse_street,
                                             meta={'province': province, 'city': city, 'district': district_name,
                                                   'search_type': search_type})
            else:
                print("# 可能出现验证码")
                pass
                # 可能出现验证码
        if search_type == "ershoufang":
            districts = response.xpath("//div[@class='qxName']/a")
            if districts:
                for district in districts:
                    district_name = district.xpath("./text()").extract_first()
                    if district_name == "不限":
                        pass
                    else:
                        district_uri = district.xpath("./@href").extract_first()
                        district_url = response.urljoin(district_uri)
                        yield scrapy.Request(url=district_url, callback=self.parse_street,
                                             meta={'province': province, 'city': city, 'district': district_name,
                                                   'search_type': search_type})
            else:
                print("# 可能出现验证码")
                pass
                # 可能出现验证码

    def parse_street(self, response):
        search_type = response.meta['search_type']
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        if search_type == 'xinfang':
            streets = response.xpath("//ol[@style='overflow:hidden;']/li/a")
            if streets:
                for street in streets:
                    street_name = street.xpath("./text()").extract()[0]
                    street_uri = street.xpath("./@href").extract()[0]
                    street_url = response.urljoin(street_uri)
                    yield scrapy.Request(url=street_url, callback=self.parse_list,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street_name, 'search_type': search_type})
            else:
                pass
        if search_type == "ershoufang":
            streets = response.xpath("//div[@class='shangQuan']/p[@id='shangQuancontain']/a")
            if streets:
                for street in streets:
                    street_name = street.xpath("./text()").extract_first()
                    if street_name == "不限":
                        pass
                    else:
                        street_uri = street.xpath("./@href").extract_first()
                        street_url = response.urljoin(street_uri)
                        yield scrapy.Request(url=street_url, callback=self.parse_list,
                                             meta={'province': province, 'city': city, 'district': district,
                                                   'street': street_name, 'search_type': search_type})
            else:
                pass

    def parse_list(self, response):
        search_type = response.meta['search_type']
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        if search_type == 'xinfang':
            housing_list = response.xpath("//div[@class='nlcd_name']/a")
            if housing_list:
                for housing in housing_list:
                    housing_url = "https:" + housing.xpath("./@href").extract_first()
                    if housing_url:
                        data = {"province": province, "city": city, "district": district, "street": street, "housing_url": housing_url, "search_type": search_type}
                        r_key = re.search("(.+)_\d+", Fangtianxia01Spider.name).group(1) + "_detail_url_hashtable"
                        self.redis.hset(r_key, housing_url, json.dumps(data))
                        # if not self.redis.sismember(Fangtianxia01Spider.name, housing_url):
                        #     yield scrapy.Request(url=housing_url, callback=self.parse_detail,
                        #                          meta={'province': province, 'city': city, 'district': district,
                        #                                'street': street, 'housing_url': housing_url,
                        #                                'search_type': search_type})
                pagination = response.xpath("//div[@class='page']/ul/li[@class='fr']/a[@class='next']")
                if pagination:
                    next_page_uri = pagination[0].xpath("./@href").extract_first()
                    next_page_url = response.urljoin(next_page_uri)
                    yield scrapy.Request(url=next_page_url, callback=self.parse_list,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street, 'search_type': search_type})
                else:
                    pass
            else:
                # 列表页面为空
                pass
        if search_type == "ershoufang":
            housing_list = response.xpath("//div[@class='houseList']/div[@class='list rel mousediv']")
            if housing_list:
                for housing in housing_list:
                    housing_url_raw = housing.xpath(".//a[@class='plotTit']/@href").extract_first()
                    housing_name = housing.xpath(".//a[@class='plotTit']/text()").extract_first()
                    if housing_url_raw[0:2] == "//":
                        housing_url = "https:" + housing_url_raw
                    else:
                        housing_url = ""  # 没有详细信息就过滤掉了， 类似https://nb.esf.fang.com/house-xm2011122880/
                    undefined_price = housing.xpath("./div[@class='listRiconwrap']/p[@class='not_data']")
                    if undefined_price:
                        housing_price = undefined_price.xpath("./text()").extract_first()
                    else:
                        housing_price = housing.xpath("./div[@class='listRiconwrap']/p[@class='priceAverage']/span")[
                                            0].xpath("./text()").extract_first() + "元/㎡"
                    if housing_url:
                        data = {"province": province, "city": city, "district": district, "street": street,
                                "housing_url": housing_url, "search_type": search_type, "housing_name": housing_name, "housing_price": housing_price}
                        r_key = re.search("(.+)_\d+", Fangtianxia01Spider.name).group(1) + "_detail_url_hashtable"
                        self.redis.hset(r_key, housing_url, json.dumps(data))
                        # if not self.redis.sismember(FangtianxiaSpider.name, housing_url):
                        #     yield scrapy.Request(url=housing_url, callback=self.parse_detail,
                        #                          meta={'province': province, 'city': city, 'district': district,
                        #                                'street': street, 'housing_name': housing_name,
                        #                                'housing_url': housing_url, 'housing_price': housing_price,
                        #                                'search_type': search_type})
                pagination = response.xpath("//a[@id='PageControl1_hlk_next']")
                if pagination:
                    next_uri = pagination[0].xpath("./@href").extract_first()
                    next_url = response.urljoin(next_uri)
                    yield scrapy.Request(url=next_url, callback=self.parse_list,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street, 'search_type': search_type})
                else:
                    pass
            else:
                pass
