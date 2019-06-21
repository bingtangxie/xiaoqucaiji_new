# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime


class XqcjNewPipeline(object):

    def process_item(self, item, spider):
        if spider.name == "fangtianxia_02":
            data = dict(item)
            data['created'] = datetime.now()
            fields = [
                'province',
                'city',
                'district',
                'street',
                'housing_id',
                'housing_url',
                'housing_name',
                'housing_alias',
                'housing_address',
                'housing_price',
                'building_type',
                'property_fee',
                'property_company',
                'property_type',
                'developer',
                'building_total',
                'house_total',
                'greening_rate',
                'right_years',
                'area',
                'capacity_rate',
                'water_supply',
                'power_supply',
                'parking_ratio',
                'heating_mode',
                'parking_place',
                'business_circle',
                'housing_detail_url',
                'flag',
                'traffic',
                'shopping',
                'hospitail',
                'postoffice',
                'bank',
                'internal_suite',
                'other',
                'building_area',
                'postcode',
                'gas_supply',
                'communication_device',
                'health_service',
                'commnity_entrance',
                'education_facility'
            ]
            for i in range(len(fields)):
                if fields[i] not in data:
                    data[fields[i]] = ""
            spider.db[spider.name].insert(data)
            spider.redis.sadd(spider.name, data['housing_url'])
