# import pymongo
# import redis
#
#
# mongo = pymongo.MongoClient(host='127.0.0.1', port=27017)
# mongo_client = mongo["xiaoqucaiji_test"]["tongcheng58"]
# rds = redis.StrictRedis(host='127.0.0.1', port=6379, db=1)
# res = mongo_client.find({})
# for item in res:
#     rds.sadd("tongcheng58", item['housing_url'])

# import pymongo
# import redis
# import re
#
#
# mongo = pymongo.MongoClient(host='127.0.0.1', port=27017)
# mongo_client = mongo["xiaoqucaiji_test"]["fangtianxia"]
# rds = redis.StrictRedis(host='127.0.0.1', port=6379, db=1)
# res = mongo_client.find({})
# for item in res:
#     housing_url = item['housing_url']
#     re.search(".+().+")
