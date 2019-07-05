from scrapy.conf import settings
import redis


redis_host = settings['REDIS_HOST']
redis_port = settings['REDIS_PORT']
redis_db = settings['REDIS_DB']
redis_password = settings['REDIS_PASS']
rds = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)
urls = rds.hkeys("anjuke_detail_url_hashtable")
finished_urls = rds.smembers("anjuke")
unfinished_urls = []
for url in urls:
    if url not in finished_urls:
        unfinished_urls.append(url.decode())
unfinished_urls_length = len(unfinished_urls)
for i in range(0, int(unfinished_urls_length * 0.25)):
    rds.sadd("anjuke_01", unfinished_urls[i])
for i in range(int(unfinished_urls_length * 0.25), int(unfinished_urls_length * 0.5)):
    rds.sadd("anjuke_02", unfinished_urls[i])
for i in range(int(unfinished_urls_length * 0.5), int(unfinished_urls_length * 0.75)):
    rds.sadd("anjuke_03", unfinished_urls[i])
for i in range(int(unfinished_urls_length * 0.75), int(unfinished_urls_length * 1)):
    rds.sadd("anjuke_04", unfinished_urls[i])




