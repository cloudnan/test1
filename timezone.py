# -*- coding: utf-8 -*-
import json
import os
import pymysql
import redis

currectt_file = r'D:\projects\test1\时区正常.txt'
incurrect_file = r'D:\projects\test1\时区异常.txt'
error_file = r'D:\projects\test1\执行异常.txt'
inmysql_file = r'D:\projects\test1\mysql中有redis中没有.txt'
inredis_file = r'D:\projects\test1\redis中有mysql中没有.txt'
# 清楚数据
if os.path.exists(currectt_file):
    os.remove(currectt_file)
if os.path.exists(incurrect_file):
    os.remove(incurrect_file)
if os.path.exists(error_file):
    os.remove(error_file)
if os.path.exists(inmysql_file):
    os.remove(inmysql_file)
if os.path.exists(inredis_file):
    os.remove(inredis_file)
# 连接redis
r = redis.StrictRedis(host="public-server.f-qa.igen", port=6379, password="123456", max_connections=1024, db=1,
                      decode_responses=True)
# 获取Redis中device_id个数
count_redis = r.hkeys('dipsio:timezone:all')
# 连接mysql，solarman3_device库
db_device = pymysql.connect("public-server.f-qa.igen", "root", "1234", "solarman3_device", charset='utf8')
cursor_device = db_device.cursor()
# 获取mysql中device_id个数
sql = 'SELECT DISTINCT(device_id)  from device_registry WHERE type in ("COLLECTOR","DTU") '
cursor_device.execute(sql)
device_id = cursor_device.fetchall()
device_id_new = []
# 判断redis和mysql中device_id是否一致
if len(count_redis) != len(device_id):
    print('设备数量不一致：redis中设备数量为 %s  ,     mysql中设备数量为 %s ' % (len(count_redis), len(device_id)))
    id_device = []
    for device in device_id:
        id_device.append(str(device[0]))
    mysql_diffs = set(id_device).difference(set(count_redis))
    for mysql_diff in mysql_diffs:
        with open(inmysql_file, 'a') as fp:
            fp.write(str(mysql_diff) + '\n')
    redis_diffs = set(count_redis).difference(set(id_device))
    for redis_diff in redis_diffs:
        with open(inredis_file, 'a') as fp:
            fp.write(str(redis_diff) + '\n')
else:
    print('设备数量一致！')

for i in device_id:
# for k in range(1,2):
#     i = 200201586
    key = i[0]
    # key = i
    sql_count = 'select count(*),system_id from solarman3_device.device_registry where device_id = %s' % key
    cursor_device.execute(sql_count)
    count_id = cursor_device.fetchone()
    # 根据device_id查system_id,并查询region_timezone，获取mysql中的电站时区
    sql1 = 'SELECT region_timezone,id from solarman3_station.power_station where id in \
            (SELECT system_id from solarman3_device.device_registry where device_id = %s ) \
            order by last_modified_date DESC' % key
    cursor_device.execute(sql1)
    region_timezone_set = cursor_device.fetchone()
    # device_id对应单个system_id时
    if count_id[0] == 1:
        if region_timezone_set != None:
            region_timezone = region_timezone_set[0]
            # 获取Redis中device_id对应的时区
            value = r.hget(name='dipsio:timezone:all', key=key)
            if value != None:
                timezone_redis = json.loads(value)['timezone']
                # Redis和mysql时区比较是否相等
                if timezone_redis == region_timezone:
                    correct_text = 'device_id: %s      时区正常\n' % key
                    with open(currectt_file, 'a') as fp:
                        fp.write(correct_text)
                else:
                    false_text = '时区不一致 device_id:%s \nsystem_id:%s,Redis中的时区为:%s ,mysql中的时区为:%s\n' % (
                        key, count_id[1], timezone_redis, region_timezone)
                    # false_text = '%s\n' % (key)
                    with open(incurrect_file, 'a') as fp:
                        fp.write(false_text)
        else:
            # error_text = '电站信息为空：device_id  %s \n'%key
            error_text = '%s\n' % (key)
            with open(error_file, 'a') as fp:
                fp.write(error_text)
    else:
        # 如果mysql查询的device_id对应多个system_id
        mysql_timezone = []
        sql_system_id = 'select system_id from solarman3_device.device_registry where device_id = %s' % key
        cursor_device.execute(sql_system_id)
        device_system_ids = cursor_device.fetchall()
        # 查询每个system_id对应的电站时区
        for device_system_id in device_system_ids:
            sql_system_timezone = 'SELECT region_timezone from solarman3_station.power_station where id=%s' % device_system_id
            cursor_device.execute(sql_system_timezone)
            mysql_system_timezone = cursor_device.fetchone()
            mysql_timezone.append(mysql_system_timezone[0])
            if mysql_system_timezone is None:
                # error_text = '电站信息为空：device_id  %s \n' % key
                error_text = '%s\n' % (key)
                with open(error_file, 'a') as fp:
                    fp.write(error_text)
        value = r.hget(name='dipsio:timezone:all', key=key)
        # 写入每个system_id对应电站的时区
        if value is not None:
            timezone_redis = json.loads(value)['timezone']
            if len(list(set(mysql_timezone))) == 1 and timezone_redis == list(set(mysql_timezone))[0]:
                correct_text = 'device_id: %s      时区正常\n' % key
                with open(currectt_file, 'a') as fp:
                    fp.write(correct_text)
            else:
                error_text = 'device_id:%s\n Redis中的时区为:%s ,mysql中的时区为:%s ,电站id：%s\n' % (
                    key,timezone_redis, mysql_timezone, device_system_ids)
            # error_text = '%s\n' % (key)
                with open(incurrect_file, 'a') as fp:
                    fp.write(error_text)
        else:
            error_text = 'redis中无信息：device_id  %s \n' % key
            with open(error_file, 'a') as fp:
                fp.write(error_text)
with open('时区正常.txt', 'r') as f:
    current_len = len(f.readlines())
with open('时区异常.txt', 'r') as f:
    incurrent_len = len(f.readlines())/2
with open('redis中有mysql中没有.txt' , 'r') as f:
    redis_len = len(f.readlines())
print('时区正常设备数量：%s'% current_len)
print('时区异常设备数量：%s' % incurrent_len)
print('redis中有mysql中没有的设备数量是: %s' % redis_len)
if current_len+int(incurrent_len) != len(device_id):
    print('正常+异常不等于mysql设备数量')
if redis_len+len(device_id) != len(count_redis):
    print('mysql中存在设备+不存在设备不等于redis中的设备数量')

# with open(error_file, 'r') as fp:
#     err_value = fp.readlines()
# with open(incurrect_file,'r') as fp:
#     inc_value = fp.readlines()
# with open(currectt_file,'r') as fp:
#     cur_value = fp.readlines()
# for i in incurrect_file:
#     if i in cur_value:
#         print(i)
# print(err_value)
# print(Counter(err_value))
# print(len(inc_value))
# print(len(set(inc_value)))
# print(len(cur_value))
# print(len(set(cur_value)))

def verifard():
    with open(error_file, 'r') as fp:
        err_value = fp.readlines()
    for i in err_value:
        sql = 'select system_id from solarman3_device.device_registry where device_id = %s' % i.split('\n')[0]
        cursor_device.execute(sql)
        system_id = cursor_device.fetchone()
        for id in system_id:
            sql1 = 'SELECT region_timezone,valid from solarman3_station.power_station where id=%s' % id
            cursor_device.execute(sql1)
            value = cursor_device.fetchone()
            print(value)


# verifard()

cursor_device.close()
