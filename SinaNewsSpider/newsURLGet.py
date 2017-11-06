# -*- coding: utf-8 -*-
'''
Created on Jul 2, 2017

@author: qinhao
'''

import simplejson
import urllib
import random
import mysql.connector
import datetime
import time

from asn1crypto.core import Null

#DB config
config = {
    'user': 'root',
    'password': '1qazxsw2',
    'host': 'localhost',
    'database': 'news',
    'raise_on_warnings': True,
}
#DB connnection
cnx = mysql.connector.connect(**config)

def excute():
    #initial values
    i=1


    #time_stamp_init = 1499047718650; 
    #                   
    # loop to get news lists MAX 2490
    while i<2500:
        print "sleep.................."
        print "page: "+ str(i)
        time_stamp_init = int(time.time()*1000) 

        time.sleep(random.uniform(10, 30))
        #time_stamp_init = time_stamp_init + random.uniform(1, 1000)
        #下面是国内新闻网址
        cat = "国内"
        new_url = 'http://api.roll.news.sina.com.cn/zt_list?channel=news&cat_1=gnxw&cat_2==gdxw1||=gatxw||=zs-pl||=mtjj&level==1||=2&show_ext=1&show_all=1&show_num=22&tag=1&format=json&page='+str(i)+'&callback=newsloadercallback&_='+str(time_stamp_init)
        #内地
        #cat = "内地"
        #http://api.roll.news.sina.com.cn/zt_list?channel=news&cat_1=gnxw&cat_2==gdxw1&level==1||=2&show_ext=1&show_all=1&show_num=22&tag=1&format=json&page=2&callback=newsloadercallback&_=1499071882065
        #港澳
        #http://api.roll.news.sina.com.cn/zt_list?channel=news&cat_1=gnxw&cat_2==gatxw&level==1||=2&show_ext=1&show_all=1&show_num=22&tag=1&format=json&page=3&callback=newsloadercallback&_=1499072091174
        #综述
        #http://api.roll.news.sina.com.cn/zt_list?channel=news&cat_1=gnxw&cat_2==zs-pl&level==1||=2&show_ext=1&show_all=1&show_num=22&tag=1&format=json&page=3&callback=newsloadercallback&_=1499072216561
        #深度
        #http://api.roll.news.sina.com.cn/zt_list?channel=news&cat_1=gnxw&cat_2==mtjj&level==1||=2&show_ext=1&show_all=1&show_num=22&tag=1&format=json&page=3&callback=newsloadercallback&_=1499072337694
        #下面是国际新闻网址
        #cat = "国际"
        #new_url='http://api.roll.news.sina.com.cn/zt_list?channel=news&cat_1=gjxw&level==1||=2&show_ext=1&show_all=1&show_num=22&tag=1&format=json&page='+str(i)+'&callback=newsloadercallback&_='+str(time_stamp_init)
        #视野趣闻
        #http://api.roll.news.sina.com.cn/zt_list?channel=news&cat_1=gjxw&cat_2==hqqw||=gjmtjj&level==1||=2&show_ext=1&show_all=1&show_num=22&tag=1&format=json&page=3&callback=newsloadercallback&_=1499072643507
        #亚洲
        #http://api.roll.news.sina.com.cn/zt_list?channel=news&cat_1=gjxw&cat_3=gj-yz&level==1||=2&show_ext=1&show_all=1&show_num=22&tag=1&format=json&page=4&callback=newsloadercallback&_=1499072737899
        #欧洲
        #http://api.roll.news.sina.com.cn/zt_list?channel=news&cat_1=gjxw&cat_3=gj-oz&level==1||=2&show_ext=1&show_all=1&show_num=22&tag=1&format=json&page=2&callback=newsloadercallback&_=1499072705558
        #美洲
        #http://api.roll.news.sina.com.cn/zt_list?channel=news&cat_1=gjxw&cat_3=gj-mz&level==1||=2&show_ext=1&show_all=1&show_num=22&tag=1&format=json&page=2&callback=newsloadercallback&_=1499072786230
        #非洲
        #http://api.roll.news.sina.com.cn/zt_list?channel=news&cat_1=gjxw&cat_3=gj-fz&level==1||=2&show_ext=1&show_all=1&show_num=22&tag=1&format=json&page=2&callback=newsloadercallback&_=1499072786230
        
        print new_url
        
        save_to_url_his(new_url,i,cat)
        i=i+1
        a = urllib.urlopen(new_url).read()
        b = simplejson.loads(a[21:-2])
        print b
        for item in b['result']['data']:
            print item['title']
            #save to mysql
            save_to_db(item)
    
    
def duplicate_check(sina_url):
    check_sql = "select * from sina_news_url_index where url='%s'" % (sina_url)
    cursor1 = cnx.cursor()
    cursor1.execute(check_sql)
    g = cursor1.fetchall()
    print check_sql
    if len(g) ==0 :
        cursor1.close()
        return False
    else:
        cursor1.close()
        print "URL exists:"+sina_url
        return True

def save_to_db(item):
    
    if duplicate_check(item['url']):
        return "duplicate"
    
    if item['column'] is not Null:
        print "column: %s" %item['column']
    else:
        return ""

    if item['comment_channel'] is not Null:
        print "comment_channel: %s" %item['comment_channel']
    else:
        return ""

    if item['createtime'] is not Null:
        dateArray = datetime.datetime.utcfromtimestamp(int(item['createtime']))
        print "createtime: %s" %item['createtime']
    else:
        return ""

    if item['ext1'] is not Null:
        print "ext1: %s" %item['ext1']
    else:
        return ""

    if item['ext2'] is not Null:
        print "ext2: %s" %item['ext2']
    else:
        return ""

    if item['ext3'] is not Null:
        print "ext3: %s" %item['ext3']
    else:
        return ""

    if item['ext4'] is not Null:
        print "ext4: %s" %item['ext4']
    else:
        return ""

    if item['ext5'] is not Null:
        print "ext5: %s" %item['ext5']
    else:
        return ""

    if item['id'] is not Null:
        print "id: %s" %item['id']
    else:
        return ""
    
    if item['img'] is not Null:
        print "img: %s" %item['img']
    else:
        return ""

    if item['keywords'] is not Null:
        print "keywords: %s" %item['keywords']
    else:
        return ""
    
    if item['level'] is not Null:
        print "level: %s" %item['level']
    else:
        return ""
    
    if item['media_name'] is not Null:
        print "media_name: %s" %item['media_name']
    else:
        return ""
    
    if item['media_type'] is not Null:
        print "media_type: %s" %item['media_type']
    else:
        return ""
    
    if item['old_level'] is not Null:
        print "old_level: %s" %item['old_level']
    else:
        return ""
    
    if item['title'] is not Null:
        print "title: %s" %item['title']
    else:
        return ""
    
    if item['url'] is not Null:
        print "url: %s" %item['url']
    else:
        return ""        

    data = (item['column'],item['comment_channel'],dateArray,item['ext1'],item['ext2'],item['ext3'],
            item['ext4'],item['ext5'],item['id'],item['img'],item['keywords'],item['level'],item['media_name'],item['media_type'],
            item['old_level'],item['title'],item['url'],"0",datetime.datetime.now())
#             list.append(data)
#    sql = "insert into sina_news_url_index(t_country_or_area,t_country_key,t_item,t_item_key,t_year,t_value,t_create_time)values(%s,%s,%s,%s,%s,%s,%s,%s)"
    sql = "insert into sina_news_url_index(column_sina ,comment_channel ,createtime ,ext1 ,ext2 ,ext3 ,ext4 ,ext5 ,id_sina ,img ,keywords ,level ,media_name ,media_type ,old_level ,title ,url ,spider_status,spider_timestamp)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor = cnx.cursor()
    cursor.execute(sql, data)
    cnx.commit()
    cursor.close()
    
    return ""

def save_to_url_his(url,page,cat):
    data=(url,str(page),cat,datetime.datetime.now())
    sql = "insert into sina_json_url_history(url_his,cat,page,time_stamp)values(%s,%s,%s,%s)"
    cursor = cnx.cursor()
    cursor.execute(sql, data)
    cnx.commit()
    cursor.close()
    return ""

if __name__ == '__main__':
    excute()

