# 数据爬取文件

import scrapy
import pymysql
import pymssql
from ..items import WupinchuzuItem
import time
from datetime import datetime,timedelta
import re
import random
import platform
import json
import os
import urllib
from urllib.parse import urlparse
import requests
import emoji

# 物品出租
class WupinchuzuSpider(scrapy.Spider):
    name = 'wupinchuzuSpider'
    spiderUrl = 'https://zunyi.58.com/zulin/pn{}/?PGTID=0d300168-01dc-4e06-0c36-9c5734a3ca86&ClickID=3'
    start_urls = spiderUrl.split(";")
    protocol = ''
    hostname = ''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start_requests(self):

        plat = platform.system().lower()
        if plat == 'linux' or plat == 'windows':
            connect = self.db_connect()
            cursor = connect.cursor()
            if self.table_exists(cursor, 'k243d_wupinchuzu') == 1:
                cursor.close()
                connect.close()
                self.temp_data()
                return

        pageNum = 1 + 1
        for url in self.start_urls:
            if '{}' in url:
                for page in range(1, pageNum):
                    next_link = url.format(page)
                    yield scrapy.Request(
                        url=next_link,
                        callback=self.parse
                    )
            else:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse
                )

    # 列表解析
    def parse(self, response):
        
        _url = urlparse(self.spiderUrl)
        self.protocol = _url.scheme
        self.hostname = _url.netloc
        plat = platform.system().lower()
        if plat == 'windows_bak':
            pass
        elif plat == 'linux' or plat == 'windows':
            connect = self.db_connect()
            cursor = connect.cursor()
            if self.table_exists(cursor, 'k243d_wupinchuzu') == 1:
                cursor.close()
                connect.close()
                self.temp_data()
                return

        list = response.css('table[class="small-tbimg ac_container list-new-table"] tr[class*="new-list"]')
        
        for item in list:

            fields = WupinchuzuItem()



            if '(.*?)' in '''a[class="t ac_linkurl"]::attr(href)''':
                try:
                    fields["laiyuan"] = re.findall(r'''a[class="t ac_linkurl"]::attr(href)''', item.extract(), re.DOTALL)[0].strip()
                except:
                    pass
            else:
                fields["laiyuan"] = self.remove_html(item.css('a[class="t ac_linkurl"]::attr(href)').extract_first())
            if '(.*?)' in '''div[class="new-long-tit new-long-tit2"]::text''':
                try:
                    fields["biaoti"] = re.findall(r'''div[class="new-long-tit new-long-tit2"]::text''', item.extract(), re.DOTALL)[0].strip()
                except:
                    pass
            else:
                fields["biaoti"] = self.remove_html(item.css('div[class="new-long-tit new-long-tit2"]::text').extract_first())
            if '(.*?)' in '''p.seller::text''':
                try:
                    fields["gongsi"] = re.findall(r'''p.seller::text''', item.extract(), re.DOTALL)[0].strip()
                except:
                    pass
            else:
                fields["gongsi"] = self.remove_html(item.css('p.seller::text').extract_first())

            detailUrlRule = item.css('a[class="t ac_linkurl"]::attr(href)').extract_first()
            if self.protocol in detailUrlRule:
                pass
            elif detailUrlRule.startswith('//'):
                detailUrlRule = self.protocol + ':' + detailUrlRule
            else:
                detailUrlRule = self.protocol + '://' + self.hostname + detailUrlRule
                # fields["laiyuan"] = detailUrlRule

            yield scrapy.Request(url=detailUrlRule, meta={'fields': fields},  callback=self.detail_parse, dont_filter=True)


    # 详情解析
    def detail_parse(self, response):
        fields = response.meta['fields']

        try:
            if '(.*?)' in '''img#bigimg1::attr(src)''':
                fields["fengmian"] = re.findall(r'''img#bigimg1::attr(src)''', response.text, re.S)[0].strip()
            else:
                if 'fengmian' != 'xiangqing' and 'fengmian' != 'detail' and 'fengmian' != 'pinglun' and 'fengmian' != 'zuofa':
                    fields["fengmian"] = self.remove_html(response.css('''img#bigimg1::attr(src)''').extract_first())
                else:
                    fields["fengmian"] = emoji.demojize(response.css('''img#bigimg1::attr(src)''').extract_first())
        except:
            pass

        if fields["fengmian"].startswith('//'):
            fields["fengmian"] = self.protocol + ':' + fields["fengmian"]
        elif fields["fengmian"].startswith('/'):
            fields["fengmian"] = self.protocol + '://' + self.hostname + fields["fengmian"]

        try:
            fields["leibie"] = response.xpath('''//*[@id="basicinfo"]/div[3]/div[1]/div[2]/text()''').extract()[0].strip()
        except:
            pass

        try:
            fields["fuwuquyu"] = response.xpath('''//*[@id="basicinfo"]/div[3]/div[2]/div[2]/a[1]/text()''').extract()[0].strip()
        except:
            pass

        try:
            fields["lianxiren"] = response.xpath('''//*[@id="basicinfo"]/div[3]/div[3]/div[2]/text()''').extract()[0].strip()
        except:
            pass

        try:
            fields["sjdz"] = response.xpath('''//*[@id="basicinfo"]/div[3]/div[5]/div[2]/a[1]/text()''').extract()[0].strip()
        except:
            pass

        try:
            if '(.*?)' in '''dl.shopinfo__intro__last dt::text''':
                fields["fatie"] = re.findall(r'''dl.shopinfo__intro__last dt::text''', response.text, re.S)[0].strip()
            else:
                if 'fatie' != 'xiangqing' and 'fatie' != 'detail' and 'fatie' != 'pinglun' and 'fatie' != 'zuofa':
                    fields["fatie"] = self.remove_html(response.css('''dl.shopinfo__intro__last dt::text''').extract_first())
                else:
                    fields["fatie"] = emoji.demojize(response.css('''dl.shopinfo__intro__last dt::text''').extract_first())
        except:
            pass


        try:
            if '(.*?)' in '''article.description_con''':
                fields["detail"] = re.findall(r'''article.description_con''', response.text, re.S)[0].strip()
            else:
                if 'detail' != 'xiangqing' and 'detail' != 'detail' and 'detail' != 'pinglun' and 'detail' != 'zuofa':
                    fields["detail"] = self.remove_html(response.css('''article.description_con''').extract_first())
                else:
                    fields["detail"] = emoji.demojize(response.css('''article.description_con''').extract_first())
        except:
            pass




        return fields

    # 去除多余html标签
    def remove_html(self, html):
        if html == None:
            return ''
        pattern = re.compile(r'<[^>]+>', re.S)
        return pattern.sub('', html).strip()

    # 数据库连接
    def db_connect(self):
        type = self.settings.get('TYPE', 'mysql')
        host = self.settings.get('HOST', 'localhost')
        port = int(self.settings.get('PORT', 3306))
        user = self.settings.get('USER', 'root')
        password = self.settings.get('PASSWORD', '123456')

        try:
            database = self.databaseName
        except:
            database = self.settings.get('DATABASE', '')

        if type == 'mysql':
            connect = pymysql.connect(host=host, port=port, db=database, user=user, passwd=password, charset='utf8')
        else:
            connect = pymssql.connect(host=host, user=user, password=password, database=database)

        return connect

    # 断表是否存在
    def table_exists(self, cursor, table_name):
        cursor.execute("show tables;")
        tables = [cursor.fetchall()]
        table_list = re.findall('(\'.*?\')',str(tables))
        table_list = [re.sub("'",'',each) for each in table_list]

        if table_name in table_list:
            return 1
        else:
            return 0

    # 数据缓存源
    def temp_data(self):

        connect = self.db_connect()
        cursor = connect.cursor()
        sql = '''
            insert into `wupinchuzu`(
                id
                ,laiyuan
                ,biaoti
                ,fengmian
                ,leibie
                ,fuwuquyu
                ,lianxiren
                ,gongsi
                ,sjdz
                ,fatie
                ,detail
            )
            select
                id
                ,laiyuan
                ,biaoti
                ,fengmian
                ,leibie
                ,fuwuquyu
                ,lianxiren
                ,gongsi
                ,sjdz
                ,fatie
                ,detail
            from `k243d_wupinchuzu`
            where(not exists (select
                id
                ,laiyuan
                ,biaoti
                ,fengmian
                ,leibie
                ,fuwuquyu
                ,lianxiren
                ,gongsi
                ,sjdz
                ,fatie
                ,detail
            from `wupinchuzu` where
                `wupinchuzu`.id=`k243d_wupinchuzu`.id
            ))
            limit {0}
        '''.format(random.randint(10,15))

        cursor.execute(sql)
        connect.commit()

        connect.close()
