# 数据容器文件

import scrapy

class SpiderItem(scrapy.Item):
    pass

class WupinchuzuItem(scrapy.Item):
    # 来源
    laiyuan = scrapy.Field()
    # 标题
    biaoti = scrapy.Field()
    # 封面
    fengmian = scrapy.Field()
    # 类别
    leibie = scrapy.Field()
    # 服务区域
    fuwuquyu = scrapy.Field()
    # 联系人
    lianxiren = scrapy.Field()
    # 卖方公司
    gongsi = scrapy.Field()
    # 商家地址
    sjdz = scrapy.Field()
    # 发帖
    fatie = scrapy.Field()
    # 店铺介绍
    detail = scrapy.Field()

