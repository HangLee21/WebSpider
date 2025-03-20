import scrapy

class ContractItem(scrapy.Item):
    sign_date = scrapy.Field()       # 合同签订时间
    publish_date = scrapy.Field()    # 合同发布时间
    purchaser = scrapy.Field()       # 采购人
    supplier = scrapy.Field()        # 供应商
    agent = scrapy.Field()           # 代理机构
    contract_link = scrapy.Field()   # 合同详情链接
    project_name = scrapy.Field()    # 合同标题
    contract_name = scrapy.Field()   # 合同名称
    file_path = scrapy.Field()       # 存放位置