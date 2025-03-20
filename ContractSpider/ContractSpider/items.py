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

import scrapy

import scrapy

class DetailItem(scrapy.Item):
    """Stores contract details"""
    contract_number = scrapy.Field()
    contract_name = scrapy.Field()
    project_number = scrapy.Field()
    project_name = scrapy.Field()
    purchaser = scrapy.Field()
    purchaser_address = scrapy.Field()
    purchaser_contact = scrapy.Field()
    supplier = scrapy.Field()
    supplier_address = scrapy.Field()
    supplier_contact = scrapy.Field()
    main_product_name = scrapy.Field()
    specifications = scrapy.Field()
    quantity = scrapy.Field()
    unit_price = scrapy.Field()
    contract_amount = scrapy.Field()
    performance_location = scrapy.Field()
    procurement_method = scrapy.Field()
    contract_sign_date = scrapy.Field()
    contract_announcement_date = scrapy.Field()
    attachment_name = scrapy.Field()
    attachment_download_url = scrapy.Field()

