import scrapy
import os
import json
import logging
from datetime import datetime
from scrapy.utils.project import get_project_settings
from tqdm import tqdm
from ContractSpider.items import DetailItem
from ContractSpider.utils.detail_link import DetailsExtractor


class DetailSpider(scrapy.Spider):
    name = "detail"
    allowed_domains = ["ccgp.gov.cn"]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        'Connection': 'Close',
    }

    # 自定义设置
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'ContractSpider.middlewares.DetailProxyMiddleware': 300,
        },
        'ITEM_PIPELINES': {
            'ContractSpider.pipelines.DetailPipeline': 300,
        }
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        settings = get_project_settings()
        self.start_date = kwargs.get("DETAIL_START_DATE", settings.get("DETAIL_START_DATE", "2025-03-01"))
        self.end_date = kwargs.get("DETAIL_END_DATE", settings.get("DETAIL_END_DATE", "2025-03-10"))
        self.extractor = DetailsExtractor(self.start_date, self.end_date)

        # 配置 logger：detail_yyyy_mm_dd.log
        today_str = datetime.now().strftime("%Y_%m_%d")
        log_file_path = f"logs/detail_{today_str}.log"
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

        self.custom_logger = logging.getLogger("detail_logger")
        self.custom_logger.setLevel(logging.INFO)
        handler = logging.FileHandler(log_file_path, encoding="utf-8")
        formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(message)s')
        handler.setFormatter(formatter)
        self.custom_logger.addHandler(handler)
        self.custom_logger.propagate = False  # 防止打印到终端

        self.progress_bar = None  # 初始化进度条

    def start_requests(self):
        urls = self.extractor.extract_urls()
        self.progress_bar = tqdm(total=len(urls), desc="合同详情", unit="条")
        for url in urls:
            yield scrapy.Request(url=url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        """解析合同详情并存储到 DetailItem"""
        try:
            content = response.css("div.content_2020")

            item = DetailItem()
            item["contract_number"] = content.xpath(".//p/strong[contains(text(), '合同编号')]/text()").get("").strip().replace('一、合同编号：  ', '')
            item["contract_name"] = content.xpath(".//p/strong[contains(text(), '合同名称')]/text()").get("").strip().replace('二、合同名称：  ', '')
            item["project_number"] = content.xpath(".//p/strong[contains(text(), '项目编号')]/text()").get("").strip().replace('三、项目编号：  ', '')
            item["project_name"] = content.xpath(".//p/strong[contains(text(), '项目名称')]/text()").get("").strip().replace('四、项目名称：  ', '')
            item["purchaser"] = content.xpath(".//p[contains(text(), '采购人（甲方）')]/text()").get("").split("：")[-1].strip()
            item["purchaser_address"] = content.xpath(".//p[contains(text(), '地  址')][1]/text()").get("").split("：")[-1].strip()
            item["purchaser_contact"] = content.xpath(".//p[contains(text(), '联系方式')][1]/text()").get("").split("：")[-1].strip()
            item["supplier"] = content.xpath(".//p[contains(text(), '供应商（乙方）')]/text()").get("").split("：")[-1].strip()
            item["supplier_address"] = content.xpath(".//p[contains(text(), '地  址')][2]/text()").get("").split("：")[-1].strip()
            item["supplier_contact"] = content.xpath(".//p[contains(text(), '联系方式')][2]/text()").get("").split("：")[-1].strip()
            item["main_product_name"] = content.xpath(".//p[contains(text(), '主要标的名称')]/text()").get("").replace('主要标的名称：', '').replace(';', '')
            item["specifications"] = content.xpath(".//p[contains(text(), '规格型号（或服务要求）')]/text()").get("").split("：")[-1].strip()
            item["quantity"] = content.xpath(".//p[contains(text(), '主要标的数量')]/text()").get("").split("：")[-1].strip()
            item["unit_price"] = content.xpath(".//p[contains(text(), '主要标的单价')]/text()").get("").split("：")[-1].strip()
            item["contract_amount"] = content.xpath(".//p[contains(text(), '合同金额')]/text()").get("").strip().replace('合同金额：', '').replace('\t', '').replace('\n', '').replace('\r', '')
            item["performance_location"] = content.xpath(".//p[contains(text(), '履约期限、地点等简要信息')]/text()").get("").strip().replace('履约期限、地点等简要信息：', '').replace('\t', '').replace('\n', '').replace('\r', '')
            item["procurement_method"] = content.xpath(".//p[contains(text(), '采购方式')]/text()").get("").strip().strip().replace('合同金额：', '').replace('\t', '').replace('\n', '').replace('\r', '')
            item["contract_sign_date"] = content.xpath(".//p/strong[contains(text(), '合同签订日期')]/text()").get("").strip().replace('七、合同签订日期：\r\n\t\t\t\t\t\t\t', '')
            item["contract_announcement_date"] = content.xpath(".//p/strong[contains(text(), '合同公告日期')]/text()").get("").strip().replace('八、合同公告日期：\r\n\t\t\t\t\t\t\t', '')

            # 解析附件
            item["attachment_name"] = content.xpath(".//li[@class='fileInfo']/div/b/text()").getall()

            attachment_scripts = content.xpath(".//li[@class='fileInfo']//a/@onclick").getall()
            item["attachment_download_url"] = []

            for script in attachment_scripts:
                start = script.find("('") + 2
                end = script.find("','")
                if start != -1 and end != -1:
                    file_id = script[start:end]
                    item["attachment_download_url"].append(f"https://download.ccgp.gov.cn/oss/download?uuid={file_id}")

            yield item

            # 更新进度条
            self.progress_bar.update(1)

        except Exception as e:
            self.custom_logger.error(f"[错误] 解析合同详情失败: {e}")

    def closed(self, reason):
        """爬虫结束时关闭进度条"""
        if self.progress_bar:
            self.progress_bar.close()
            self.custom_logger.info("所有合同详情爬取完成。")
