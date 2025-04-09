import scrapy
import logging
import os
from datetime import datetime
from tqdm import tqdm
from ContractSpider.items import DetailItem
from ContractSpider.utils.detail_link import DetailsExtractor


class DetailSpider(scrapy.Spider):
    name = "detail"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        'Connection': 'Close',
    }

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
        from scrapy.utils.project import get_project_settings
        settings = get_project_settings()

        self.start_date = kwargs.get("DETAIL_START_DATE") or settings.get("DETAIL_START_DATE")
        self.end_date = kwargs.get("DETAIL_END_DATE") or settings.get("DETAIL_END_DATE")
        self.extractor = DetailsExtractor(self.start_date, self.end_date)

        # 初始化日志记录
        today_str = datetime.now().strftime("%Y_%m_%d")
        log_file_name = f"detail_{today_str}.log"
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, log_file_name)
        logging.basicConfig(
            filename=log_file_path,
            format="%(asctime)s - %(levelname)s - %(message)s",
            level=logging.INFO
        )

        # 初始化进度条变量
        self.total = 0
        self.count = 0
        self.pbar = None

    def start_requests(self):
        urls = self.extractor.extract_urls()
        self.total = len(urls)
        self.count = 0
        self.pbar = tqdm(total=self.total, desc="合同详情抓取进度")

        for url in urls:
            yield scrapy.Request(url=url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        self.count += 1
        self.pbar.update(1)

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
            item["procurement_method"] = content.xpath(".//p[contains(text(), '采购方式')]/text()").get("").strip().replace('采购方式：', '').replace('\t', '').replace('\n', '').replace('\r', '')
            item["contract_sign_date"] = content.xpath(".//p/strong[contains(text(), '合同签订日期')]/text()").get("").strip().replace('七、合同签订日期：\r\n\t\t\t\t\t\t\t', '')
            item["contract_announcement_date"] = content.xpath(".//p/strong[contains(text(), '合同公告日期')]/text()").get("").strip().replace('八、合同公告日期：\r\n\t\t\t\t\t\t\t', '')

            item["attachment_name"] = content.xpath(".//li[@class='fileInfo']/div/b/text()").getall()

            attachment_scripts = content.xpath(".//li[@class='fileInfo']//a/@onclick").getall()
            item["attachment_download_url"] = []
            for script in attachment_scripts:
                start = script.find("('") + 2
                end = script.find("','")
                if start != -1 and end != -1:
                    file_id = script[start:end]
                    item["attachment_download_url"].append(f"https://download.ccgp.gov.cn/oss/download?uuid={file_id}")

            return item

        except Exception as e:
            self.logger.error(f"解析失败: {response.url}，错误: {e}")
