import scrapy

from ContractSpider.items import DetailItem
from ContractSpider.utils.detail_link import DetailsExtractor


class DetailSpider(scrapy.Spider):
    name = "detail"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        'Connection': 'Close',
    }

    # 自定义套件
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'ContractSpider.middlewares.DetailProxyMiddleware': 300,
        },
        'ITEM_PIPELINES': {
            'ContractSpider.pipelines.DetailPipeline': 300,
        }
    }

    def start_requests(self):
        extractor = DetailsExtractor()
        urls = extractor.extract_urls()
        for url in urls:
            yield scrapy.Request(url=url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        """Parses contract details from the page and stores them in DetailItem"""
        content = response.css("div.content_2020")

        item = DetailItem()
        item["contract_number"] = content.xpath(".//p/strong[contains(text(), '合同编号')]/following::text()[1]").get(
            "").strip()
        item["contract_name"] = content.xpath(".//p/strong[contains(text(), '合同名称')]/following::text()[1]").get(
            "").strip()
        item["project_number"] = content.xpath(".//p/strong[contains(text(), '项目编号')]/following::text()[1]").get(
            "").strip()
        item["project_name"] = content.xpath(".//p/strong[contains(text(), '项目名称')]/following::text()[1]").get(
            "").strip()
        item["purchaser"] = content.xpath(".//p[contains(text(), '采购人（甲方）')]/text()").get("").split("：")[
            -1].strip()
        item["purchaser_address"] = content.xpath(".//p[contains(text(), '地  址')][1]/text()").get("").split("：")[
            -1].strip()
        item["purchaser_contact"] = content.xpath(".//p[contains(text(), '联系方式')][1]/text()").get("").split("：")[
            -1].strip()
        item["supplier"] = content.xpath(".//p[contains(text(), '供应商（乙方）')]/text()").get("").split("：")[-1].strip()
        item["supplier_address"] = content.xpath(".//p[contains(text(), '地  址')][2]/text()").get("").split("：")[
            -1].strip()
        item["supplier_contact"] = content.xpath(".//p[contains(text(), '联系方式')][2]/text()").get("").split("：")[
            -1].strip()
        item["main_product_name"] = content.xpath(".//p[contains(text(), '主要标的名称')]/text()").get("").split("：")[
            -1].strip()
        item["specifications"] = \
            content.xpath(".//p[contains(text(), '规格型号（或服务要求）')]/text()").get("").split("：")[-1].strip()
        item["quantity"] = content.xpath(".//p[contains(text(), '主要标的数量')]/text()").get("").split("：")[-1].strip()
        item["unit_price"] = content.xpath(".//p[contains(text(), '主要标的单价')]/text()").get("").split("：")[
            -1].strip()
        item["contract_amount"] = content.xpath(".//p[contains(text(), '合同金额')]/text()").get("").strip()
        item["performance_location"] = content.xpath(".//p[contains(text(), '履约期限、地点等简要信息')]/text()").get(
            "").strip()
        item["procurement_method"] = content.xpath(".//p[contains(text(), '采购方式')]/text()").get("").strip()
        item["contract_sign_date"] = content.xpath(
            ".//p/strong[contains(text(), '合同签订日期')]/following::text()[1]").get("").strip()
        item["contract_announcement_date"] = content.xpath(
            ".//p/strong[contains(text(), '合同公告日期')]/following::text()[1]").get("").strip()

        # 解析多个附件名称
        item["attachment_name"] = content.xpath(".//li[@class='fileInfo']/div/b/text()").getall()

        # 解析多个附件下载链接
        attachment_scripts = content.xpath(".//li[@class='fileInfo']//a/@onclick").getall()
        item["attachment_download_url"] = []

        for script in attachment_scripts:
            start = script.find("('") + 2
            end = script.find("','")
            if start != -1 and end != -1:
                file_id = script[start:end]
                item["attachment_download_url"].append(f"https://download.ccgp.gov.cn/oss/download?uuid={file_id}")

        yield item
