import scrapy
import os
import json
import logging
from datetime import datetime
from scrapy.utils.project import get_project_settings
from ContractSpider.items import ContractItem

class ContractSpider(scrapy.Spider):
    name = "contract"
    allowed_domains = ["htgs.ccgp.gov.cn"]

    data_url = 'http://htgs.ccgp.gov.cn/GS8/contractpublish/getContractByAjax?contractSign=0'

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        'Connection': 'Close',
    }

    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'ContractSpider.middlewares.RotateProxyMiddleware': 300,
        },
        'ITEM_PIPELINES': {
            'ContractSpider.pipelines.ContractPipeline': 300,
        }
    }

    base_payload = {
        "code": "KL4S",
        "codeResult": "eebb0586e81a4700e5758a228af0dfb5",
        "currentPage": "0",
        "isChange": "",
        "searchAgentName": "",
        "searchContractCode": "",
        "searchContractName": "",
        "searchPlacardEndDate": "",
        "searchPlacardStartDate": "",
        "searchProjCode": "",
        "searchProjName": "",
        "searchPurchaserName": "",
        "searchSupplyName": ""
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        settings = get_project_settings()
        self.start_date = kwargs.get("CONTRACT_START_DATE", settings.get("CONTRACT_START_DATE", "2024-10-01"))
        self.end_date = kwargs.get("CONTRACT_END_DATE", settings.get("CONTRACT_END_DATE", "2024-10-31"))

        self.base_payload['searchPlacardStartDate'] = self.start_date
        self.base_payload['searchPlacardEndDate'] = self.end_date

        # 配置 logger
        today_str = datetime.now().strftime("%Y_%m_%d")
        log_file_path = f"logs/contract_{today_str}.log"
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

        self.custom_logger = logging.getLogger(self.name)
        if not self.custom_logger.handlers:
            self.custom_logger.setLevel(logging.INFO)
            handler = logging.FileHandler(log_file_path, encoding="utf-8")
            formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(filename)s:%(lineno)d - %(message)s')
            handler.setFormatter(formatter)
            self.custom_logger.addHandler(handler)
            self.custom_logger.propagate = False
    
    def start_requests(self):
        """爬虫启动入口，直接请求第一页"""
        self.custom_logger.info(f"爬虫启动，日期范围: {self.start_date} to {self.end_date}")
        payload = self.base_payload.copy()
        payload['currentPage'] = '1'
        yield scrapy.FormRequest(
            url=self.data_url,
            method="POST",
            headers=self.headers,
            formdata=payload,
            callback=self.parse,
            meta={'page': 1, 'payload': payload}
        )

    def parse(self, response):
        page = response.meta["page"]
        payload = response.meta["payload"]

        # 1. 处理最终失败的请求（由中间件返回）
        if response.status != 200 or not response.body:
            self.custom_logger.error(f"❌ 第 {page} 页请求最终失败(状态码: {response.status})，将跳过并请求下一页。")
            yield from self.request_next_page(page, payload)
            return

        # 2. 处理JSON解析错误
        try:
            response_json = json.loads(response.text)
        except json.JSONDecodeError:
            self.custom_logger.error(f"📄 第 {page} 页返回的JSON格式错误，将跳过并请求下一页。响应: {response.text[:200]}")
            yield from self.request_next_page(page, payload)
            return
        
        rows = response_json.get("rows", [])
        self.custom_logger.info(f"正在处理第 {page} 页，获取到 {len(rows)} 条数据。")

        # 3. 如果当前页返回的 "rows" 为空，则认为爬取结束
        if not rows:
            self.custom_logger.info(f"✅ 第 {page} 页没有返回数据(rows为空)，认为已到达末页，爬虫正常结束。")
            return

        # 正常提取数据
        for row in rows:
            item = ContractItem()
            # ... 填充 item 的代码 ...
            item["sign_date"] = row.get("signDate", "").strip()
            item["publish_date"] = row.get("publishDate", "").strip()
            item["purchaser"] = row.get("purchaserName", "").strip()
            item["supplier"] = row.get("supplyName", "").strip()
            item["agent"] = row.get("agentName", "").strip()
            item["contract_link"] = f'http://htgs.ccgp.gov.cn/GS8/contractpublish/detail/{row.get("uuid", "")}?contractSign=0'
            item["project_name"] = row.get("projName", "").strip()
            item["contract_name"] = row.get("contractName", "").strip()
            
            publish_date_str = item["publish_date"].split()[0] if item["publish_date"] else ''
            if not publish_date_str:
                self.custom_logger.warning(f"跳过一条数据，因为发布日期为空: {row}")
                continue
            
            try:
                date_obj = datetime.strptime(publish_date_str, "%Y-%m-%d")
                folder_path = os.path.join("downloads", date_obj.strftime("%Y-%m"))
                os.makedirs(folder_path, exist_ok=True)
                item["file_path"] = os.path.join(folder_path, f"{date_obj.strftime('%Y-%m-%d')}.xlsx")
                yield item
            except (ValueError, KeyError) as e:
                self.custom_logger.warning(f"跳过一条数据，因日期或UUID格式错误({e}): {row}")
                continue

        # 4. 成功处理完当前页数据后，请求下一页
        yield from self.request_next_page(page, payload)

    def request_next_page(self, current_page, payload):
        """
        健壮的翻页函数。它只负责生成下一页的请求。
        """
        next_page = current_page + 1
        payload['currentPage'] = str(next_page)
        self.custom_logger.info(f"➡️ 准备请求第 {next_page} 页...")
        yield scrapy.FormRequest(
            url=self.data_url,
            method="POST",
            headers=self.headers,
            formdata=payload,
            callback=self.parse,
            meta={'page': next_page, 'payload': payload}
        )

    def close(self, reason):
        self.custom_logger.info(f"爬虫关闭，原因: {reason}")