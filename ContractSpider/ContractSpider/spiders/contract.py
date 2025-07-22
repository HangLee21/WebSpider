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
        self.custom_logger.info(f"[parse] 正在解析第 {response.meta['page']} 页")
        payload = response.meta["payload"]
        page = response.meta["page"]
        payload = response.meta["payload"]

        if response.body == b'':
            self.custom_logger.error(f"[错误] 第 {page} 页返回空内容")
            self.custom_logger.error(f"[Payload] {payload}, [Page] {page}")
            self.current_page += 1
            payload["currentPage"] = str(self.current_page)
            yield scrapy.FormRequest(
                url=self.data_url,
                method="POST",
                headers=self.headers,
                formdata=payload.copy(),
                callback=self.parse,
                meta={"page": self.current_page, "payload": payload}
            )
        else:
            if response.status != 200:
                self.custom_logger.error(f"[警告] 页面 {page} 状态码错误: {response.status}")
                self._retry_or_skip(payload, page, reason="状态码错误")
                return

            try:
                # 检查 JSON 是否完整
                try:
                    response_json = json.loads(response.text)
                except json.JSONDecodeError:
                    self.custom_logger.error(f"[错误] 第 {page} 页返回的 JSON 不完整或格式错误（长度: {len(response.text)}）")
                    yield from self._retry_or_skip(payload, page, reason="JSON 格式错误")
                    return

                self.current_page = page
                self.custom_logger.info(f'current page: {self.current_page}')

                for row in response_json.get("rows", []):
                    item = ContractItem()
                    item["sign_date"] = row.get("signDate", "").strip()
                    item["publish_date"] = row.get("publishDate", "").strip()
                    item["purchaser"] = row.get("purchaserName", "").strip()
                    item["supplier"] = row.get("supplyName", "").strip()
                    item["agent"] = row.get("agentName", "").strip()
                    item[
                        "contract_link"] = f'http://htgs.ccgp.gov.cn/GS8/contractpublish/detail/{row["uuid"]}?contractSign=0'
                    item["project_name"] = row.get("projName", "").strip()
                    item["contract_name"] = row.get("contractName", "").strip()

                    publish_date = item["publish_date"]
                    try:
                        date_obj = datetime.strptime(publish_date.split()[0], "%Y-%m-%d")
                        end_date_obj = datetime.strptime(self.end_date, "%Y-%m-%d")
                        if date_obj == end_date_obj:
                            continue
                        folder_path = os.path.join(self.download_dir, date_obj.strftime("%Y-%m"))
                        os.makedirs(folder_path, exist_ok=True)
                        file_path = os.path.join(folder_path, f"{date_obj.strftime('%Y-%m-%d')}.xlsx")
                    except ValueError:
                        self.custom_logger.warning(f"无效日期格式: {publish_date}")
                        continue

                    item["file_path"] = file_path
                    yield item

                self.progress_bar.update(1)
                self.retry_count = 0  # 成功解析，重置重试计数

                if self.current_page < self.total_pages:
                    self.current_page += 1
                    payload["currentPage"] = str(self.current_page)
                    yield scrapy.FormRequest(
                        url=self.data_url,
                        method="POST",
                        headers=self.headers,
                        formdata=payload.copy(),
                        callback=self.parse,
                        meta={"page": self.current_page, "payload": payload}
                    )
                else:
                    self.custom_logger.info("所有合同页已爬取完成。")
                    self.progress_bar.close()

            except Exception as e:
                self.custom_logger.error(f"[异常] 第 {self.current_page} 页解析异常: {e}")
                self.custom_logger.error(f"[错误内容]：{response.text[:500]}")
                self._retry_or_skip(payload, page, reason="解析异常")

    def _retry_or_skip(self, payload, page, reason="未知原因"):
        self.custom_logger.error(f"[错误] 第 {page} 页解析失败，原因: {reason}")
        if self.retry_count < self.max_retries:
            self.retry_count += 1
            self.custom_logger.warning(f"[重试] 第 {page} 页（原因: {reason}）第 {self.retry_count} 次重试")
            yield scrapy.FormRequest(
                url=self.data_url,
                method="POST",
                headers=self.headers,
                formdata=payload.copy(),
                callback=self.parse,
                meta={"page": page, "payload": payload},
                dont_filter=True  # ✅ 避免被去重
            )
        else:
            self.custom_logger.error(f"[跳过] 第 {page} 页因 \"{reason}\" 连续失败 {self.max_retries} 次，跳过该页")
            self.retry_count = 0
            self.current_page += 1
            yield scrapy.FormRequest(
                url=self.data_url,
                method="POST",
                headers=self.headers,
                formdata=payload.copy(),
                callback=self.parse,
                meta={"page": self.current_page, "payload": payload},
                dont_filter=True
            )


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