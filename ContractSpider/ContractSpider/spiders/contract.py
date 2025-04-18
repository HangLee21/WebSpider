import scrapy
import os
import json
import logging
from datetime import datetime
from scrapy.utils.project import get_project_settings
from tqdm import tqdm
from ContractSpider.items import ContractItem


class ContractSpider(scrapy.Spider):
    name = "contract"
    allowed_domains = ["htgs.ccgp.gov.cn"]

    data_url = 'http://htgs.ccgp.gov.cn/GS8/contractpublish/getContractByAjax?contractSign=0'
    count_url = 'http://htgs.ccgp.gov.cn/GS8/contractpublish/getCountByAjax?contractSign=0'

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

    total_pages = -1
    current_page = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        settings = get_project_settings()
        self.start_date = kwargs.get("CONTRACT_START_DATE", settings.get("CONTRACT_START_DATE", "2025-03-01"))
        self.end_date = kwargs.get("CONTRACT_END_DATE", settings.get("CONTRACT_END_DATE", "2025-03-10"))

        self.base_payload['searchPlacardStartDate'] = self.start_date
        self.base_payload['searchPlacardEndDate'] = self.end_date

        self.download_dir = "downloads"

        # 配置 logger：contract_yyyy_mm_dd.log
        today_str = datetime.now().strftime("%Y_%m_%d")
        log_file_path = f"logs/contract_{today_str}.log"
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

        self.custom_logger = logging.getLogger("contract_logger")
        self.custom_logger.setLevel(logging.INFO)

        handler = logging.FileHandler(log_file_path, encoding="utf-8")
        formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(filename)s:%(lineno)d - %(message)s')
        handler.setFormatter(formatter)

        self.custom_logger.addHandler(handler)
        self.custom_logger.propagate = False  # 防止日志冒泡到终端

        # 初始化进度条（在获取总页数后设置 total）
        self.progress_bar = None

    def start_requests(self):
        yield scrapy.FormRequest(
            url=self.count_url,
            method="POST",
            headers=self.headers,
            formdata=self.base_payload.copy(),
            callback=self.parse_total_pages
        )

    def parse_total_pages(self, response):
        try:
            response_json = json.loads(response.text)
            total_count = int(response_json)
            page_size = 20
            self.total_pages = (total_count // page_size) + (1 if total_count % page_size != 0 else 0)

            self.custom_logger.info(f"总合同数: {total_count}, 每页 {page_size} 条, 总页数: {self.total_pages}")

            # 初始化进度条
            # self.progress_bar = tqdm(total=self.total_pages, desc="合同页", unit="页")

            payload = self.base_payload.copy()
            payload["currentPage"] = "1"

            yield scrapy.FormRequest(
                url=self.data_url,
                method="POST",
                headers=self.headers,
                formdata=payload,
                callback=self.parse,
                meta={"page": 1, "payload": payload}
            )
        except Exception as e:
            self.custom_logger.error(f"解析总页数失败: {e}")

    def parse(self, response):
        # print(response.meta)

        payload = response.meta["payload"]
        page = response.meta["page"]

        if response.status != 200:
            self.custom_logger.error(f"[警告] 页面 {page} 状态码错误: {response.status}")
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
            return

        try:
            response_json = json.loads(response.text)
            self.current_page = page
            self.custom_logger.info(f'current page: {self.current_page}')

            for row in response_json.get("rows", []):
                # self.custom_logger.info(f'row: {row}')
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

                # self.custom_logger.info(f'item: {item}')
                try:
                    date_obj = datetime.strptime(publish_date.split()[0], "%Y-%m-%d")
                    folder_path = os.path.join(self.download_dir, date_obj.strftime("%Y-%m"))
                    os.makedirs(folder_path, exist_ok=True)
                    file_path = os.path.join(folder_path, f"{date_obj.strftime('%Y-%m-%d')}.xlsx")

                except ValueError:
                    self.custom_logger.warning(f"无效日期格式: {publish_date}")
                    continue

                item["file_path"] = file_path
                # self.custom_logger.info(f'item: {item}')
                yield item

            # 进度更新
            # self.progress_bar.update(1)

            # 下一页
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
                # self.progress_bar.close()

        except Exception as e:
            self.custom_logger.error(f"[错误] 第 {self.current_page} 页解析失败: {e}")
