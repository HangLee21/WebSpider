import logging

import scrapy
import json
import os
from datetime import datetime

from tqdm import tqdm

from ContractSpider.items import ContractItem


class ContractSpider(scrapy.Spider):
    name = "contract"
    allowed_domains = ["htgs.ccgp.gov.cn"]

    # API 接口
    data_url = 'http://htgs.ccgp.gov.cn/GS8/contractpublish/getContractByAjax?contractSign=0'
    count_url = 'http://htgs.ccgp.gov.cn/GS8/contractpublish/getCountByAjax?contractSign=0'

    total_pages = -1  # 先初始化

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from scrapy.utils.project import get_project_settings
        settings = get_project_settings()
        # 兼容命令行参数 `-a CONTRACT_START_DATE=2025-03-04` `-a CONTRACT_END_DATE=2025-03-10`
        self.start_date = kwargs.get("CONTRACT_START_DATE", None)
        self.end_date = kwargs.get("CONTRACT_END_DATE", None)
        self.current_page = 1  # 从1开始

        # 如果命令行未提供，则从 settings.py 读取
        if not self.start_date:
            self.start_date = settings.get("CONTRACT_START_DATE", "2025-03-01")
        if not self.end_date:
            self.end_date = settings.get("CONTRACT_END_DATE", "2025-03-10")

        self.base_payload['searchPlacardEndDate'] = self.end_date
        self.base_payload['searchPlacardStartDate'] = self.start_date

        self.download_dir = "downloads"  # 指定下载目录

        # 日志配置
        today = datetime.now()
        log_filename = f"contract_{today.year}_{today.month:02d}_{today.day:02d}.log"
        log_path = os.path.join("logs", log_filename)
        os.makedirs("logs", exist_ok=True)
        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            filemode="w"
        )

        self.logger.info("✅ ContractSpider 初始化完成")

        # 初始化进度条（先设为 None）
        self.progress_bar = None


    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        'Connection': 'Close',
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

    def start_requests(self):
        """获取总页数"""
        yield scrapy.FormRequest(
            url=self.count_url,
            method="POST",
            headers=self.headers,
            formdata=self.base_payload.copy(),
            callback=self.parse_total_pages
        )

    def parse_total_pages(self, response):
        """解析总页数，启动爬取"""
        try:
            response_json = json.loads(response.text)
            total_count = int(response_json)
            page_size = 20
            self.total_pages = (total_count // page_size) + (1 if total_count % page_size != 0 else 0)

            self.logger.info(f"总合同数: {total_count}, 每页 {page_size} 条, 预计页数: {self.total_pages}")

            # 初始化 tqdm 进度条
            self.progress_bar = tqdm(total=self.total_pages, desc="合同页数进度", ncols=80)

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
            self.logger.error(f"解析总页数失败: {e}")


    def parse(self, response):
        """解析合同数据并存储"""
        payload = response.meta["payload"]
        self.logger.info(f"📄 当前页: {self.current_page}")
        if self.progress_bar:
            self.progress_bar.update(1)

        if response.status != 200:
            logging.error(f"error {response.status} in page {self.current_page}")
            self.current_page = self.current_page + 1
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
            try:
                response_json = json.loads(response.text)
                self.current_page = int(response.meta["page"])

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

                    # **文件路径逻辑**
                    publish_date = item["publish_date"]

                    try:
                        # 仅取日期部分，防止时间导致解析失败
                        date_obj = datetime.strptime(publish_date.split()[0], "%Y-%m-%d")
                        # 将 self.end_date 转换为 datetime 对象
                        end_date_obj = datetime.strptime(self.end_date, "%Y-%m-%d")
                        # 不包含结束日期当天（包含时只有00:00时刻的数据）
                        if date_obj == end_date_obj:
                            continue
                        folder_path = os.path.join(self.download_dir, date_obj.strftime("%Y-%m"))  # 按月分类
                        os.makedirs(folder_path, exist_ok=True)  # 确保目录存在
                        file_path = os.path.join(folder_path, f"{date_obj.strftime('%Y-%m-%d')}.xlsx")  # 按天存放
                    except ValueError:
                        self.logger.error(f"无效的日期格式: {publish_date}")
                        continue  # 跳过错误数据

                    item["file_path"] = file_path  # 传递路径给 pipeline
                    yield item  # 交给 pipelines 处理

                # **爬取下一页**
                if self.current_page <= self.total_pages:
                    self.current_page = self.current_page + 1
                    payload["currentPage"] = str(self.current_page)

                    yield scrapy.FormRequest(
                        url=self.data_url,
                        method="POST",
                        headers=self.headers,
                        formdata=payload.copy(),
                        callback=self.parse,
                        meta={"page": self.current_page, "payload": payload}
                    )
            except Exception as e:
                self.logger.error(f"解析数据失败: {e}")


    def closed(self, reason):
        if self.progress_bar:
            self.progress_bar.close()
        self.logger.info(f"📦 ContractSpider 爬虫结束，原因：{reason}")

