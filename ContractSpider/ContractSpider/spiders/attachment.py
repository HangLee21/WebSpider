import os
import json
import logging
import scrapy
import pandas as pd
from datetime import datetime
from scrapy.utils.project import get_project_settings
from tqdm import tqdm


class AttachmentSpider(scrapy.Spider):
    name = "attachment"

    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'ContractSpider.middlewares.AttachmentProxyMiddleware': 300,
        },
        'LOG_ENABLED': False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.settings = get_project_settings()
        self.start_date = kwargs.get("ATTACHMENT_START_DATE")
        self.end_date = kwargs.get("ATTACHMENT_END_DATE")
        self.max_retry = 3

        today = datetime.now()
        log_filename = f"attachment_{today.year}_{today.month:02d}_{today.day:02d}.log"
        log_path = os.path.join("logs", log_filename)
        os.makedirs("logs", exist_ok=True)

        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            filemode='w',
        )

        self.custom_logger = logging.getLogger("AttachmentSpider")
        self.custom_logger.setLevel(logging.INFO)
        handler = logging.FileHandler(log_path, encoding="utf-8")
        formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(filename)s:%(lineno)d - %(message)s')
        handler.setFormatter(formatter)
        self.custom_logger.addHandler(handler)
        self.custom_logger.propagate = False
        self.custom_logger.info("日志初始化完成 ✅")

        if not self.start_date:
            self.start_date = self.settings.get('ATTACHMENT_START_DATE')
        if not self.end_date:
            self.end_date = self.settings.get('ATTACHMENT_END_DATE')

        self.downloads_folder = "detail_downloads"
        self.save_folder = "attachments"
        self.target_column = "附件下载链接"
        self.contract_number_column = "合同编号"
        self.contract_name_column = "合同名称"
        self.attachment_data = self.extract_links()
        self.progress_bar = None

    def extract_links(self):
        attachment_list = []

        try:
            start_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
        except Exception as e:
            self.custom_logger.error(f"⚠️ 日期格式错误: {e}")
            return []

        for folder_name in os.listdir(self.downloads_folder):
            folder_path = os.path.join(self.downloads_folder, folder_name)
            if not os.path.isdir(folder_path):
                continue

            for file_name in os.listdir(folder_path):
                if not file_name.endswith(".xlsx"):
                    continue

                # 从文件名中解析出日期部分（假设文件名格式为 yyyy-mm-dd.xlsx）
                try:
                    file_date = datetime.strptime(file_name.replace(".xlsx", ""), "%Y-%m-%d")
                except ValueError:
                    continue  # 跳过非日期命名的文件

                if start_dt <= file_date <= end_dt:
                    file_path = os.path.join(folder_path, file_name)
                    attachment_list.extend(self.process_excel(file_path))

        return attachment_list

    def process_excel(self, file_path):
        try:
            df = pd.read_excel(file_path, engine="openpyxl")
        except Exception as e:
            self.custom_logger.error(f"❌ 无法读取 Excel 文件 {file_path}，跳过处理。错误信息: {e}")
            return []

        if self.target_column not in df.columns or \
           self.contract_number_column not in df.columns or \
           self.contract_name_column not in df.columns:
            self.custom_logger.error(f"⚠️ {file_path} 缺少必要列，跳过处理。")
            return []

        attachment_list = []
        for _, row in df.iterrows():
            contract_date = row.get("合同公告日期", "")
            if not self.is_within_date_range(contract_date):
                continue

            contract_number = row[self.contract_number_column]
            contract_name = row[self.contract_name_column]
            attachment_links = row[self.target_column]

            if pd.isna(attachment_links):
                continue

            links = [link.strip() for link in str(attachment_links).split(",") if link.strip()]
            for index, link in enumerate(links, start=1):
                try:
                    folder_name = datetime.strptime(contract_date, "%Y-%m-%d").strftime("%Y-%m")
                except Exception:
                    folder_name = "未知日期"
                save_name = f"{contract_number}_{contract_name}_{index}.pdf"
                attachment_list.append({
                    "folder_name": folder_name,
                    "file_name": save_name,
                    "url": link
                })

        self.custom_logger.info(f"✅ 提取到: {len(attachment_list)} 个链接")
        return attachment_list

    def is_within_date_range(self, contract_date):
        if not contract_date or pd.isna(contract_date):
            return False
        try:
            contract_date = datetime.strptime(str(contract_date), "%Y-%m-%d")
        except ValueError:
            return False
        if self.start_date and contract_date < datetime.strptime(self.start_date, "%Y-%m-%d"):
            return False
        if self.end_date and contract_date > datetime.strptime(self.end_date, "%Y-%m-%d"):
            return False
        return True

    def start_requests(self):
        total_files = len(self.attachment_data)
        self.progress_bar = tqdm(total=total_files, desc="下载进度", ncols=80)

        for item in self.attachment_data:
            folder_path = os.path.join(self.save_folder, item["folder_name"])
            os.makedirs(folder_path, exist_ok=True)
            file_path = os.path.join(folder_path, item["file_name"])
            if os.path.exists(file_path):
                self.custom_logger.info(f"文件已存在，跳过下载: {file_path}")
                self.progress_bar.update(1)
                continue

            request = scrapy.Request(
                method="GET",
                url=item["url"],
                headers={
                    'Connection': 'close',
                    'Referer': 'http://htgs.ccgp.gov.cn/',
                },
                meta={
                    "file_path": file_path,
                    "file_name": item["file_name"],
                    "retry_count": 0,
                },
                callback=self.save_attachment,
                errback=self.handle_error
            )
            yield request

    def save_attachment(self, response):
        file_path = response.meta["file_path"]
        with open(file_path, "wb") as f:
            f.write(response.body)
        self.custom_logger.info(f"✅ 下载成功: {file_path}")
        self.progress_bar.update(1)

    def handle_error(self, failure):
        request = failure.request
        retry_count = request.meta.get("retry_count", 0)
        file_path = request.meta.get("file_path")
        file_name = request.meta.get("file_name")

        if retry_count < self.max_retry:
            new_request = request.copy()
            new_request.meta["retry_count"] = retry_count + 1
            self.custom_logger.warning(f"⚠️ 第 {retry_count + 1} 次重试: {request.url}")
            yield new_request
        else:
            self.custom_logger.error(f"❌ 最终失败: {request.url} => {file_path}")
            failed_item = {"url": request.url, "file_name": file_name}
            self.save_failed_task(failed_item)
            self.progress_bar.update(1)

    def save_failed_task(self, failed_item):
        failed_path = os.path.join("logs", "failed_downloads.json")
        os.makedirs(os.path.dirname(failed_path), exist_ok=True)

        try:
            if os.path.exists(failed_path):
                with open(failed_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                data = []
        except Exception as e:
            self.custom_logger.error(f"❌ 读取失败文件记录出错：{e}")
            data = []

        data.append(failed_item)

        try:
            with open(failed_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
        except Exception as e:
            self.custom_logger.error(f"❌ 保存失败记录出错：{e}")

    def closed(self, reason):
        if self.progress_bar:
            self.progress_bar.close()
        self.custom_logger.info(f"爬虫结束，原因：{reason}")
