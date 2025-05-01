import logging
import os
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
        'LOG_ENABLED': False,  # 禁用 Scrapy 默认日志
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
        'Connection': 'close',
        'Referer': 'http://htgs.ccgp.gov.cn/'
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.settings = get_project_settings()
        self.start_date = kwargs.get("ATTACHMENT_START_DATE", None)
        self.end_date = kwargs.get("ATTACHMENT_END_DATE", None)

        if not self.start_date:
            self.start_date = self.settings.get('ATTACHMENT_START_DATE')  # 起始日期

        if not self.end_date:
            self.end_date = self.settings.get('ATTACHMENT_END_DATE')  # 结束日期
        self.downloads_folder = "detail_downloads"  # Excel 存储根目录
        self.save_folder = "attachments"  # 附件存储根目录
        self.target_column = "附件下载链接"  # 需要提取的列名
        self.contract_number_column = "合同编号"  # 合同编号
        self.contract_name_column = "合同名称"  # 合同名称
        self.attachment_data = self.extract_links()  # 预提取附件信息

        today = datetime.now()
        log_filename = f"attachment_{today.year}_{today.month:02d}_{today.day:02d}.log"
        log_path = os.path.join("logs", log_filename)
        os.makedirs("logs", exist_ok=True)

        # 配置自定义的 logging
        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            filemode='w',  # 每次运行覆盖旧日志
        )

        self.custom_logger = logging.getLogger("AttachmentSpider")  # 使用自定义的 custom_logger
        self.custom_logger.setLevel(logging.INFO)
        handler = logging.FileHandler(log_path, encoding="utf-8")
        formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(filename)s:%(lineno)d - %(message)s')
        handler.setFormatter(formatter)
        self.custom_logger.addHandler(handler)
        self.custom_logger.propagate = False  # 防止打印到终端
        self.custom_logger.info("日志初始化完成 ✅")

        # 初始化 tqdm 进度条
        self.progress_bar = None

    def extract_links(self):
        """遍历 detail_downloads 文件夹并提取附件下载链接"""
        attachment_list = []

        for folder_name in os.listdir(self.downloads_folder):
            folder_path = os.path.join(self.downloads_folder, folder_name)
            if not os.path.isdir(folder_path):
                continue

            for file_name in os.listdir(folder_path):
                if file_name.endswith(".xlsx"):
                    file_path = os.path.join(folder_path, file_name)
                    attachment_list.extend(self.process_excel(file_path))

        return attachment_list

    def process_excel(self, file_path):
        """解析 Excel 文件中的附件信息"""
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

        return attachment_list

    def is_within_date_range(self, contract_date):
        """检查合同公告日期是否在指定范围内"""
        if not contract_date or pd.isna(contract_date):
            return False

        try:
            contract_date = datetime.strptime(str(contract_date), "%Y-%m-%d")
        except ValueError:
            return False

        if self.start_date and contract_date < datetime.strptime(self.start_date, "%Y-%m-%d"):
            return False
        if self.end_date and contract_date >= datetime.strptime(self.end_date, "%Y-%m-%d"):
            return False
        return True

    def handle_error(self, failure):
        """处理下载错误"""
        self.custom_logger.error(f"❌ 下载失败: {failure.request.url}")
        if self.progress_bar:
            self.progress_bar.update(1)

    def start_requests(self):
        """根据提取的链接发送下载请求"""
        total_files = len(self.attachment_data)
        # self.progress_bar = tqdm(total=total_files, desc="下载进度", ncols=80)

        for item in self.attachment_data:
            folder_path = os.path.join(self.save_folder, item["folder_name"])
            os.makedirs(folder_path, exist_ok=True)

            file_path = os.path.join(folder_path, item["file_name"])
            if os.path.exists(file_path):
                self.custom_logger.info(f"文件已存在，跳过下载: {file_path}")
                # self.progress_bar.update(1)
                continue

            request = scrapy.Request(
                method="GET",
                url=item["url"],
                headers=self.headers,
                meta={"file_path": file_path},
                callback=self.save_attachment,
                errback=self.handle_error
            )
            yield request


    def save_attachment(self, response):
        """保存附件到本地"""
        file_path = response.meta["file_path"]
        with open(file_path, "wb") as f:
            f.write(response.body)
        self.custom_logger.info(f"✅ 下载成功: {file_path}")
        # self.progress_bar.update(1)

    def closed(self, reason):
        """爬虫结束时关闭进度条"""
        if self.progress_bar:
            self.progress_bar.close()
        self.custom_logger.info(f"爬虫结束，原因：{reason}")
