import os
import json
import logging

import scrapy
import pandas as pd
from datetime import datetime

from filetype import filetype
from scrapy.utils.project import get_project_settings
from tqdm import tqdm
from urllib.parse import urlparse, parse_qs
import mimetypes
import requests

# 加了修改5.11

class AttachmentSpider(scrapy.Spider):
    name = "attachment"

    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'ContractSpider.middlewares.AttachmentProxyMiddleware': 300,
        },
        'LOG_ENABLED': False,
    }

    ACCEPTED_MIME_TYPES = {
        "application/pdf",
        "application/zip",
        "application/msword",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/octet-stream",
        "image/jpeg",
        "image/png",
        "image/gif",
        "audio/mpeg",
        "video/mp4",
        "application/json"
    }

    MIME_EXTENSION_MAP = {
        "application/wps-office.et": ".et",
        "application/wps-office.dps": ".dps",
        "application/wps-office.wps": ".wps",
        "application/pdf": ".pdf",
        "application/msword": ".doc",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/zip": ".zip",
        "application/octet-stream": "",  # fallback
        "application/x-rar": ".rar",
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "text/plain": ".txt"
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.settings = get_project_settings()
        self.start_date = kwargs.get("ATTACHMENT_START_DATE")
        self.end_date = kwargs.get("ATTACHMENT_END_DATE")
        self.retry_failed = kwargs.get("retry_failed", "0") == "1"  # 新增参数，用于控制是否重跑失败任务
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
        
        self.failed_tasks_path = os.path.join("logs", "failed_downloads.json")
        
        if self.retry_failed:
            self.custom_logger.info("📢 正在重跑失败任务模式...")
            self.attachment_data = self.load_failed_tasks()
        else:
            self.attachment_data = self.extract_links()
        
        self.progress_bar = None

    def load_failed_tasks(self):
        """加载失败的下载任务"""
        if not os.path.exists(self.failed_tasks_path):
            self.custom_logger.warning(f"⚠️ 未找到失败任务记录文件: {self.failed_tasks_path}")
            return []
            
        try:
            with open(self.failed_tasks_path, "r", encoding="utf-8") as f:
                failed_tasks = json.load(f)
                
            # 将失败任务转换为与extract_links()相同的格式
            formatted_tasks = []
            for task in failed_tasks:
                url = task.get("url")
                file_name = task.get("file_name")
                folder_name = task.get("folder_name")
                
                # 确保folder_name存在且格式正确
                if not folder_name or not isinstance(folder_name, str):
                    # 尝试从文件名中提取日期（如果有）
                    try:
                        # 假设文件名格式为"合同编号_合同名称_1.pdf"
                        parts = file_name.split("_")
                        if len(parts) >= 3:
                            # 尝试从文件名的第一部分（合同编号）中提取年月
                            contract_number = parts[0]
                            if contract_number.startswith("20") and len(contract_number) >= 6:
                                year_month = contract_number[:6]  # 如"202211"
                                folder_name = f"{year_month[:4]}-{year_month[4:6]}"  # 变为"2022-11"
                            else:
                                folder_name = "重试任务"
                        else:
                            folder_name = "重试任务"
                    except Exception:
                        folder_name = "重试任务"
                        
                self.custom_logger.info(f"📁 失败任务使用文件夹: {folder_name}, 文件: {file_name}")
                
                if not url or not file_name:
                    continue
                    
                formatted_tasks.append({
                    "url": url,
                    "file_name": file_name,
                    "folder_name": folder_name
                })
                
            self.custom_logger.info(f"✅ 加载了 {len(formatted_tasks)} 个失败任务")
            
                
            return formatted_tasks
        except Exception as e:
            self.custom_logger.error(f"❌ 读取失败任务文件出错: {e}")
            return []

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
                # 并不是全都是PDF
                ext = self.get_file_extension(link)
                save_name = f"{contract_number}_{contract_name}_{index}{ext}"
                attachment_list.append({
                    "folder_name": folder_name,
                    "file_name": save_name,
                    "url": link
                })

        self.custom_logger.info(f"✅ 从{file_path}中提取到: {len(attachment_list)} 个链接")
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
        
        if total_files == 0:
            self.custom_logger.warning("⚠️ 没有可下载的附件")
            return
            
        mode = "重跑失败任务" if self.retry_failed else "正常下载"
        self.custom_logger.info(f"🚀 开始{mode}，共 {total_files} 个文件")

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
                    "folder_name": item["folder_name"],
                    "retry_count": 0,
                },
                callback=self.save_attachment,
                errback=self.handle_error
            )
            yield request

    def save_attachment(self, response):
        file_path = response.meta["file_path"]
        self.custom_logger.info(f"📥 开始下载: {file_path}")
        # 保存原始文件
        with open(file_path, "wb") as f:
            f.write(response.body)
            self.custom_logger.info(f"✅ 原始文件保存成功: {file_path}")

        # 如果文件没有后缀名，尝试识别文件类型并重命名
        base, ext = os.path.splitext(file_path)
        if not ext:
            kind = filetype.guess(response.body)
            if kind:
                extension = kind.extension
                if extension == 'xls':
                    extension = 'docx'
                new_file_path = f"{file_path}.{extension}"
                os.rename(file_path, new_file_path)
                file_path = new_file_path
                self.custom_logger.info(f"🔁 文件类型识别成功，重命名为: {file_path}")
            else:
                self.custom_logger.warning(f"⚠️ 无法识别文件类型，保持原始文件名: {file_path}")
        else:
            self.custom_logger.info(f"✅ 文件已存在，跳过重命名: {file_path}")

        self.custom_logger.info(f"✅ 下载成功: {file_path}")
        self.progress_bar.update(1)

    def handle_error(self, failure):
        request = failure.request
        retry_count = request.meta.get("retry_count", 0)
        file_path = request.meta.get("file_path")
        file_name = request.meta.get("file_name")
        folder_name = request.meta.get("folder_name")

        if retry_count < self.max_retry:
            new_request = request.copy()
            new_request.meta["retry_count"] = retry_count + 1
            self.custom_logger.warning(f"⚠️ 第 {retry_count + 1} 次重试: {request.url}")
            yield new_request
        else:
            self.custom_logger.error(f"❌ 最终失败: {request.url} => {file_path}")
            # 记录文件夹信息的同时打印日志，方便调试
            self.custom_logger.info(f"📁 记录失败任务，文件夹: {folder_name}, 文件: {file_name}")
            failed_item = {
                "url": request.url, 
                "file_name": file_name,
                "folder_name": folder_name
            }
            self.save_failed_task(failed_item)
            self.progress_bar.update(1)

    def save_failed_task(self, failed_item):
        try:
            if os.path.exists(self.failed_tasks_path):
                with open(self.failed_tasks_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                data = []
        except Exception as e:
            self.custom_logger.error(f"❌ 读取失败文件记录出错：{e}")
            data = []

        data.append(failed_item)

        try:
            with open(self.failed_tasks_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            self.custom_logger.info(f"💾 保存失败记录成功: {failed_item.get('file_name')}")
        except Exception as e:
            self.custom_logger.error(f"❌ 保存失败记录出错：{e}")

    def closed(self, reason):
        if self.progress_bar:
            self.progress_bar.close()
        self.custom_logger.info(f"爬虫结束，原因：{reason}")

    def get_file_extension(self, url):
        # 1. 从 URL 路径中提取
        path = urlparse(url).path
        _, ext = os.path.splitext(path)
        if ext:
            return ext

        # 2. 从 URL 参数中提取
        query = urlparse(url).query
        params = parse_qs(query)
        for value_list in params.values():
            for value in value_list:
                _, ext = os.path.splitext(value)
                if ext:
                    return ext

        # 3. 从 Content-Type 判断（增加过滤）
        try:
            response = requests.head(url, allow_redirects=True, timeout=5)
            content_type = response.headers.get('Content-Type', '').split(';')[0].strip()
            if content_type in self.ACCEPTED_MIME_TYPES:
                guessed_ext = mimetypes.guess_extension(content_type)
                if guessed_ext:
                    return guessed_ext
        except Exception:
            pass

        # 4. 所有方法都失败时返回空字符串
        return ''
