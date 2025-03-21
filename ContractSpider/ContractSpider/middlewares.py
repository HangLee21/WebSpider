# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import os
import time
from datetime import datetime

import pandas as pd
from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from scrapy.downloadermiddlewares.retry import get_retry_request
from scrapy.exceptions import DropItem


class ContractspiderSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class ContractspiderDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


import time
import json
import logging
from scrapy.utils.request import fingerprint  # ✅ 适配新版 Scrapy
from scrapy.downloadermiddlewares.retry import get_retry_request


class RotateProxyMiddleware:
    MAX_RETRY_COUNT = 5  # 允许的最大重试次数
    FAILED_JSON_FILE = "failed_requests.json"  # 失败请求存储文件

    def __init__(self, api_url):
        self.api_url = api_url
        self.failed_urls = {}

    @classmethod
    def from_crawler(cls, crawler):
        api_url = crawler.settings.get('PROXY_API_URL', '')
        return cls(api_url)

    def get_new_proxy(self):
        """获取新的代理IP"""
        return self.api_url  # 假设 API 直接返回代理地址

    def process_request(self, request, spider):
        """为请求设置代理"""
        new_proxy = self.get_new_proxy()
        request.meta['proxy'] = new_proxy
        logging.info(f"使用代理 {new_proxy} 访问 {request.url}")

    def process_response(self, request, response, spider):
        """处理403，重试或记录失败URL"""
        if response.status != 200:
            logging.error(f'error {response.text}')
            start_date = request.meta['searchPlacardStartDate']
            end_date = request.meta['searchPlacardEndDate']
            page = request.meta['page']
            url = request.url
            fingerprint_hash = fingerprint(request)  # ✅ 计算唯一请求指纹
            self.failed_urls[fingerprint_hash] = self.failed_urls.get(fingerprint_hash, 0) + 1

            # **超过最大重试次数，记录失败URL并继续**
            if self.failed_urls[fingerprint_hash] >= self.MAX_RETRY_COUNT:
                logging.error(
                    f"Start Date {start_date} End Date {end_date} Page {page} 403 超过 {self.MAX_RETRY_COUNT} 次，放弃重试！")
                self.save_failed_json(start_date, end_date, page)
                return response  # **✅ 直接返回 response，让 Scrapy 继续执行**

            # **更换代理并重试**
            time.sleep(5)  # 避免请求过快
            new_proxy = self.get_new_proxy()
            request.meta['proxy'] = new_proxy
            logging.warning(f"403 错误，尝试使用新代理 {new_proxy} 重新请求 {page}")

            retry_request = get_retry_request(request, spider=spider, reason=f"403 error with proxy {new_proxy}")
            if retry_request:
                return retry_request  # **✅ 返回新 Request 进行重试**
            else:
                return response  # **✅ 避免 NoneType 错误，继续后续爬取**

        return response  # **✅ 正常请求返回 Response**

    def save_failed_json(self, start_date, end_date, page, url):
        """将失败的请求信息保存到 JSON 文件"""
        failed_data = {
            "start_date": start_date,
            "end_date": end_date,
            "page": page,
            "url": url
        }

        # **检查文件是否存在**
        if os.path.exists(self.FAILED_JSON_FILE):
            with open(self.FAILED_JSON_FILE, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    if not isinstance(data, list):  # 避免文件损坏导致的错误
                        data = []
                except json.JSONDecodeError:
                    data = []
        else:
            data = []

        # **追加新的失败请求**
        data.append(failed_data)

        # **写回 JSON 文件**
        with open(self.FAILED_JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logging.info(f"已将失败请求保存到 {self.FAILED_JSON_FILE}: {failed_data}")


import logging
import json
from scrapy.exceptions import IgnoreRequest


class DetailProxyMiddleware:
    MAX_RETRY_COUNT = 5  # 允许的最大重试次数
    FAILED_JSON_FILE = "failed_detail.json"  # 失败请求存储文件

    def __init__(self, api_url):
        self.api_url = api_url
        self.failed_urls = {}

    @classmethod
    def from_crawler(cls, crawler):
        api_url = crawler.settings.get('PROXY_API_URL', '')
        return cls(api_url)

    def get_new_proxy(self):
        """获取新的代理IP"""
        return self.api_url  # 假设 API 直接返回代理地址

    def process_request(self, request, spider):
        """为请求设置代理"""
        new_proxy = self.get_new_proxy()
        request.meta['proxy'] = new_proxy
        logging.info(f"使用代理 {new_proxy} 访问 {request.url}")

    def process_response(self, request, response, spider):
        """处理403或其他错误状态，进行重试或记录失败URL"""
        if response.status != 200:
            retry_times = request.meta.get('retry_times', 0)

            if retry_times < self.MAX_RETRY_COUNT:
                # 增加重试次数
                retry_times += 1
                new_request = request.copy()
                new_request.meta['retry_times'] = retry_times
                new_request.dont_filter = True  # 避免被 Scrapy 过滤掉
                time.sleep(5)  # 避免请求过快
                new_proxy = self.get_new_proxy()
                new_request.meta['proxy'] = new_proxy
                logging.warning(f"重试 {retry_times}/{self.MAX_RETRY_COUNT} - {request.url}，状态码: {response.status}")
                return new_request
            else:
                # 记录失败的请求
                self.record_failed_request(request.url, response.status)
                logging.error(f"请求失败（已达最大重试次数）: {request.url} 状态码: {response.status}")
                raise IgnoreRequest(f"请求失败（{response.status}）: {request.url}")

        return response

    def process_exception(self, request, exception, spider):
        """处理请求异常，例如代理失效"""
        retry_times = request.meta.get('retry_times', 0)

        if retry_times < self.MAX_RETRY_COUNT:
            retry_times += 1
            new_request = request.copy()
            new_request.meta['retry_times'] = retry_times
            new_request.dont_filter = True
            new_proxy = self.get_new_proxy()
            new_request.meta['proxy'] = new_proxy
            logging.warning(f"请求异常 {exception}，重试 {retry_times}/{self.MAX_RETRY_COUNT} - {request.url}")
            return new_request
        else:
            self.record_failed_request(request.url, str(exception))
            logging.error(f"请求异常失败（已达最大重试次数）: {request.url} 异常: {exception}")
            raise IgnoreRequest(f"请求异常失败: {request.url}")

    def record_failed_request(self, url, reason):
        """记录失败请求到 JSON 文件"""
        if url not in self.failed_urls:
            self.failed_urls[url] = reason

        with open(self.FAILED_JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(self.failed_urls, f, ensure_ascii=False, indent=4)


import json
import logging
import os
import random
import requests
from scrapy.exceptions import IgnoreRequest

class AttachmentProxyMiddleware:
    MAX_RETRY_COUNT = 5  # 允许的最大重试次数
    FAILED_JSON_FILE = "failed_attachment.json"  # 失败请求存储文件

    def __init__(self, api_url):
        self.api_url = api_url
        self.failed_urls = {}  # 记录失败 URL 及其重试次数

    @classmethod
    def from_crawler(cls, crawler):
        """从 Scrapy 配置文件 settings.py 获取代理 API"""
        api_url = crawler.settings.get('PROXY_API_URL', '')
        return cls(api_url)

    def get_new_proxy(self):
        """获取新的代理IP"""
        return self.api_url  # 假设 API 直接返回代理地址

    def process_request(self, request, spider):
        """为请求设置代理"""
        new_proxy = self.get_new_proxy()
        if new_proxy:
            request.meta['proxy'] = new_proxy
            logging.info(f"🛡️ 使用代理 {new_proxy} 访问 {request.url}")

    def process_response(self, request, response, spider):
        """处理异常响应（403、500），进行重试或记录失败"""
        if response.status in [403, 500]:  # 代理被封或服务器错误
            retry_count = request.meta.get("retry_count", 0)

            if retry_count < self.MAX_RETRY_COUNT:
                new_proxy = self.get_new_proxy()
                if new_proxy:
                    request.meta["proxy"] = new_proxy
                    request.meta["retry_count"] = retry_count + 1
                    logging.warning(f"⚠️ 请求 {request.url} 失败，使用新代理 {new_proxy} 进行第 {retry_count + 1} 次重试")
                    return request  # 重新尝试请求

            # 超过最大重试次数，记录失败 URL
            self.failed_urls[request.url] = retry_count + 1
            logging.error(f"❌ 请求 {request.url} 失败 {self.MAX_RETRY_COUNT} 次，记录失败")

            # 记录失败的 URL 到 JSON 文件
            self.save_failed_urls()

            raise IgnoreRequest(f"请求 {request.url} 多次失败，跳过")

        return response

    def save_failed_urls(self):
        """保存失败的 URL 到 JSON 文件"""
        if self.failed_urls:
            with open(self.FAILED_JSON_FILE, "w", encoding="utf-8") as f:
                json.dump(self.failed_urls, f, indent=4, ensure_ascii=False)
            logging.info(f"📄 失败的 URL 已保存到 {self.FAILED_JSON_FILE}")

