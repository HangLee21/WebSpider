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
from scrapy.http import HtmlResponse


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
        max_retries = 10
        retry_delay = 3
        for attempt in range(max_retries):
            try:
                new_proxy = self.get_new_proxy()
                request.meta['proxy'] = new_proxy
                return
            except Exception as e:
                spider.custom_logger.error(f"获取代理失败，第 {attempt + 1} 次重试: {e}")
                time.sleep(retry_delay)

        spider.custom_logger.error("超过最大重试次数，放弃设置代理")


    def process_response(self, request, response, spider):
        """处理非200状态请求，超过最大重试次数则返回空响应，避免程序中断"""

        if response.status != 200:
            start_date = request.meta.get('searchPlacardStartDate', '')
            end_date = request.meta.get('searchPlacardEndDate', '')
            page = request.meta.get('page', '')
            url = request.url
            retry_count = self.failed_urls.get(url, 0) + 1
            self.failed_urls[url] = retry_count

            if retry_count >= self.MAX_RETRY_COUNT:
                spider.custom_logger.error(
                    f"[跳过] Start Date {start_date}, End Date {end_date}, Page {page} - 失败 {retry_count} 次，状态码 {response.status}"
                )
                self.save_failed_json(start_date, end_date, page, url, spider)

                # ✅ 构造空响应，避免后续处理失败内容
                return HtmlResponse(
                    url=response.url,
                    status=response.status,
                    request=request,
                    body=b"",
                    encoding='utf-8'
                )

            # ✅ 重试逻辑
            time.sleep(5)
            new_proxy = self.get_new_proxy()
            request.meta['proxy'] = new_proxy
            spider.custom_logger.warning(
                f"[重试] 状态码 {response.status} 第 {retry_count} 次，使用代理 {new_proxy} - {page}"
            )

            retry_request = get_retry_request(request, spider=spider, reason=f"Status {response.status}")
            if retry_request:
                retry_request.meta['proxy'] = new_proxy
                retry_request.dont_filter = True
                return retry_request
            else:
                return HtmlResponse(
                    url=response.url,
                    status=response.status,
                    request=request,
                    body=b"",
                    encoding='utf-8'
                )

        return response  # 正常响应返回

    def save_failed_json(self, start_date, end_date, page, url, spider):
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

        spider.custom_logger.info(f"已将失败请求保存到 {self.FAILED_JSON_FILE}: {failed_data}")




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
        max_retries = 10
        retry_delay = 3
        for attempt in range(max_retries):
            try:
                new_proxy = self.get_new_proxy()
                request.meta['proxy'] = new_proxy
                return
            except Exception as e:
                spider.custom_logger.error(f"获取代理失败，第 {attempt + 1} 次重试: {e}")
                time.sleep(retry_delay)

        spider.custom_logger.error("超过最大重试次数，放弃设置代理")

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
                spider.custom_logger.warning(f"重试 {retry_times}/{self.MAX_RETRY_COUNT} - {request.url}，状态码: {response.status}")
                return new_request
            else:
                # 记录失败的请求
                self.record_failed_request(request.url, response.status)
                spider.custom_logger.error(f"请求失败（已达最大重试次数）: {request.url} 状态码: {response.status}")
                # 返回一个“空”的响应对象，使 pipeline 继续往下走
                return HtmlResponse(
                    url=request.url,
                    status=response.status,
                    body=b"",  # 空内容
                    encoding='utf-8',
                    request=request
                )

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
            spider.custom_logger.warning(f"请求异常 {exception}，重试 {retry_times}/{self.MAX_RETRY_COUNT} - {request.url}")
            return new_request
        else:
            self.record_failed_request(request.url, str(exception))
            spider.custom_logger.error(f"请求异常失败（已达最大重试次数）: {request.url} 异常: {exception}")
            raise IgnoreRequest(f"请求异常失败: {request.url}")

    def record_failed_request(self, url, reason):
        """记录失败请求到 JSON 文件"""
        if url not in self.failed_urls:
            self.failed_urls[url] = reason

        with open(self.FAILED_JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(self.failed_urls, f, ensure_ascii=False, indent=4)


import os
from scrapy.exceptions import IgnoreRequest
import json
import time
from scrapy.http import HtmlResponse
from fake_useragent import UserAgent


class AttachmentProxyMiddleware:
    MAX_RETRY_COUNT = 5  # 最大重试次数
    FAILED_JSON_FILE = "failed_attachment.json"

    def __init__(self, api_url):
        self.api_url = api_url
        self.failed_urls = {}
        self.ua = UserAgent()

    @classmethod
    def from_crawler(cls, crawler):
        api_url = crawler.settings.get('PROXY_API_URL', '')
        return cls(api_url)

    def get_new_proxy(self):
        """从 API 获取代理地址"""
        # 可根据实际接口修改为请求远程 API
        return self.api_url

    def get_random_user_agent(self):
        """生成随机 User-Agent"""
        return self.ua.random

    def set_proxy_and_ua(self, request, spider, max_attempts=5, retry_delay=2):
        """为请求设置代理和 User-Agent，最多尝试 max_attempts 次"""
        for attempt in range(1, max_attempts + 1):
            try:
                new_proxy = self.get_new_proxy()
                random_ua = self.get_random_user_agent()
                request.meta['proxy'] = new_proxy
                request.headers['User-Agent'] = random_ua
                # spider.custom_logger.info(f"✅ 使用代理: {new_proxy}，User-Agent: {random_ua}")
                return  # 成功后退出
            except Exception as e:
                spider.custom_logger.warning(f"⚠️ 第 {attempt} 次设置代理/User-Agent 失败: {e}")
                time.sleep(retry_delay)

        spider.custom_logger.error(f"❌ 多次尝试设置代理和 User-Agent 均失败")

    def process_request(self, request, spider):
        """每个请求都设置代理和 User-Agent"""
        self.set_proxy_and_ua(request, spider)

    def process_response(self, request, response, spider):
        """请求失败时更换代理和 UA 并重试"""
        if response.status in [403, 429, 500, 502, 503, 504]:
            retry_count = request.meta.get("retry_count", 0)
            if retry_count < self.MAX_RETRY_COUNT:
                retry_request = request.copy()
                retry_request.meta["retry_count"] = retry_count + 1
                self.set_proxy_and_ua(retry_request, spider)
                spider.custom_logger.warning(f"⚠️ 第 {retry_count + 1} 次重试请求: {request.url},文件名: {request.meta.get('file_name')}")
                return retry_request

            # 达到最大重试次数
            self.failed_urls[request.url] = retry_count + 1
            spider.custom_logger.error(f"❌ 请求失败 {self.MAX_RETRY_COUNT} 次，记录失败: {request.url}")
            self.save_failed_urls(spider)

            return HtmlResponse(
                url=request.url,
                status=response.status,
                body=b"",
                encoding="utf-8",
                request=request,
            )

        return response

    def save_failed_urls(self, spider):
        """保存失败的 URL 到 JSON"""
        if self.failed_urls:
            with open(self.FAILED_JSON_FILE, "w", encoding="utf-8") as f:
                json.dump(self.failed_urls, f, indent=4, ensure_ascii=False)
            spider.custom_logger.info(f"📄 保存失败 URL 到 {self.FAILED_JSON_FILE}")


