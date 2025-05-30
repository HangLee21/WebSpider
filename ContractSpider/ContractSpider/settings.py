# Scrapy settings for ContractSpider project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "ContractSpider"

SPIDER_MODULES = ["ContractSpider.spiders"]
NEWSPIDER_MODULE = "ContractSpider.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "ContractSpider (+http://www.yourdomain.com)"

# Obey robots.txt rules
# ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "ContractSpider.middlewares.ContractspiderSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    "ContractSpider.middlewares.ContractspiderDownloaderMiddleware": 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    "ContractSpider.pipelines.ContractspiderPipeline": 300,
#}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

DOWNLOAD_FAIL_ON_DATALOSS = False

DOWNLOAD_MAXSIZE = 0  # 0 表示不限制大小

# Set settings whose default value is deprecated to a future-proof value
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# 搜索页时间范围 左闭右开
CONTRACT_START_DATE = "2023-11-01"
CONTRACT_END_DATE = "2023-11-06"

# 详情页时间范围 需前者已经爬取
DETAIL_START_DATE = "2025-03-10"
DETAIL_END_DATE = "2025-03-11"

# 附件页时间范围 需前者已经爬取
ATTACHMENT_START_DATE = "2022-11-01"
ATTACHMENT_END_DATE = "2022-11-02"

ROBOTSTXT_OBEY = False  # 是否遵守 robots.txt 规则
DOWNLOAD_DELAY = 5  # 避免被封，延迟 2 秒
CONCURRENT_REQUESTS = 8  # 并发数
COOKIES_ENABLED = False  # 禁用 Cookies
LOG_ENABLED = False

ITEM_PIPELINES = {
    'ContractSpider.pipelines.ContractPipeline': 300, # 搜索页管道
    'ContractSpider.pipelines.DetailPipeline': 400, # 详情页管道
}

# 青果代理
#
authKey = '726E4307'
password = 'A404C43ABFCE'
proxyAddr = 'tun-buhuph.qg.net:19518'

PROXY_API_URL = "http://%(user)s:%(password)s@%(server)s" % {
    "user": authKey,
    "password": password,
    "server": proxyAddr,
}  # 代理API URL，返回新的IP

DOWNLOADER_MIDDLEWARES = {
    'ContractSpider.middlewares.RotateProxyMiddleware': 400,  # 搜索页中间件
    'ContractSpider.middlewares.DetailProxyMiddleware': 500,  # 详情页中间件
    'ContractSpider.middlewares.AttachmentProxyMiddleware': 600,  # 附件页中间件
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 800,  # 系统默认代理中间件

}

RETRY_ENABLED = True
RETRY_TIMES = 5
RETRY_HTTP_CODES = [403, 404, 407, 500, 502, 503, 504]
HTTPERROR_ALLOWED_CODES = [403, 404, 407, 500, 502, 503, 504]

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
DOWNLOAD_TIMEOUT = 120


