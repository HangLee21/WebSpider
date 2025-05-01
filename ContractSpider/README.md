### conda使用规范
根据`environment.yml`创建环境
激活环境
windows系统下`/d`来进行分盘路径跳转
```bash
conda env create -f environment.yml
conda activate scrapy_env
cd /d F:/xxxx
```

### 配置代理账号密码
```
authKey = xxxx
password = xxxx
proxyAddr = xxxx
```

### 爬取搜索页

* 直接爬取 通过设置settings来限制时间
* 修改`settings.py`限制运行时间范围
```python
CONTRACT_START_DATE = "2025-03-04"
CONTRACT_END_DATE = "2025-03-05"
```
* 运行代码
```
scrapy crawl contract
```
* 也可通过参数传递时间
```
scrapy crawl contract -a CONTRACT_START_DATE=2025-03-04 -a CONTRACT_END_DATE=2025-03-10
```

结果存储到`downloads`文件夹下

### 爬取详情页
基于前者进行爬取，运行前需要确保**搜索页已经爬取**

* 修改`settings.py`限制运行时间范围
```python
DETAIL_START_DATE = "2025-03-05"
DETAIL_END_DATE = "2025-03-05"
```
* 运行代码
```
scrapy crawl detail
```
* 也可通过参数传递时间
```
scrapy crawl detail -a DETAIL_START_DATE=2025-03-04 -a DETAIL_END_DATE=2025-03-10
```
结果存储到`detail_downloads`文件夹下


### 下载附件
基于前者进行爬取，运行前需要确保**详情页已经爬取**

* 修改`settings.py`限制运行时间范围
```python
ATTACHMENT_START_DATE = "2025-03-04"
ATTACHMENT_END_DATE = "2025-03-05"
```
* 运行代码
```
scrapy crawl attachment
```
* 也可通过参数传递时间
```
scrapy crawl attachment -a ATTACHMENT_START_DATE=2025-05-31 -a ATTACHMENT_END_DATE=2025-06-01
```
结果存储到`attachments`文件夹下

* 重跑失败任务
```
scrapy crawl attachment -a retry_failed=1
```
