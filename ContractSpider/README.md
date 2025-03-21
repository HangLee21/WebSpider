### 安装scrapy
```
pip install scrapy
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
ATTACHMENT_START_DATE = "2025-03-05"
ATTACHMENT_END_DATE = "2025-03-05"
```
* 运行代码
```
scrapy crawl attachment
```
* 也可通过参数传递时间
```
scrapy crawl attachment -a ATTACHMENT_START_DATE=2025-03-04 -a ATTACHMENT_END_DATE=2025-03-10
```
结果存储到`attachments`文件夹下
