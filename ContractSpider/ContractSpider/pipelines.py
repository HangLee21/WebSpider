import os
import pandas as pd
from datetime import datetime
from scrapy.exceptions import DropItem

class ContractPipeline:
    def process_item(self, item, spider):
        file_path = item.get("file_path")
        if not file_path:
            return item  # 跳过无效数据

        # **将数据转换为 DataFrame**
        data = {
            "签订日期": [item["sign_date"]],
            "发布时间": [item["publish_date"]],
            "采购人": [item["purchaser"]],
            "供应商": [item["supplier"]],
            "代理机构": [item["agent"]],
            "合同名称": [item["contract_name"]],
            "项目名称": [item["project_name"]],
            "网页链接": [item["contract_link"]],
        }

        df = pd.DataFrame(data)

        # **保存到 Excel**
        if os.path.exists(file_path):
            # 文件已存在时，追加数据
            with pd.ExcelWriter(file_path, mode="a", if_sheet_exists="overlay", engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Contracts", header=False,
                            startrow=writer.sheets["Contracts"].max_row)
        else:
            # 文件不存在时，创建新文件
            df.to_excel(file_path, index=False, sheet_name="Contracts")

        # spider.logger.info(f"保存合同数据: {file_path}")
        return item




class DetailPipeline:
    def __init__(self):
        # 存储目录
        self.base_folder = "detail_downloads"
        os.makedirs(self.base_folder, exist_ok=True)

        # 英文字段名与中文表头的映射
        self.headers_map = {
            "contract_number": "合同编号",
            "contract_name": "合同名称",
            "project_number": "项目编号",
            "project_name": "项目名称",
            "purchaser": "采购人（甲方）",
            "purchaser_address": "采购人地址",
            "purchaser_contact": "采购人联系方式",
            "supplier": "供应商（乙方）",
            "supplier_address": "供应商地址",
            "supplier_contact": "供应商联系方式",
            "main_product_name": "主要标的名称",
            "specifications": "规格型号（或服务要求）",
            "quantity": "主要标的数量",
            "unit_price": "主要标的单价",
            "contract_amount": "合同金额",
            "performance_location": "履约地点",
            "procurement_method": "采购方式",
            "contract_sign_date": "合同签订日期",
            "contract_announcement_date": "合同公告日期",
            "attachment_name": "附件名称",
            "attachment_download_url": "附件下载链接"
        }

    def process_item(self, item, spider):
        # 确保 item 包含 `contract_announcement_date`
        if 'contract_announcement_date' not in item or not item['contract_announcement_date']:
            raise DropItem("Missing contract_announcement_date in %s" % item)

        # 解析合同公告日期
        try:
            announcement_date = datetime.strptime(item['contract_announcement_date'], "%Y-%m-%d")
        except ValueError:
            raise DropItem(f"Invalid contract_announcement_date format: {item['contract_announcement_date']}")

        # 生成目录结构
        folder_name = announcement_date.strftime("%Y-%m")
        folder_path = os.path.join(self.base_folder, folder_name)
        os.makedirs(folder_path, exist_ok=True)

        # 生成文件路径
        file_name = f"{announcement_date.strftime('%Y-%m-%d')}.xlsx"
        file_path = os.path.join(folder_path, file_name)

        # 转换 `attachment_name` 和 `attachment_download_url` 为字符串
        item["attachment_name"] = ", ".join(item["attachment_name"]) if isinstance(item["attachment_name"], list) else item["attachment_name"]
        item["attachment_download_url"] = ", ".join(item["attachment_download_url"]) if isinstance(item["attachment_download_url"], list) else item["attachment_download_url"]

        # 转换 item 为 DataFrame
        item_dict = {self.headers_map[key]: value for key, value in dict(item).items() if key in self.headers_map}
        df = pd.DataFrame([item_dict])

        # 判断文件是否存在
        if os.path.exists(file_path):
            # 追加模式，不写入表头
            df.to_excel(file_path, index=False, header=False, engine='openpyxl', mode="a")
        else:
            # 新建文件，写入表头
            df.to_excel(file_path, index=False, engine='openpyxl')

        spider.logger.info(f"Detail item saved: {file_path}")
        return item


