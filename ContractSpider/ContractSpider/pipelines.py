import os
import pandas as pd

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
