import os
import glob
import pandas as pd
from scrapy.utils.project import get_project_settings

class DetailsExtractor:
    def __init__(self, start_date, end_date):
        self.settings = get_project_settings()
        self.start_date = start_date
        self.end_date = end_date
        self.downloads_folder = "downloads"  # 根目录
        self.target_column = "网页链接"  # 需要提取的列名
        self.urls = []

    def get_matching_files(self):
        """获取符合日期范围的 Excel 文件（遍历子文件夹）"""
        matching_files = []
        start_month = self.start_date[:7]  # 例如 "2025-03"
        end_month = self.end_date[:7]  # 例如 "2025-04"

        for folder in os.listdir(self.downloads_folder):
            folder_path = os.path.join(self.downloads_folder, folder)
            if not os.path.isdir(folder_path):
                continue  # 跳过非文件夹

            if start_month <= folder <= end_month:  # 只处理符合时间范围的子文件夹
                for file_path in glob.glob(os.path.join(folder_path, "*.xlsx")):
                    filename = os.path.basename(file_path)
                    date_str = filename.split('.')[0]  # 假设文件名格式为 YYYY-MM-DD.xlsx
                    if self.start_date <= date_str <= self.end_date:
                        matching_files.append(file_path)

        return matching_files

    def extract_urls(self):
        """从 Excel 文件中提取 URL"""
        matching_files = self.get_matching_files()
        if not matching_files:
            print("❌ 未找到匹配的 Excel 文件")
            return []

        for file_path in matching_files:
            try:
                df = pd.read_excel(file_path, dtype=str)  # 读取 Excel 文件
                if self.target_column in df.columns:
                    urls = df[self.target_column].dropna().tolist()  # 读取非空的网页链接
                    self.urls.extend(urls)
                    print(f"✅ 从 {file_path} 提取 {len(urls)} 个链接")
                else:
                    print(f"⚠️ {file_path} 中未找到 '{self.target_column}' 列")
            except Exception as e:
                print(f"❌ 读取 {file_path} 失败: {e}")

        return self.urls

if __name__ == "__main__":
    extractor = DetailsExtractor()
    urls = extractor.extract_urls()
    print(f"📌 共提取 {len(urls)} 个链接")
