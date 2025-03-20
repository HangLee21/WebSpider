import requests
import json

# 目标 URL
url = "http://htgs.ccgp.gov.cn/GS8/contractpublish/getContractByAjax?contractSign=0"

# 请求头
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",
    "Content-Type": "application/json"
}

# 需要发送的 JSON 数据
payload = {
    "searchContractCode": "",
    "searchContractName": "",
    "searchProjCode": "",
    "searchProjName": "",
    "searchPurchaserName": "",
    "searchSupplyName": "",
    "searchAgentName": "",
    "searchPlacardStartDate": "2025-03-04",
    "searchPlacardEndDate": "2025-03-09",
    "code": "HMAA",
    "isChange": "",
    "currentPage": 1,
    "codeResult": "c63aa87bb37c42155b76a96a776540ba"
}

code_url = 'http://htgs.ccgp.gov.cn/GS8/genCodeImg?t={}'
code_get = 'http://htgs.ccgp.gov.cn/GS8/upload/verifyCodes/{}.jpg'
# 发送 POST 请求
try:
    # response = requests.post(url, headers=headers, params=payload, timeout=10)
    # response = requests.post(code_url.format(0.12312312), headers=headers, timeout=10)
    # print(response.text)
    file_response = requests.get('http://htgs.ccgp.gov.cn/GS8/upload/verifyCodes/00004bbedcee85e67c4fea9e13f90883.jpg', headers=headers, timeout=10)
    print(file_response)
    # response.raise_for_status()
except requests.exceptions.RequestException as e:
    print(f"请求失败: {e}")

