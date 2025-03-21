import requests

authKey = '6ACC2B08'
password = '07E82A6EECAE'
proxyAddr = 'tun-uzqqwl.qg.net:19381'

proxyUrl = "http://%(user)s:%(password)s@%(server)s" % {
    "user": authKey,
    "password": password,
    "server": proxyAddr,
}  # 代理API URL，返回新的IP

proxies = {
    "http": proxyUrl,
    "https": proxyUrl,
}

targetURL = "https://test.ipw.cn"

resp = requests.get(targetURL, proxies=proxies)
print(resp.text)