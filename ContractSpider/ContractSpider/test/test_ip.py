import requests

authKey = '58DF86C4'
password = 'A3ADF69DC637'
proxyAddr = 'tun-hdxjvl.qg.net:15140'

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