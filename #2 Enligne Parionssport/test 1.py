from curl_cffi import requests
import json
from bs4 import BeautifulSoup


# For saving json files
def save_json_file(file_path: str, data: dict) -> None:
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"{file_path} saved successfully!")
    # logging.info("%s saved successfully!", file_path)

# For saving HTML files (Only for testing and debugging)
def save_html_file(file_path: str, html: str) -> None:
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"{file_path} saved successfully!")
    # logging.info("%s saved successfully!", file_path)

# For extracting raw json from unrendered html
def extract_raw_json_from_html(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    script_tag = soup.find("script", {"id": "serverApp-state"})

    if not script_tag or not script_tag.string:
        print("An internal error occurred!")
        return {}
    
    return json.loads(script_tag.string)

cookies = {
    'PIM-SESSION-ID': 'wfxeMVpkrmqamf3a',
    'TCPID': '12622151502039465016',
    'pa_privacy': '%22optin%22',
    'tc_sample_1881_1885': '1',
    'et': '1',
    'CAID': '202512300151313926512313',
    'TC_PRIVACY_PSEL': '0%40016%7C46%7C1881%408%2C9%2C10%40%401770715912582%2C1770715912582%2C1786267912582%40',
    'TC_PRIVACY_PSEL_CENTER': '8%2C9%2C10',
    '_pcid': '%7B%22browserId%22%3A%22mlgej8phybt7y1nj%22%2C%22_t%22%3A%22n14tgrrs%7Cmlgejafs%22%7D',
    'pa_vid': '%22mlgej8phybt7y1nj%22',
    '_pctx': '%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAE0RXSwH18ykAjABZ8AcxgwAHgB8AtqlFQAVoQBmkkAF8gA',
    'loop_num': 'd7841cca-3179-4fa7-86e4-71cd4e2a8f98%3AMFDe7%7CDR%7C%2F',
    'kameleoonVisitorCode': 'yxg7b71xopgfyf25',
    '_gcl_au': '1.1.643701667.1770715913',
    '_fbp': 'fb.1.1770715913540.611750014570864368',
    '_clck': '1phz8qh%5E2%5Eg3g%5E0%5E2232',
    'ry_ry-p4r1p53l_so_realytics': 'eyJpZCI6InJ5XzE3NDE0OTU5LUMxMDEtNEU4Ni1BNThELUI1N0QyMEM5QzREQSIsImNpZCI6bnVsbCwib3JpZ2luIjp0cnVlLCJyZWYiOm51bGwsImNvbnQiOm51bGwsIm5zIjp0cnVlLCJzYyI6Im9rIiwic3AiOm51bGx9',
    'ry_ry-p4r1p53l_realytics': 'eyJpZCI6InJ5XzE3NDE0OTU5LUMxMDEtNEU4Ni1BNThELUI1N0QyMEM5QzREQSIsImNpZCI6bnVsbCwiZXhwIjoxODAyMjUxOTEzODc4LCJjcyI6MX0%3D',
    '_sp_srt_ses.6807': '*',
    '_pcus': 'eyJ1c2VyU2VnbWVudHMiOm51bGwsIl90IjoibjE0dGd3emh8bWxnZWpmbmgifQ%3D%3D',
    'abp-pselw': '1065943818.41733.0000',
    'etuix': 'xYEiposi7LgLG2xLpvDaBhjiwK51SX2.2b.1KpFMCG.sEg7NStkgAA--',
    '_uetsid': '55c66030066311f1a11a09678120072b|imnejb|2|g3g|0|2232',
    'et0': 'CfYHnFWoraCjs2ezVuMTIE0.vno6t8T2dyAG2VXzVapxPEV_34lKQMHKLMYEQ8pF3R05usk5HKgJON1vsTFISJp4wWfvIFdDPFSfjvXf8NRfQ.5owHGwOpg-',
    '_sp_srt_id.6807': '5a21dc35-838c-4664-abe4-89d9658945a1.1770715914.1.1770717568..302293f1-732b-4889-8467-7abb762d8b42....0',
    '_clsk': '1ahqpr1%5E1770717567641%5E15%5E1%5Eh.clarity.ms%2Fcollect',
    '_uetvid': '55c680e0066311f18c0df5588ed7043c|1l7ik61|1770717567901|16|1|bat.bing.com/p/insights/c/h',
    '_MFB_': 'fHw3OXx8fFtdfHx8NjcuNTA3NDQ3NzI2NzYzMDJ8',
    'datadome': '2naq1vxZMpWy8hpnBnZLL3gU3edkQiGYUrRzASXK3VAOZW65IucjbhkLrZ7_MR6bD23HvGKHHvicbgAWm1s6X6MML322TAqgTi4T_rfoxHr73f_AQfR9LQWY98D7csz_',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'sec-ch-device-memory': '8',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-full-version-list': '"Not)A;Brand";v="8.0.0.0", "Chromium";v="138.0.7204.184", "Google Chrome";v="138.0.7204.184"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    # 'cookie': 'PIM-SESSION-ID=wfxeMVpkrmqamf3a; TCPID=12622151502039465016; pa_privacy=%22optin%22; tc_sample_1881_1885=1; et=1; CAID=202512300151313926512313; TC_PRIVACY_PSEL=0%40016%7C46%7C1881%408%2C9%2C10%40%401770715912582%2C1770715912582%2C1786267912582%40; TC_PRIVACY_PSEL_CENTER=8%2C9%2C10; _pcid=%7B%22browserId%22%3A%22mlgej8phybt7y1nj%22%2C%22_t%22%3A%22n14tgrrs%7Cmlgejafs%22%7D; pa_vid=%22mlgej8phybt7y1nj%22; _pctx=%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAE0RXSwH18ykAjABZ8AcxgwAHgB8AtqlFQAVoQBmkkAF8gA; loop_num=d7841cca-3179-4fa7-86e4-71cd4e2a8f98%3AMFDe7%7CDR%7C%2F; kameleoonVisitorCode=yxg7b71xopgfyf25; _gcl_au=1.1.643701667.1770715913; _fbp=fb.1.1770715913540.611750014570864368; _clck=1phz8qh%5E2%5Eg3g%5E0%5E2232; ry_ry-p4r1p53l_so_realytics=eyJpZCI6InJ5XzE3NDE0OTU5LUMxMDEtNEU4Ni1BNThELUI1N0QyMEM5QzREQSIsImNpZCI6bnVsbCwib3JpZ2luIjp0cnVlLCJyZWYiOm51bGwsImNvbnQiOm51bGwsIm5zIjp0cnVlLCJzYyI6Im9rIiwic3AiOm51bGx9; ry_ry-p4r1p53l_realytics=eyJpZCI6InJ5XzE3NDE0OTU5LUMxMDEtNEU4Ni1BNThELUI1N0QyMEM5QzREQSIsImNpZCI6bnVsbCwiZXhwIjoxODAyMjUxOTEzODc4LCJjcyI6MX0%3D; _sp_srt_ses.6807=*; _pcus=eyJ1c2VyU2VnbWVudHMiOm51bGwsIl90IjoibjE0dGd3emh8bWxnZWpmbmgifQ%3D%3D; abp-pselw=1065943818.41733.0000; etuix=xYEiposi7LgLG2xLpvDaBhjiwK51SX2.2b.1KpFMCG.sEg7NStkgAA--; _uetsid=55c66030066311f1a11a09678120072b|imnejb|2|g3g|0|2232; et0=CfYHnFWoraCjs2ezVuMTIE0.vno6t8T2dyAG2VXzVapxPEV_34lKQMHKLMYEQ8pF3R05usk5HKgJON1vsTFISJp4wWfvIFdDPFSfjvXf8NRfQ.5owHGwOpg-; _sp_srt_id.6807=5a21dc35-838c-4664-abe4-89d9658945a1.1770715914.1.1770717568..302293f1-732b-4889-8467-7abb762d8b42....0; _clsk=1ahqpr1%5E1770717567641%5E15%5E1%5Eh.clarity.ms%2Fcollect; _uetvid=55c680e0066311f18c0df5588ed7043c|1l7ik61|1770717567901|16|1|bat.bing.com/p/insights/c/h; _MFB_=fHw3OXx8fFtdfHx8NjcuNTA3NDQ3NzI2NzYzMDJ8; datadome=2naq1vxZMpWy8hpnBnZLL3gU3edkQiGYUrRzASXK3VAOZW65IucjbhkLrZ7_MR6bD23HvGKHHvicbgAWm1s6X6MML322TAqgTi4T_rfoxHr73f_AQfR9LQWY98D7csz_',
}

response = requests.get(
    'https://www.enligne.parionssport.fdj.fr/paris-football/azerbaidjan/d1-azerbaidjan/3317637/shamakhi-fc-vs-fc-qarabag',
    cookies=cookies,
    headers=headers,
)

print(response.headers)
print(response.status_code)

if response.status_code == 200:
    save_html_file("betting test.html", response.text)
    save_json_file("betting test.json", extract_raw_json_from_html(response.text))
    