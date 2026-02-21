import re
import os
import json
import random
import asyncio
import aiofiles
import logging
import pandas
import unicodedata
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib import parse
from datetime import datetime
from curl_cffi import AsyncSession
from pydantic import BaseModel, ConfigDict, Field, model_validator


# Pydantic data models
class Selection(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int | None = Field(default=None)
    name: str | None = Field(default=None, alias="description")
    pos: int | None = Field(default=None)
    odd: str | float | int | None = Field(default=None, alias="price")
    conditional_bet_enabled: bool | None = Field(default=None, alias="conditionalBetEnabled")

class Market(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: int | None = Field(default=None)
    column_name: str | None = Field(default=None, alias="description")
    pos: int | None = Field(default=None)
    period: str | None = Field(default=None)
    style: str | None = Field(default=None)
    cashout: bool | None = Field(default=None)
    conditional_bet_enabled: bool | None = Field(default=None, alias="conditionalBetEnabled")
    selections: list[Selection] | None = Field(default_factory=list, alias="outcomes")

class GroupedMarket(BaseModel):
    model_config = ConfigDict(extra="ignore")
    market_name: str | None = Field(default=None, alias="description")
    markets: list[Market] | None = Field(default_factory=list)

class Match(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str | int | None = Field(default=None)
    opponent_a: dict | str | None = Field(default_factory=dict, alias="opponentA")
    opponent_b: dict | str | None = Field(default_factory=dict, alias="opponentB")
    desc_display: str | None = Field(default=None, alias="descDisplay")
    path: dict | None = Field(default_factory=dict)
    sport_code: str | None = Field(default=None, alias="sportCode")
    conditional_bet_enabled: bool | None = Field(default=None, alias="conditionalBetEnabled")
    cashout: bool | None = Field(default=None)
    stream_ref: str | None = Field(default=None, alias="streamRef")
    tv_channel: str | None = Field(default=None, alias="tvChannel")
    start: str | None = Field(default=None)
    grouped_markets: list[GroupedMarket] = Field(default_factory=list, alias="groupedMarkets")

    @model_validator(mode="before")
    @classmethod
    def normalize_match_api_response(cls, data: dict):
        if not isinstance(data, dict) or "items" not in data:
            return data
        
        items = data.get("items") or {}
        market_style_templates = data.get("marketStyleTemplates") or {}

        if not items:
            return data
        
        normalized_data = {}
        grouped = {}
        market_index = {}

        for id, gm in market_style_templates.items():
            grouped[str(id)] = {
                "description": gm.get("description"), 
                "markets": {}
            }
            
        for key, value in items.items():
            if key.startswith("e") and "id" not in normalized_data:
                normalized_data.update({
                    "id": key[1:],
                    "opponentA": value.get("a"),
                    "opponentB": value.get("b"),
                    "descDisplay": value.get("desc"),
                    "path": value.get("path"),
                    "sportCode": value.get("code"),
                    "conditionalBetEnabled": value.get("conditionalBetEnabled"),
                    "cashout": value.get("cashout"),
                    "streamRef": value.get("streamRef"),
                    "tvChannel": value.get("tvChannel"),
                    "start": value.get("start"),
                })

            elif key.startswith("m"):
                gm_key = str(
                    value.get("marketStyleTemplateId")
                    or f"{value.get('markettypeId')}_{value.get('periodId')}"
                )

                grouped.setdefault(gm_key, {
                    "description": f"{value.get('desc')} - {value.get('period')}",
                    "markets": {}
                })

                market = {
                    "id": key[1:],
                    "description": value.get("desc"),
                    "pos": value.get("pos"),
                    "period": value.get("period"),
                    "style": value.get("style"),
                    "cashout": value.get("cashout"),
                    "conditionalBetEnabled": value.get("conditionalBetEnabled"),
                    "outcomes": [],
                }
                
                grouped[gm_key]["markets"][key] = market
                market_index[key] = market

            elif key.startswith("o"):
                market = market_index.get(value.get("parent"))
                if market:
                    market["outcomes"].append({
                        "id": key[1:],
                        "description": value.get("desc"),
                        "pos": value.get("pos"),
                        "price": value.get("price"),
                        "conditionalBetEnabled": value.get("conditionalBetEnabled"),
                    })
    
        normalized_data["groupedMarkets"] = [
            {
                "description": gm["description"],
                "markets": list(gm["markets"].values())
            }
            for gm in grouped.values()
            if gm["markets"]
        ]

        return normalized_data


workers = 30
http_semaphore = asyncio.Semaphore(10)
os.makedirs("output_files", exist_ok=True)


base_url = "https://www.enligne.parionssport.fdj.fr"
match_api = "https://www.enligne.parionssport.fdj.fr/lvs-api/ff"
enrichment_api_ids = {
    "paris-football": "p240", 
    "paris-football/coupes-d-europe": "p58528549", 
    "paris-football/angleterre": "p58532399", 
    "paris-football/france": "p58531496", 
    "paris-football/allemagne": "p58529610", 
    "paris-football/italie": "p58529752", 
    "paris-football/espagne": "p58532113", 

    "paris-tennis": "p239", 
    "paris-basketball": "p227", 
    "paris-baseball": "p226",
    "paris-boxe": "p238",
    "paris-cyclisme": "p2700",
    "paris-golf": "p237",
    "paris-handball": "p1100",
    "paris-hockey-sur-glace": "p2100",
    "paris-rugby": "p22877",
    "paris-rugby-a-xiii": "p22878",
    "paris-ufc-mma": "p1201",
    "paris-snooker": "p22884",
}

base_api_cookies = {
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
    'kameleoonVisitorCode': 'yxg7b71xopgfyf25',
    '_gcl_au': '1.1.643701667.1770715913',
    '_fbp': 'fb.1.1770715913540.611750014570864368',
    'ry_ry-p4r1p53l_realytics': 'eyJpZCI6InJ5XzE3NDE0OTU5LUMxMDEtNEU4Ni1BNThELUI1N0QyMEM5QzREQSIsImNpZCI6bnVsbCwiZXhwIjoxODAyMjUxOTEzODc4LCJjcyI6MX0%3D',
    '_pcus': 'eyJ1c2VyU2VnbWVudHMiOm51bGwsIl90IjoibjE0dGd3emh8bWxnZWpmbmgifQ%3D%3D',
    'etuix': 'xYEiposi7LgLG2xLpvDaBhjiwK51SX2.2b.1KpFMCG.sEg7NStkgAA--',
    '_clck': '1phz8qh%5E2%5Eg3h%5E0%5E2232',
    'PIM-SESSION-ID': 'IC1DXeCCcyPRvtDD',
    'loop_num': 'd7841cca-3179-4fa7-86e4-71cd4e2a8f98%3AMFbkG%7CDR%7C%2F',
    'ry_ry-p4r1p53l_so_realytics': 'eyJpZCI6InJ5XzE3NDE0OTU5LUMxMDEtNEU4Ni1BNThELUI1N0QyMEM5QzREQSIsImNpZCI6bnVsbCwib3JpZ2luIjp0cnVlLCJyZWYiOm51bGwsImNvbnQiOm51bGwsIm5zIjp0cnVlLCJzYyI6Im9rIiwic3AiOm51bGx9',
    '_uetsid': '55c66030066311f1a11a09678120072b|imnejb|2|g3h|0|2232',
    'et0': 'UcrQzU_FlWmPBo1H6YDcHjYvxDP.Gv9gik9u1Q48GkvzP6b45GcGvcJe.Dgxwh.sg6lnwZSL8uyuFWO_38SR3pX1Hm6u_8sgNoWYTA__CQzNJthVSMvScffkdbvzU5GHg145OEHwfq8GHQJLbiYs',
    '_sp_srt_ses.6807': '*',
    '_sp_srt_id.6807': '5a21dc35-838c-4664-abe4-89d9658945a1.1770715914.3.1770818708.1770779811.1727394e-df1a-4a1e-916d-e0c5b0d629ef.21c4290f-0239-40c2-844f-d6e5ba3f9e6e...0',
    'datadome': '7KME3aXuO_hMNw4tILwCGkmbAJEJ5KLf1MbipWK79VztEpD6mcBvl9Enop4bK_nLOnfFub_71p7IborPNwlEsBDOqgWgnRpJRiinqTHo83MILt3_K0rsCPhEFQLydko7',
    'abp-pselw': '1602814730.41733.0000',
    '_clsk': 'jhv0ah%5E1770818750790%5E3%5E1%5Ea.clarity.ms%2Fcollect',
    '_uetvid': '55c680e0066311f18c0df5588ed7043c|l42upd|1770818750811|3|1|bat.bing.com/p/insights/c/a',
    '_MFB_': 'fHw4fHx8W118fHwzNC43MjY5NTM4MDYxODEwNnw=',
}

base_api_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'if-none-match': 'W/"47615-uDUXWUaOU3rkifX3mDLPN+kebqQ"',
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
}

enrich_api_headers = {
    'x-lvs-hstoken': 'lafI5HDRbsAdVL02-Siiu2-EQhP_ga_ee25eUUupYqHV-CFi7dZJxoGvE3BgEb-5Dfe5jMiswbdpbG8Li-SuvaNV-ZG26LzuqAN4XWtcc9oFy6BxtDDqxY1Q_P6m786J3vgiX9W76CqXOBjWEv4m_g==',
}

enrich_api_params = {
    'lineId': '1',
    'originId': '3',
    'breakdownEventsIntoDays': 'true',
    'showPromotions': 'false',
    'pageIndex': '0',
}

match_api_cookies = {
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
    'kameleoonVisitorCode': 'yxg7b71xopgfyf25',
    '_gcl_au': '1.1.643701667.1770715913',
    '_fbp': 'fb.1.1770715913540.611750014570864368',
    'ry_ry-p4r1p53l_realytics': 'eyJpZCI6InJ5XzE3NDE0OTU5LUMxMDEtNEU4Ni1BNThELUI1N0QyMEM5QzREQSIsImNpZCI6bnVsbCwiZXhwIjoxODAyMjUxOTEzODc4LCJjcyI6MX0%3D',
    '_pcus': 'eyJ1c2VyU2VnbWVudHMiOm51bGwsIl90IjoibjE0dGd3emh8bWxnZWpmbmgifQ%3D%3D',
    'PIM-SESSION-ID': 'a8QLUd4CGUCt4X9s',
    'loop_num': 'd7841cca-3179-4fa7-86e4-71cd4e2a8f98%3AMHooS%7CDR%7C%2F',
    'ry_ry-p4r1p53l_so_realytics': 'eyJpZCI6InJ5XzE3NDE0OTU5LUMxMDEtNEU4Ni1BNThELUI1N0QyMEM5QzREQSIsImNpZCI6bnVsbCwib3JpZ2luIjp0cnVlLCJyZWYiOm51bGwsImNvbnQiOm51bGwsIm5zIjp0cnVlLCJzYyI6Im9rIiwic3AiOm51bGx9',
    'abp-pselw': '1552483082.41733.0000',
    'etuix': 'd4UjeOk10288R_0pUfAnYsOzpaifOBl9raULqmsTqcX3IoCn8bAkGA--',
    '_clck': 'ba5k11%5E2%5Eg3o%5E1%5E2232',
    '_sp_srt_ses.6807': '*',
    'et0': 'EE7qljC6289VuL3VxTlq3NRqvH0z52pn3y0ozaEaiCuOhV59eK_CEr545bn7zeo31jAUK8QXY4l7xstEsoz_lBDLu5KMoxscva7jrFkHPUoINg52iGXIj6CJUnWhJnKWNwOTdZHf4Sx8NWb0JAMk4z50QXe2Zv1yJpymCBu7wkMic21f00vbMt7GK8IK1.rtnTSMqHYkLRzid0lbKHQVdelqwGsi2mFFWPdZewmBR3WFI0HPuJeo1KJ8A0l9E7ikvqSoxBhR3YCVJfWCZioitqXCtEkSyogLKi74c8s8cAjvcESCvgY8SaqSnyVEySy_TN3kONwDHf389WSmkSnI21o-',
    '_uetsid': 'f9683fe00c9311f19b2f81b60a52a1d7|gnehad|2|g3o|0|2240',
    'datadome': '5QwX4XCM_LEn0nT~9CIqMbTh~3GBiHp5CYaJWvtQ~l3Fqe23Orgjm1AMfRgvuQb6p~0SjbJ0txwyvu4NCFnPFcsT5iqkUtf7CjrIIlqLtO7D0mK~jew5clGyAxqLc3K1',
    '_sp_srt_id.6807': '5a21dc35-838c-4664-abe4-89d9658945a1.1770715914.28.1771396950.1771229741.d9869a6d-33f7-4d79-87f0-a34b9e33bb00.d0bad5da-a522-4cf8-ac02-25f54ec57442.4e6c834d-9029-4db6-94b3-5486f5c4023c.1771396949788.1',
    '_MFB_': 'fHwyM3x8fFtdfHx8NTkuNjkyMDQxMDMyMTMyNzM1fA==',
    '_uetvid': '55c680e0066311f18c0df5588ed7043c|fc1b2|1771396950379|10|1|bat.bing.com/p/insights/c/n',
    '_clsk': 'hl7vn5%5E1771396950381%5E10%5E1%5En.clarity.ms%2Fcollect',
}

match_api_headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'if-none-match': 'W/"6b5a-1PxucVoIi7OOv+5sbBVOxhvL4ao"',
    'priority': 'u=1, i',
    'referer': 'https://www.enligne.parionssport.fdj.fr/paris-baseball/mlb/mlb/3300735/mlb-2026',
    'sec-ch-device-memory': '8',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-full-version-list': '"Not:A-Brand";v="99.0.0.0", "Google Chrome";v="145.0.7632.76", "Chromium";v="145.0.7632.76"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'x-lvs-hstoken': 'liaiyHsoj49qY-j5DtWv4TL9PQB9yusqmgJQHGii0tSNr9YZAu_zPkpRN0q3LIAT0i4fuRieqLoMK-EgwJ77UTGsupvkAVo_s6XgE-R6GohWySHbGHDiZGOEAq56UYaICwa0hLhZSww-AAJd658mig==',
}

match_api_params = {
    'lineId': '1',
    'originId': '3',
    'ext': '1',
    'showPromotions': 'false',
    'showMarketTypeGroups': 'true',
}

sports = {
    # Football entire page
    "Football (All)": "paris-football",
    # The six european leagues
    "Football (All Europe)": "paris-football/coupes-d-europe",
    "Football (All England)": "paris-football/angleterre",
    "Football (All French)": "paris-football/france",
    "Football (All Germany)": "paris-football/allemagne",
    "Football (All Italy)": "paris-football/italie",
    "Football (All Spain)": "paris-football/espagne",
    
    # Other sports
    "Tennis (All)": "paris-tennis",
    "Basketball (All)": "paris-basketball",
    "Baseball (All)": "paris-baseball",
    "Boxing (All)": "paris-boxe",
    "Cycling (All)": "paris-cyclisme",
    "Golf (All)": "paris-golf",
    "Handball (All)": "paris-handball",
    "Ice hockey (All)": "paris-hockey-sur-glace",
    "Rugby (All)": "paris-rugby",
    "Rugby League (All)": "paris-rugby-a-xiii",
    "Snooker (All)": "paris-snooker",
    "UFC-MMA (All)": "paris-ufc-mma",
}


# Helper functions.
# ----

# Logger helper for debugging only
def setup_logging():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = log_dir / f"scraper_{timestamp}.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")

    file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return log_file

# For fetching data from endpoints (URLs)
async def fetch(url: str, match_num: int, session: AsyncSession, retrieve_json: bool, headers: dict, cookies: dict = None, params: dict = None) -> str:
    retries = 5

    for attempt in range(1, retries + 1):
        try:
            async with http_semaphore:
                # random_port = ports[random.randrange(0, 7)]
                # proxies = {"https": f"https://user-{username}-country-{country}:{password}@dc.oxylabs.io:{random_port}"} if random_port != None else None
                # response = await session.get(url, params=params, headers=headers, cookies=cookies, proxies=proxies, timeout=15)
                response = await session.get(url, params=params, headers=headers, cookies=cookies, timeout=15)

            if response.status_code == 429:
                raise RuntimeError("429 proxy rate limiting")

            if response.status_code == 403:
                raise RuntimeError("403 blocked")

            if response.status_code != 200:
                raise RuntimeError(f"Non-200 response: {response.status_code}")

            if retrieve_json: 
                return response.json()
            else:
                return response.text
        
        except KeyboardInterrupt:
            raise

        except Exception as e:
            logging.warning("Fetch failed: %s (%s) | match=%d | attempt=%d/%d", type(e).__name__, e, match_num, attempt, retries,)

            if attempt == retries:
                raise RuntimeError(f"Fetch failed for {url}") from e

            if "429" in str(e):
                rest_time = random.uniform(7, 12)
            elif "403" in str(e):
                rest_time = random.uniform(5, 15)
            else:
                rest_time = random.uniform(2, 5)
            
            logging.info("Retrying after resting %.2fs", rest_time)
            await asyncio.sleep(rest_time)

# For saving json files
async def save_json_file(file_path: str, data: dict) -> None:
    async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(data, indent=2, ensure_ascii=False))
    logging.info("%s saved successfully!", file_path)

# For saving HTML files (Only for testing and debugging)
async def save_html_file(file_path: str, html: str) -> None:
    async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
        await f.write(html)
    logging.info("%s saved successfully!", file_path)

# To flatten the json into rows for saving in csv or database
def flatten_matches(matches: list[dict]) -> list[dict]:
    rows = []

    for match in matches:
        path = match.get("path", {})
        match_base = {
            "match_id": match.get("id"),
            "opponent_a": match.get("opponent_a"),
            "opponent_b": match.get("opponent_b"),
            "desc_display": match.get("desc_display"),
            "sport": path.get("Sport"),
            "category": path.get("Category"),
            "league": path.get("League"),
            "sport_code": match.get("sport_code"),
            "match_conditional_bet_enabled": match.get("conditional_bet_enabled"),
            "match_cashout": match.get("cashout"),
            "stream_ref": match.get("stream_ref"),
            "tv_channel": match.get("tv_channel"),
            "start": match.get("start"),
        }

        for grouped in match.get("grouped_markets", []) or []:
            grouped_base = {
                "grouped_market_name": grouped.get("market_name"),
            }

            for market in grouped.get("markets", []) or []:
                market_base = {
                    "market_id": market.get("id"),
                    "column_name": market.get("column_name"),
                    "market_position": market.get("pos"),
                    "period": market.get("period"),
                    "style": market.get("style"),
                    "market_cashout": market.get("cashout"),
                    "market_conditional_bet_enabled": market.get("conditional_bet_enabled"),
                }

                for sel in market.get("selections", []) or []:
                    row = {}
                    row.update(match_base)
                    row.update(grouped_base)
                    row.update(market_base)
                    row.update({
                        "selection_id": sel.get("id"),
                        "selection_name": sel.get("name"),
                        "selection_position": sel.get("pos"),
                        "selection_odd": sel.get("odd"),
                        "selection_conditional_bet_enabled": sel.get("conditional_bet_enabled"),
                    })
                    rows.append(row)

    return rows

# For saving csv files in a different thread to avoid blocking main thread
def _write_csv(file_path: str, matches: list[dict]) -> None:
    # data_frame = pandas.json_normalize(matches, sep=" - ")
    rows = flatten_matches(matches)
    data_frame = pandas.DataFrame(rows)
    data_frame.to_csv(file_path, index=False, encoding="utf-8")
    # with open(file_path, "w", encoding="utf-8", newline="") as f:
        # data_frame.to_csv(f, index=False)

# To initialize csv file write func
async def save_csv_file(file_path: str, data_list: list[dict]) -> None:
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _write_csv, file_path, data_list)
    logging.info("%s saved successfully!", file_path)

# Important functions for request based API scraping.
# ----

# To normalize French chars of url slugs 
def slugify_abbr(value: str) -> str:
    if not value:
        return ""

    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")

    value = value.lower()
    value = value.replace(" ", "-")
    value = value.replace("&", "-")
    value = value.replace("/", "-")
    value = value.replace("(", "")
    value = value.replace(")", "")
    value = value.replace(".", "-")
    value = value.replace("'", "-")

    value = re.sub(r"-+", "-", value)

    return value.strip("-")

# To construct urls from an enrichment api
async def construct_urls_and_get_match_ids(sport_name: str, sport_path: str, session: AsyncSession) -> tuple[list[str], list[str]] | tuple[None, None]:
    api_id = enrichment_api_ids.get(sport_path)
    if not api_id:
        logging.critical("Enrichment API id is not defined! | sport_path=%s", sport_path)
        return None, None
    
    api_endpoint = f"https://www.enligne.parionssport.fdj.fr/lvs-api/next/50/{api_id}"
    for attempt in range(5):
        async with http_semaphore:
            # random_port = ports[random.randrange(0, 7)]
            # proxies = {"https": f"https://user-{username}-country-{country}:{password}@dc.oxylabs.io:{random_port}"} if random_port != None else None
            # r = await session.get(api_endpoint, params=enrich_api_params, headers=enrich_api_headers, proxies=proxies, timeout=15)
            r = await session.get(api_endpoint, params=enrich_api_params, headers=enrich_api_headers, timeout=15)

        if r.status_code == 200:
            break

        rest_time = random.uniform(2, 5)
        logging.warning("Fetch failed: %d | sport=%s | attempt=%d/5", r.status_code, sport_name, attempt + 1)
        logging.info("Retrying after resting %.2fs", rest_time)
        await asyncio.sleep(rest_time)

    else:
        return None, None

    enriched_json = r.json()
    items = enriched_json.get("items", {})
    urls = []
    match_ids = []

    for key, event in items.items():
        if not key.startswith("e"):
            continue
        
        match_ids.append(key)
        event_id = key[1:]

        path = event.get("path", {})
        league = path.get("League")
        category = path.get("Category")
        desc = event.get("desc")

        if not league or not category or not desc:
            continue

        league_slug = slugify_abbr(league)
        category_slug = slugify_abbr(category)
        desc_slug = slugify_abbr(desc)
        sport_slug = sport_path.split("/")[0]

        relative_path = f"{sport_slug}/{category_slug}/{league_slug}/{event_id}/{desc_slug}"
        url = parse.urljoin(base_url, relative_path)
        urls.append(url)

    return match_ids, urls

# To get urls of all matches and json of listing page from raw html listing endpoint
async def get_urls_of_all_matches(sport_name: str, html: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    script_tag = soup.find("script", {"id": "sport-main-jsonLd"})

    if script_tag:
        try:
            json_data = json.loads(script_tag.get_text())
            if json_data:
                return [match["url"] for match in json_data if isinstance(match, dict) and match.get("url")]
        except json.JSONDecodeError:
            pass

    logging.info("Json not found, extracting URLs from html | sport=%s", sport_name)
    anchors = soup.select("a.psel-event__link")
    if anchors:
        urls = [parse.urljoin(base_url, a["href"]) for a in anchors if a.get("href")]
    
    return urls or []

# To process single sport
async def process_sport(sport_name: str, sport_url: str, sport_path: str, session: AsyncSession, sports_data: dict) -> None:
    attempt = 0
    while True:
        attempt += 1
        try: 
            logging.info("Scraping urls of %s | attempt=%d", sport_name, attempt)
            
            match_ids, urls = await construct_urls_and_get_match_ids(sport_name, sport_path, session)

            if match_ids and urls == None:
                logging.info("Enrichment API failed, falling back to json extraction | sport=%s | attempt=%d", sport_name, attempt)

                listing_html = await fetch(sport_url, 0, session, False, base_api_headers, base_api_cookies)
                urls = await get_urls_of_all_matches(sport_name, listing_html)
                match_ids = None
                
                # [print(url) for url in urls]

                if not urls:
                    # await save_html_file(f"{sport_name} (Test).html", listing_html)
                    # return
                    logging.info("Html parsing failed too | sport=%s | attempt=%d", sport_name, attempt)
                    raise RuntimeError("No urls found, retrying...")
            
            sports_data[sport_name] = (match_ids, urls)

            logging.info("Got %d urls | sport=%s", len(urls), sport_name)
            break

        except KeyboardInterrupt:
            logging.info("\nStopping scraper...")
            return

        except Exception:
            logging.exception("\nUnknown error | sport=%s | attempt=%d", sport_name, attempt)
            logging.info("Retrying...\n")
            await asyncio.sleep(5)

# Returns raw json of a match page 
def get_json_of_a_match(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    script_tag = soup.find("script", {"id": "serverApp-state"})
    json_data = json.loads(script_tag.string) if script_tag and script_tag.string else {}

    events = json_data.get("EventsDetail", {}).get("events", [])
    return events[0] if events else {}

# To process single match
async def process_match(sport_name: str, match_num: int, total_matches: int, match_id: str | None, match_url: str, session: AsyncSession) -> Match:
    retries = 5

    for attempt in range(1, retries + 1):
        try:
            logging.info("Processing | match=%d/%d | attempt=%d | sport=%s", match_num, total_matches, attempt, sport_name)

            if match_id:
                endpoint = f"{match_api}/{match_id}"
                raw_match_json = await fetch(endpoint, match_num, session, True, match_api_headers, match_api_cookies, match_api_params)

                if not raw_match_json:
                    logging.warning("Match API retuned empty json, failing back to page API")
                    break

                return Match(**raw_match_json).model_dump()

            
            match_page = await fetch(match_url, match_num, session, False, base_api_headers, base_api_cookies)
            raw_match_json = get_json_of_a_match(match_page)

            if not raw_match_json:
                raise RuntimeError("Page API also failed, Empty match JSON")

            # file_path = Path("Test files") / f"{sport_name} - Match {match_num}.json"
            # await save_json_file(file_path, raw_match_json)
            return Match(**raw_match_json).model_dump()

        except KeyboardInterrupt:
            raise

        except Exception as e:
            logging.warning("Match failed | match=%d | attempt=%d/%d | sport=%s | error=%s", match_num, attempt, retries, sport_name, e)

            if attempt == retries:
                logging.error("Giving up | match=%d | sport=%s", match_num, sport_name,)
                return

            await asyncio.sleep(2)

# To manage worker's life cycle
async def match_worker(name: int, sport_name: str, total_matches: int, queue: asyncio.Queue, session: AsyncSession, results: list):
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return

        match_num, match_id, match_url = item
        try:
            match = await process_match(sport_name, match_num, total_matches, match_id, match_url, session)
            if match:
                results[match_num - 1] = match
        finally:
            queue.task_done()

# Manages queue to fetch match concurrently
async def get_json_of_matches(sport_name: str, ids: list[str] | None, urls: list[str], session: AsyncSession) -> list[Match | None]:
    queue: asyncio.Queue = asyncio.Queue()
    total_matches = len(urls)
    results: list[Match | None] = [None] * total_matches

    workers_tasks = [
        asyncio.create_task(match_worker(i + 1, sport_name, total_matches, queue, session, results))
        for i in range(workers)
    ]

    for match_num, match_url in enumerate(urls, start=1):
        match_id = ids[match_num -1] if ids else None
        await queue.put((match_num, match_id, match_url))

    await queue.join()

    for _ in workers_tasks:
        await queue.put(None)

    await asyncio.gather(*workers_tasks)
    return results

# Core scraping logic
async def main():
    log_file = setup_logging()
    logging.info("Scraper started")

    if log_file:
        logging.info("Logging to %s", log_file)

    async with AsyncSession(impersonate="chrome") as session:
        sports_data = {}
        tasks = []      

        for sport_name, sport_path in sports.items():
            sport_url = parse.urljoin(base_url, sport_path)

            tasks.append(asyncio.create_task(
                process_sport(sport_name, sport_url, sport_path, session, sports_data)
            ))

        await asyncio.gather(*tasks)

    for sport_name, (ids, urls) in sports_data.items():
        sport_data = {}
        async with AsyncSession(impersonate="chrome") as session:
            logging.info("Extracting markets | sport=%s | total matches=%d", sport_name, len(urls))
            results = await get_json_of_matches(sport_name, ids, urls, session)
            
            sport_data["sport_name"] = sport_name
            sport_data["matches"] = [m for m in results if m is not None]

            file_path = Path("output_files") / f"{sport_name}.csv"
            # json_file_path = Path("output_files") / f"{sport_name}.json"
            # await save_json_file(json_file_path, sport_data)
            await save_csv_file(file_path, sport_data["matches"])

if __name__ == "__main__":
    asyncio.run(main())