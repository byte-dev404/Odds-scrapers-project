import re
import os
import json
import random
import asyncio
import aiofiles
import logging
import unicodedata
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib import parse
from datetime import datetime
from curl_cffi import AsyncSession
from pydantic import BaseModel, Field


# Pydantic data models
class Selection(BaseModel):
    id: int | None = Field(default=None)
    name: str | None = Field(default=None, alias="description")
    pos: int | None = Field(default=None)
    odd: str | float | int | None = Field(default=None, alias="price")
    conditional_bet_enabled: bool | None = Field(default=None, alias="conditionalBetEnabled")
    up: bool | None = Field(default=None)
    down: bool | None = Field(default=None)
    hidden: bool | None = Field(default=None)
    suspended: bool | None = Field(default=None)
    selected: bool | None = Field(default=None)
    isMarketCashout: bool | None = Field(default=None)
    isMulti: bool | None = Field(default=None)

class Market(BaseModel):
    id: int | None = Field(default=None)
    column_name: str | None = Field(default=None, alias="description")
    pos: int | None = Field(default=None)
    period: str | None = Field(default=None)
    style: str | None = Field(default=None)
    cashout: bool | None = Field(default=None)
    conditional_bet_enabled: bool | None = Field(default=None, alias="conditionalBetEnabled")
    suspended: bool | None = Field(default=None)
    selections: list[Selection] | None = Field(default_factory=list, alias="outcomes")

class GroupedMarket(BaseModel):
    id: int | None = Field(default=None)
    market_name: str | None = Field(default=None, alias="description")
    cashout: bool | None = Field(default=None)
    template: str | None = Field(default=None)
    layout_properties: dict | None = Field(default_factory=dict, alias="properties")
    markets: list[Market] | None = Field(default_factory=list)

class Match(BaseModel):
    id: int | None = Field(default=None)
    opponent_a: dict = Field(default_factory=dict, alias="opponentA")
    opponent_b: dict = Field(default_factory=dict, alias="opponentB")
    description: str | None = Field(default=None)
    desc_display: str | None = Field(default=None, alias="descDisplay")
    path: dict | None = Field(default_factory=dict)
    sport_code: str | None = Field(default=None, alias="sportCode")
    conditional_bet_enabled: bool | None = Field(default=None, alias="conditionalBetEnabled")
    cashout: bool | None = Field(default=None, alias="cashout")
    combi_boost: bool | None = Field(default=None, alias="combiBoost")
    stream_ref: str | None = Field(default=None, alias="streamRef")
    tv_channel: str | None = Field(default=None, alias="tvChannel")
    start: str | None = Field(default=None)
    parsed_start: str | None = Field(default=None, alias="parsedStart")
    event_kind: str | None = Field(default=None, alias="eventKind")
    grouped_markets: list[GroupedMarket] = Field(default_factory=list, alias="groupedMarkets")

class Sport(BaseModel):
    sport_name: str
    matches: list[Match] = Field(default_factory=list)

workers = 15
http_semaphore = asyncio.Semaphore(5)
os.makedirs("output_files", exist_ok=True)

load_dotenv()
proxy_endpoint = os.getenv("thordata_rotating_residential_proxy_endpoint")

if not proxy_endpoint:
    raise RuntimeError("Proxy environment variable is missing.")

proxies={
    "http": proxy_endpoint,
    "https": proxy_endpoint,
}

base_url = "https://www.enligne.parionssport.fdj.fr"
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
    "paris-ufc-mma": "p1201",
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
    'showPromotions': 'true',
    'pageIndex': '0',
}

sports = {
    # Football entire page
    "Football (ALL)": "paris-football",
    # The six european leagues
    "Football (All Europe)": "paris-football/coupes-d-europe",
    "Football (All England)": "paris-football/angleterre",
    "Football (All French)": "paris-football/france",
    "Football (All Germany)": "paris-football/allemagne",
    "Football (All Italy)": "paris-football/italie",
    "Football (All Spain)": "paris-football/espagne",
    
    # Other sports
    "Tennis (ALL)": "paris-tennis",
    "Basketball (ALL)": "paris-basketball",
    "Baseball (ALL)": "paris-baseball",
    "Boxing (ALL)": "paris-boxe",
    "Cycling (ALL)": "paris-cyclisme",
    "Golf (ALL)": "paris-golf",
    "Handball (ALL)": "paris-handball",
    "Ice hockey (ALL)": "paris-hockey-sur-glace",
    "Rugby (ALL)": "paris-rugby",
    "UFC-MMA (ALL)": "paris-ufc-mma",
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
async def fetch(url: str, match_num, session: AsyncSession) -> str:
    retries = 5

    for attempt in range(1, retries + 1):
        try:
            async with http_semaphore:
                response = await session.get(url, headers=base_api_headers, proxies=proxies, timeout=15)

            if response.status_code == 429:
                raise RuntimeError("429 proxy rate limiting")

            if response.status_code == 403:
                raise RuntimeError("403 blocked")

            if response.status_code != 200:
                raise RuntimeError(f"Non-200 response: {response.status_code}")

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

# For saving csv files (Added later)
async def save_csv_file(file_path: str, json: dict) -> None:
    pass


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
async def construct_urls(sport_path: str, session: AsyncSession) -> list[str]:
    api_id = enrichment_api_ids.get(sport_path)
    if not api_id:
        return []
    
    api_endpoint = f"https://www.enligne.parionssport.fdj.fr/lvs-api/next/50/{api_id}"
    for attempt in range(5):
        async with http_semaphore:
            r = await session.get(api_endpoint, params=enrich_api_params, headers=enrich_api_headers, proxies=proxies, timeout=15)

        if r.status_code == 200:
            break

        rest_time = random.uniform(2, 5)
        logging.info("Retrying after resting %.2fs", rest_time)
        await asyncio.sleep(rest_time)

    else:
        raise RuntimeError("Enrichment API failed after retries")

    enriched_json = r.json()
    items = enriched_json.get("items", {})
    urls = []

    for key, event in items.items():
        if not key.startswith("e"):
            continue
        
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

    return urls

# To get urls of all matches and json of listing page from raw html listing endpoint
async def get_urls_of_all_matches(sport_path: str, html: str, session: AsyncSession) -> list:
    soup = BeautifulSoup(html, "html.parser")
    script_tag = soup.find("script", {"id": "sport-main-jsonLd"})

    if script_tag:
        try:
            json_data = json.loads(script_tag.get_text())
            if json_data:
                return [match["url"] for match in json_data if isinstance(match, dict) and match.get("url")]
        except json.JSONDecodeError:
            pass

    logging.info("Json not found, extracting URLs from html | sport=%s", sport_path)
    anchors = soup.select("a.psel-event__link")
    if anchors:
        urls = [parse.urljoin(base_url, a["href"]) for a in anchors if a.get("href")]
    else:
        logging.info("Falling back to enrichment URLs | sport=%s", sport_path)
        urls = await construct_urls(sport_path, session)

    return urls or []

# Returns raw json of a match page 
def get_json_of_a_match(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    script_tag = soup.find("script", {"id": "serverApp-state"})
    json_data = json.loads(script_tag.string) if script_tag and script_tag.string else {}

    events = json_data.get("EventsDetail", {}).get("events", [])
    return events[0] if events else {}

# To process single match
async def process_match(sport_name: str, match_num: int, total_matches: int, match_url: str, session: AsyncSession) -> Match:
    retries = 5

    for attempt in range(1, retries + 1):
        try:
            logging.info("Processing | match=%d/%d | attempt=%d | sport=%s", match_num, total_matches, attempt, sport_name)

            match_page = await fetch(match_url, match_num, session)
            raw_match_json = get_json_of_a_match(match_page)

            if not raw_match_json:
                raise RuntimeError("Empty match JSON")

            # file_path = Path("Test files") / f"{sport_name} - Match {match_num}.json"
            # await save_json_file(file_path, raw_match_json)
            return Match(**raw_match_json)

        except KeyboardInterrupt:
            raise

        except Exception as e:
            logging.warning("Match failed | match=%d | attempt=%d/%d | sport=%s | error=%s", match_num, attempt, retries, sport_name, e)

            if attempt == retries:
                logging.error("Giving up | match=%d | sport=%s", match_num, sport_name,)
                return

            await asyncio.sleep(2)

# To manage worker's life cycle
async def worker(name: int, sport_name: str, total_matches: int, queue: asyncio.Queue, session: AsyncSession, results: list):
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return

        match_num, match_url = item
        try:
            match = await process_match(sport_name, match_num, total_matches, match_url, session)
            if match:
                results[match_num - 1] = match
        finally:
            queue.task_done()

# Manages queue to fetch match concurrently
async def get_json_of_matches(sport_name: str, urls: list[str], session: AsyncSession) -> list[Match | None]:
    queue: asyncio.Queue = asyncio.Queue()
    total_matches = len(urls)
    results: list[Match | None] = [None] * total_matches

    workers_tasks = [
        asyncio.create_task(worker(i + 1, sport_name, total_matches, queue, session, results))
        for i in range(workers)
    ]

    for match_num, match_url in enumerate(urls, start=1):
        await queue.put((match_num, match_url))

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

    sports_data = {}

    async with AsyncSession(impersonate="chrome", cookies=base_api_cookies) as session:
        for sport_name, sport_path in sports.items():
            sport_url = parse.urljoin(base_url, sport_path)
            attempt = 0

            while True:
                attempt += 1
                try: 
                    logging.info("Scraping urls of %s | attempt=%d", sport_name, attempt)
                    
                    listing_html = await fetch(sport_url, 0, session)
                    urls = await get_urls_of_all_matches(sport_path, listing_html, session)
                    # [print(url) for url in urls]

                    if not urls:
                        # await save_html_file(f"{sport_name} (Test).html", listing_html)
                        # return
                        raise RuntimeError("No urls found")
                    
                    sports_data[sport_name] = urls

                    logging.info("\n")
                    break

                except KeyboardInterrupt:
                    logging.info("\nStopping scraper...")
                    return

                except Exception:
                    logging.exception("\nUnknown error | sport=%s | attempt=%d", sport_name, attempt)
                    logging.info("Retrying...\n")
                    await asyncio.sleep(5)


    for sport_name, urls in sports_data.items():
        sport_data = {}
        async with AsyncSession(impersonate="chrome", cookies=base_api_cookies) as session:
            logging.info("Extracting markets | sport=%s | total matches=%d", sport_name, len(urls))
            results = await get_json_of_matches(sport_name, urls, session)
            
            sport_data["sport_name"] = sport_name
            sport_data["matches"] = [m for m in results if m is not None]

            file_path = Path("Test files") / f"{sport_name}.json"
            await save_json_file(file_path, Sport(**sport_data).model_dump())

if __name__ == "__main__":
    asyncio.run(main())