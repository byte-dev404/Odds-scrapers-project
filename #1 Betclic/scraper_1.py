import json
import random
import asyncio
import aiofiles
import traceback
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib import parse
from datetime import datetime
from bs4 import BeautifulSoup
from curl_cffi import requests, AsyncSession
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from playwright.async_api import async_playwright, Page, BrowserContext, Browser
from playwright.async_api import Error as PlaywrightError


base_url = "https://www.betclic.fr"

sports = {
    # Entire page
    "Football (The whole offer)": "football-sfootball",

    # Europe Leagues
    "Football (Conference League)": "football-sfootball/ligue-conference-c28946",
    "Football (Champions League)": "football-sfootball/ligue-des-champions-c8",

    # England Leagues 
    "Football (England. Championship)": "football-sfootball/angl-championship-c28",
    "Football (England. Premier League)": "football-sfootball/angl-premier-league-c3",
    "Football (England EFL Cup)": "football-sfootball/angleterre-efl-cup-c41",
    "Football (England FA Cup)": "football-sfootball/angleterre-fa-cup-c44",

    # French Leagues
    "Football (Arkema Premier League)": "football-sfootball/arkema-premiere-ligue-c691",
    "Football (French Cup)": "football-sfootball/coupe-de-france-c36",
    "Football (Ligue 1 McDonald’s®)": "football-sfootball/ligue-1-mcdonald-s-c4",
    "Football (Ligue 2 BKT®)": "football-sfootball/ligue-2-bkt-c19",

    # Italy Leagues
    "Football (Italy Cup)": "football-sfootball/italie-coupe-c50",
    "Football (Italy Serie A)": "football-sfootball/italie-serie-a-c6",
    "Football (Italy Serie B)": "football-sfootball/italie-serie-b-c30",

    # Germany Leagues
    "Football (Germany Bundesliga)": "football-sfootball/allemagne-bundesliga-c5",
    "Football (Germany Bundesliga 2)": "football-sfootball/allemagne-bundesliga-2-c29",
    "Football (Germany Cup)": "football-sfootball/allemagne-coupe-c55",

    # Spain Leagues
    "Football (Spain Copa del Rey)": "football-sfootball/espagne-coupe-du-roi-c47",
    "Football (Spain LaLiga)": "football-sfootball/espagne-laliga-c7",
    "Football (Spain Liga Segunda)": "football-sfootball/espagne-liga-segunda-c31",

    # Other sports
    "Tennis": "tennis-stennis",
    "Basketball": "basketball-sbasketball",
    "Baseball": "baseball-sbaseball",
    "Boxing": "boxe-sboxing",
    "Cycling": "cyclisme-scycling",
    "Golf": "golf-sgolf",
    "Handball": "handball-shandball",
    "Ice hockey": "hockey-sur-glace-sice_hockey",
    "MMA": "mma-smartial_arts",
    "Rugby union": "rugby-a-xv-srugby_union",
}

# Chrome profile for API
cookies_for_api = {
    'bc-device-id': 'fd981878-faf4-43a3-a1ce-6b87948f3b6c',
    'BC-TOKEN': 'eyJhbGciOiJIUzI1NiIsImtpZCI6ImM0MzQ3NWY5LTVlNjMtNDFjZC1hMWNlLWJlMTM1NWZmODUwZSIsInR5cCI6IkpXVCJ9.eyJqdGkiOiI3ZWNjMzdhMS05MWY0LTQ5NjYtYjA3MS05OTNmMmJmOGJlODgiLCJyem4iOiJGUiIsImJybiI6IkJFVENMSUMiLCJwbHQiOiJERVNLVE9QIiwibG5nIjoiZnIiLCJ1bnYiOiJTUE9SVFMiLCJzaXQiOiJGUkZSIiwiZnB0IjoiZmQ5ODE4NzgtZmFmNC00M2EzLWExY2UtNmI4Nzk0OGYzYjZjIiwibmJmIjoxNzY5MTc2OTk4LCJleHAiOjE3Njk3ODE3OTgsImlzcyI6IkJFVENMSUMuRlIifQ.9D_CNmR3lIUDkpTZrun9yL2_PzBKGj3CA8GfziJG_KQ',
    'theme': 'light',
    'TCPID': '126151933355270501940',
    'TC_PRIVACY': '0%21010%7C19%7C5606%212%2C5%216%211769177040214%2C1769177040214%2C1784729040214%21',
    'TC_PRIVACY_CENTER': '2%2C5',
    '_gcl_au': '1.1.424578170.1769177040',
    '_scid': 'p5slgBlaBnUaHV7xaSG1wdqg4QMBs6Zv',
    '_ScCbts': '%5B%22243%3Bchrome.2%3A2%3A5%22%2C%22299%3Bchrome.2%3A2%3A5%22%5D',
    '_fbp': 'fb.1.1769177048871.590296656404793136',
    '_clck': 'ehtgpe%5E2%5Eg31%5E1%5E2214',
    'DATADOG_CORRELATION_ID': '2fff5e17-c8aa-4147-a0dd-d90eb25abce8',
    'BC-TIMEZONE': '{%22ianaName%22:%22Asia/Calcutta%22}',
    '_scid_r': 'oBslgBlaBnUaHV7xaSG1wdqg4QMBs6ZvJBnCHw',
    '_uetsid': 'ca11d280fa8f11f0a6493bf55528b43b',
    '_uetvid': '611a4790f86411f0852d557c2f3bf65a',
    '_clsk': 'rptgbg%5E1769418818950%5E1%5E1%5Ed.clarity.ms%2Fcollect',
    '_dd_s': 'aid=03a3d112-3286-450b-8ab7-13e58bc864a3&rum=0&expire=1769419721964&logs=1&id=a74a621b-7824-4971-8349-d4db9950c8d8&created=1769418791503',
}

headers_for_api = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'referer': 'https://www.betclic.fr/',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
}

# Edge profile for browser automation
cookies_for_playwright = {
    'theme': 'light',
    'bc-device-id': '1430c0b4-8a57-40f6-9ce1-4e47eaf35e67',
    'BC-TOKEN': 'eyJhbGciOiJIUzI1NiIsImtpZCI6ImM0MzQ3NWY5LTVlNjMtNDFjZC1hMWNlLWJlMTM1NWZmODUwZSIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJhODA5NWNhZS0xNzQyLTRkNTYtOTFkZC03YjJjMmUxODJiZTMiLCJyem4iOiJGUiIsImJybiI6IkJFVENMSUMiLCJwbHQiOiJERVNLVE9QIiwibG5nIjoiZnIiLCJ1bnYiOiJTUE9SVFMiLCJzaXQiOiJGUkZSIiwiZnB0IjoiMTQzMGMwYjQtOGE1Ny00MGY2LTljZTEtNGU0N2VhZjM1ZTY3IiwibmJmIjoxNzcwMDMzMjQ1LCJleHAiOjE3NzA2MzgwNDUsImlzcyI6IkJFVENMSUMuRlIifQ.6-6lFg6Ut__30EHvPQSAPl56ULPBCqKoHe2UmBSKDEk',
    'DATADOG_CORRELATION_ID': '87ddb5f4-f3d6-458e-8779-d27050c50a5b',
    'BC-TIMEZONE': '{%22ianaName%22:%22Asia/Calcutta%22}',
    'TCPID': '126211724269556248308',
    '_dd_s': 'aid=50acd36d-9bf8-40b5-a817-5fd3d481e58d&rum=0&expire=1770034179721&logs=1&id=4a09a95d-daa8-4a27-b99f-32402f06d7c9&created=1770033258971',
    'TC_PRIVACY': '0%21010%7C19%7C5606%212%2C5%216%211770033279726%2C1770033279726%2C1785585279726%21',
    'TC_PRIVACY_CENTER': '2%2C5',
    '_gcl_au': '1.1.234646844.1770033280',
}

headers_for_playwright = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'referer': 'https://www.betclic.fr/',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Microsoft Edge";v="144"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0',
}


# Pydantic data models
class Contestant(BaseModel):
    contestant_id: str = Field(alias="contestantId")
    full_name: str = Field(alias="name")
    short_name: str = Field(alias="shortName")
    form_results: list[int] = Field(alias="formResults", default_factory=list)
    ranking: str | None = None

class Competition_info(BaseModel):
    competition_id: str = Field(alias="id")
    competition_name: str = Field(alias="name")
    competition_sport: str = Field(alias="sport")
    country_code: str = Field(alias="countryCode")

    @field_validator("competition_sport", mode="before")
    @classmethod
    def flat_competition_sport(cls, v):
        if isinstance(v, dict):
            return v.get("name") or v.get("code").title() or ""
        return v or ""

class Main_selections(BaseModel):
    selection_id: str = Field(alias="id")
    name: str
    betslip_name: str = Field(alias="betslipName")
    odds: float
    status: int
    is_live: bool = Field(alias="isLive", default=False)
    keys: list[str] = Field(default_factory=list)
    betslipMarketId: str
    player_ids: list[str] = Field(alias="playerIds", default_factory=list)
    bet_trend: int | None = Field(alias="betTrend", default=None)

class Market_overview(BaseModel):
    market_id: str = Field(alias="id")
    name: str
    betslip_name: str = Field(alias="betslipName")
    has_boosted_odds: bool = Field(alias="hasBoostedOdds", default=False)
    is_cashoutable: bool = Field(alias="isCashoutable", default=True)
    main_selections: list[Main_selections] = Field(alias="mainSelections", default_factory=list)
    position: int = 0
    match_id: str = Field(alias="matchId")
    is_outright: bool = Field(alias="isOutright", default=False)
    is_good_deal: bool = Field(alias="isGoodDeal", default=False)
    is_early_win: bool = Field(alias="isEarlyWin", default=False)

class Match_info(BaseModel):
    round_name: str = Field(alias="roundName")
    match_leg_type: int = Field(alias="matchLegType", default=0)
    group_name: str = Field(alias="groupName", default="")

class GroupedSelection(BaseModel):
    name: str
    odds: dict[str, float]

class Selection(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    odds: float

class Market_details(BaseModel):
    model_config = ConfigDict(extra="ignore")

    market_id: str = Field(alias="id")
    market_name: str = Field(alias="name")
    position: int
    is_early_win: bool = Field(alias="isEarlyWin")
    is_cashoutable: bool = Field(alias="isCashoutable")
    match_id: str = Field(alias="matchId")
    selections: list[GroupedSelection | Selection] = Field(default_factory=list)

    @classmethod
    def from_raw(cls, raw: dict) -> "Market_details":
        selections: list[dict] = []
        selections.extend(raw.get("mainSelections", []))

        for row in raw.get("selectionMatrix", []):
            for item in row.get("selections", []):
                sel = item.get("selectionOneof", {}).get("selection")
                if sel:
                    selections.append(sel)

        # for group in raw.get("splitCardGroups", []):
        #     name = group.get("name", "")
        #     sel = group.get("selections", [])
        #     selections.extend({str(name): sel})
            # selections.append({"name": name, "odds": sel})

        for group in raw.get("splitCardGroups", []):
            odds_map: dict[str, float] = {}

            for sel in group.get("selections", []):
                sel_name = sel.get("name")
                sel_odds = sel.get("odds")

                if sel_name and isinstance(sel_odds, (int, float)):
                    odds_map[sel_name] = sel_odds

            if odds_map:
                selections.append({"name": group.get("name", ""), "odds": odds_map,})

        grouped_markets = raw.get("groupMarkets", [])
        if grouped_markets:
            table: dict[str, dict[str, float]] = {}

            for group in grouped_markets:
                group_name = group.get("name", "")

                for row in group.get("selectionMatrix", []):
                    sel = row.get("selections", [{}])[0].get("selectionOneof", {}).get("selection", {})
                    team_name = sel.get("name")
                    odds = sel.get("odds")

                    if not team_name:
                        continue

                    table.setdefault(team_name, {})[group_name] = odds

            for team, values in table.items():
                selections.append({"name": team, "odds": values})

        raw = dict(raw)
        raw["selections"] = selections

        return cls.model_validate(raw)

# Maybe this model is useless (look at this later)
class All_markets(BaseModel):
    model_config = ConfigDict(extra="ignore")
    markets: list[Market_details]

class Match(BaseModel):
    match_id: str = Field(alias="matchId")
    team_names: str = Field(alias="name")
    match_date_utc: str = Field(alias="matchDateUtc")
    is_live: bool = Field(alias="isLive", default=False)
    has_live_stream: bool = Field(alias="hasLiveStream", default=False)
    contestants: list[Contestant] = Field(alias="contestants", default_factory=list)
    is_bet_builder_eligible: bool = Field(alias="isBetbuilderEligible", default=False)
    is_match_of_the_day: bool = Field(alias="isMatchOfTheDay", default=False)
    has_match_center: bool = Field(alias="hasMatchCenter", default=False)
    has_lineup: bool = Field(alias="hasLineup", default=False)
    open_market_count: int = Field(alias="openMarketCount", default=0)
    competition: Competition_info = Field(alias="competition")
    market_overview: Market_overview | None = None
    match_info: Match_info | None = Field(alias="competitionInfo", default=None)
    streaming_provider_type: int = Field(alias="streamingProviderType", default=0)
    all_Markets: All_markets | None = None

    @model_validator(mode="before")
    @classmethod
    def fallback_competition_info(cls, data: dict):
        if not data.get("competitionInfo"):
            inner = data.get("matchInfo", {}).get("competitionInfo")
            if inner:
                data["competitionInfo"] = inner
        return data

class Sport_data(BaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    sport_name: str | None = Field(alias="name", default=None)
    matches: list[Match] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def resolve_sport_name(cls, data: dict):
        if data.get("name"):
            return data

        competition = data.get("competition")
        if isinstance(competition, dict):
            sport = competition.get("sport")
            comp_name = competition.get("name")

            if isinstance(sport, dict):
                sport_name = sport.get("name")

                if sport_name and comp_name:
                    data = dict(data)
                    data["name"] = f"{sport_name} - {comp_name}"
                    return data

        return data


# Helper classes
# ----

# To manage and rotate multiple request sessions asynchronously
class SessionManager:
    def __init__(self, max_requests=6, rest_seconds=(3, 6)):
        self.max_requests = max_requests
        self.rest_seconds = rest_seconds
        self._session: AsyncSession | None = None
        self._request_count = 0
        self._active_requests = 0
        self._lock = asyncio.Lock()

    async def get_session(self) -> AsyncSession:
        async with self._lock:
            if self._session is None or self._request_count >= self.max_requests:
                await self._rotate_session()

            self._request_count += 1
            self._active_requests += 1
            return self._session

    async def release(self):
        async with self._lock:
            self._active_requests -= 1

    async def _rotate_session(self):


        sleep_time = random.uniform(*self.rest_seconds)
        logging.info("Rotating API session, resting %.2fs", sleep_time)
        await asyncio.sleep(sleep_time)

        self._session = AsyncSession()
        self._request_count = 0
    
    async def _rotate_session(self):
        if self._session:
            # while self._active_requests > 0:
            #     await asyncio.sleep(0.1)
            await self._session.close()

        sleep_time = random.uniform(*self.rest_seconds)
        logging.info("Rotating API session, resting %.2fs", sleep_time)
        await asyncio.sleep(sleep_time)

        self._session = AsyncSession()
        self._request_count = 0
        self._active_requests = 0

    async def close(self):
        async with self._lock:
            if self._session:
                while self._active_requests > 0:
                    await asyncio.sleep(0.1)
                await self._session.close()
                self._session = None


# Helper functions.
# ----

# Logger helper for debugging only
def setup_logging():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = log_dir / f"scraper_{timestamp}.log"

    logger = logging.getLogger()

    if logger.handlers:
        return None

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")

    file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return log_file

# For fetching data from endpoints (URLs)
async def fetch(url: str, session_mgr: SessionManager) -> str:
    retries = 5

    for attempt in range(1, retries + 1):
        session = await session_mgr.get_session()

        try:
            response = await session.get(url, cookies=cookies_for_api, headers=headers_for_api, impersonate="chrome")

            if response.status_code == 403:
                raise RuntimeError("403 blocked")

            if response.status_code != 200:
                raise RuntimeError(f"Non-200 response: {response.status_code}")

            return response.text
        
        except KeyboardInterrupt:
            raise

        except Exception as e:
            logging.warning("Fetch failed: %s (%s) | attempt=%d/%d | url=%s", type(e).__name__, e, attempt, retries, url)

            if attempt == retries:
                raise RuntimeError(f"Fetch failed for {url}") from e

            if "403" in str(e):
                rest_time = 15 + random.random() * 10
            else:
                rest_time = random.uniform(8, 15)
            
            logging.info("Retrying after resting %.2fs", rest_time)
            await asyncio.sleep(rest_time)

        finally:
            await session_mgr.release()

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


# Important functions for request based API scraping.
# ----

# To get urls of all matches and json of listing page from raw html listing endpoint
def get_urls_and_json(html: str) -> tuple[list[str], dict]:
    soup = BeautifulSoup(html, "html.parser")
    card_tags = soup.select("a.cardEvent")
    card_urls = [tag['href'] for tag in card_tags]

    script_tag = soup.find("script", {"id": "ng-state"})
    json_data =  json.loads(script_tag.string) if script_tag and script_tag.string else {}

    return card_urls, json_data

# Returns raw json of the first tab (Le top/The top) from a match page 
def get_json_data(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    script_tag = soup.find("script", {"id": "ng-state"})
    return json.loads(script_tag.string) if script_tag and script_tag.string else {}


# Important functions for browser automation (Playwright) based scraping.
# ----

# Converts normal cooikes to playwright's required format
def convert_cookies(cookies_dict: dict) -> list[dict]:
    return [
        {
            "name": name,
            "value": value,
            "domain": ".betclic.fr",
            "path": "/",
            "secure": True,
        }
        for name, value in cookies_dict.items()
    ]

# To close the (Early win) modal/pop up
async def close_modal(page: Page) -> None:
    for _ in range(10):
        close_btn = page.locator("button:has-text('Fermer')")
        if await close_btn.count() > 0:
            await close_btn.first.click()
            break
        await page.wait_for_timeout(300)

# To wait until angular loads all dynamic content
async def wait_for_dom_stable(page: Page, timeout: int = 8000) -> None:
    await page.evaluate(
        """
        (maxTime) => new Promise(resolve => {
            let last = document.body.innerHTML.length;
            let sameCount = 0;

            const interval = setInterval(() => {
                const current = document.body.innerHTML.length;

                if (current === last) {
                    sameCount++;
                } else {
                    sameCount = 0;
                    last = current;
                }

                if (sameCount >= 3) {
                    clearInterval(interval);
                    resolve();
                }
            }, 200);

            setTimeout(() => {
                clearInterval(interval);
                resolve();
            }, maxTime);
        })
        """,
        timeout
    )

# For disabling overlays to avoid flaky behaviour when expanding cards
async def disable_overlays(page: Page) -> None:
    await page.evaluate("""
        () => {
            const sticky = document.querySelector('.event_header.is-sticky');
            if (sticky) sticky.style.pointerEvents = 'none';

            const backToTop = document.querySelector('bcdk-scrolltop-button');
            if (backToTop) backToTop.style.pointerEvents = 'none';
        }
    """)

# For expanding all the cards inside a market tab of a match page
async def click_all_show_more_btns(page: Page) -> None:
    try:
        await page.wait_for_selector(".btn.is-seeMore", timeout=3000)
    except:
        return # For tabs that has no expandable cards

    while True:
        btn = page.locator(".btn.is-seeMore:not(.is-expanded)").first
        if await btn.count() == 0:
            break

        try:
            await page.evaluate("""
            (btn) => {
                btn.scrollIntoView({ block: 'center', inline: 'center' });
            }
            """, await btn.element_handle())

            await page.wait_for_timeout(80)
            await btn.click()

        except PlaywrightError:
            await page.wait_for_timeout(200)
            continue

        await page.wait_for_timeout(120)

def extract_odds_from_tabs(html):
    soup = BeautifulSoup(html, 'html.parser')
    market_boxes = soup.find_all('div', class_='marketBox')
    
    for box in market_boxes:
        market_data = {}
        
        name_tag = box.find('h2', class_='marketBox_headTitle')
        if not name_tag:
            continue
        market_data["market_name"] = " ".join(name_tag.get_text(strip=True).split()).capitalize() if name_tag else ""

        selections = []

        selections = box.find_all('div', class_='marketBox_lineSelection')
        for sel in selections:
            name_el = sel.find('p', class_='marketBox_label')
            odds_el = sel.find('bcdk-bet-button-label')
            
            if name_el and odds_el:
                sel_obj = {}
                sel_obj["name"] = " ".join(name_el.get_text(strip=True).split()) if name_tag else ""
                sel_obj["odds"] = " ".join(odds_el.get_text(strip=True).split()) if odds_el else 0.00
                selections.append(sel_obj)

        market_data["selections"] = selections

        return market_data

# Saves raw html of all other market tabs of a match page (Right now only saves html)
async def get_markets_from_other_tabs(browser_context: BrowserContext, sport: str, match_num: int, match_url: str) -> None:
    excluded_tabs = {"MyCombi", "Le Top", "The Top"}
    attempt = 0

    while True:
        pages = []
        skipped_page = 0
        attempt += 1

        try:
            page = await browser_context.new_page()
            current_tab_index = 0
            tabs_count = None

            await page.goto(match_url, wait_until="domcontentloaded", timeout=60000)
            await close_modal(page)

            await page.wait_for_selector("app-desktop")
            await close_modal(page)

            await page.wait_for_timeout(500)
            await close_modal(page)

            tabs = page.locator("div.tab_item")
            tabs_count = await tabs.count()

            for i in range(tabs_count):
                current_tab = tabs.nth(i)
                text = (await current_tab.text_content() or "").strip()

                if text in excluded_tabs:
                    logging.debug("Skipped excluded tab: %s", text)
                    skipped_page += 1
                    continue

                current_tab_index = i

                for _ in range(5):
                    classes = await current_tab.get_attribute("class") or ""
                    if "isActive" in classes:
                        break

                    el = await current_tab.element_handle()

                    if not el:
                        raise RuntimeError("Tab element disappeared")
                    
                    await page.evaluate("(el) => el.click()", el)
                    await page.wait_for_selector("app-desktop", timeout=15000)

                    # await page.wait_for_function("(el) => el.classList.contains('isActive')", arg=el, timeout=6000)
                    # await page.wait_for_selector("div.marketBox_container.is-active", state="attached", timeout=5000)
                else:
                    raise RuntimeError(f"Tab {i} never became active")

                # Wait time for angular to hydrate betclic 
                await wait_for_dom_stable(page)
                await page.wait_for_timeout(300)

                await disable_overlays(page)
                await click_all_show_more_btns(page)
                logging.debug("Expanded all cards! | match=%d | tab=%d", match_num, i)

                await page.wait_for_timeout(500)
                raw_html  = await page.locator("div.marketBox_container.is-active").inner_html()

                logging.debug("Captured tab HTML | match=%d | tab=%d", match_num, i)
                pages.append(raw_html)

        except KeyboardInterrupt:
            logging.info("Interrupted by user, Exiting cleanly at (Tab level).")
            raise

        except PlaywrightError as e:
            msg = str(e)

            if "ERR_CONNECTION_CLOSED" in msg or "net::" in msg:
                logging.warning("Network error, Retrying match... | match=%d | attempt=%d", match_num, attempt)
                await asyncio.sleep(5)
                continue
            logging.exception("Playwright error | match=%d | tab=%d/%d", match_num, current_tab_index, tabs_count)

        except Exception:
            logging.exception("Unknown error | match=%d | attempt=%d | tab=%d/%d", match_num, attempt, current_tab_index, tabs_count)

            logging.info("Retrying...")
            await asyncio.sleep(2)

        finally:
            if not page.is_closed():
                await page.close()


        if len(pages) == tabs_count - skipped_page:
            for i, html_page in enumerate(pages, start=2):
                await save_html_file(f"({sport}) - (Match {match_num}) - (tab {i}).html", html_page)
            break

async def process_single_match(sport: str, match_number: int, link: str, total_matches: int, session_mgr: SessionManager, context: BrowserContext) -> All_markets:
    url = parse.urljoin(base_url, link)

    for attempt in range(1, 6):
        logging.info("Extracting markets | match=%d/%d | attempt=%d", match_number, total_matches, attempt)

        match_page = await fetch(url, session_mgr)
        json_data = get_json_data(match_page)

        important_keys = [k for k in json_data if k.startswith("grpc:")]
        main_key = important_keys[1] if len(important_keys) > 1 else None

        payload = json_data.get(main_key, {}).get("response", {}).get("payload", {})
        subcats = payload.get("match", {}).get("subCategories")

        if isinstance(subcats, list):
            markets = subcats[0].get("markets", [])
            await get_markets_from_other_tabs(context, sport, match_number, url)
            return All_markets(markets=[Market_details.from_raw(m) for m in markets])

        logging.info("Invalid subCategories, retrying...")

    raise RuntimeError(f"Failed to fetch markets for match {match_number}")

# # Gets market details of a match from all available tabs inside a match page
async def get_detailed_markets(sport: str, links: list[str], session_mgr: SessionManager, browser_obj: Browser, max_workers: int) -> list[All_markets | None]:
    results: list[All_markets | None] = [None] * len(links)
    queue = asyncio.Queue()

    for idx, link in enumerate(links):
        await queue.put((idx, link))

    async def worker(worker_id: int):
        context = await browser_obj.new_context(extra_http_headers=headers_for_playwright)
        await context.add_cookies(convert_cookies(cookies_for_playwright))

        try:
            while True:
                try:
                    idx, link = await queue.get()
                except asyncio.CancelledError:
                    return

                try:
                    result = await process_single_match(sport, idx + 1, link, len(links), session_mgr, context)
                    results[idx] = result
                except Exception:
                    logging.exception("Worker %d failed match %d", worker_id, idx + 1)
                    results[idx] = None
                finally:
                    queue.task_done()
        finally:
            await context.close()

    workers = [asyncio.create_task(worker(i)) for i in range(max_workers)]
    await queue.join()

    for w in workers:
        w.cancel()

    await asyncio.gather(*workers, return_exceptions=True)
    return results


# async def get_detailed_markets(sport: str, links: list[str], session: AsyncSession, browser_context: BrowserContext) -> list[All_markets]:
#     clean_markets = []

#     for match_number, link in enumerate(links, start=1):
#         url = parse.urljoin(base_url, link)

#         for attempt in range(1, 6):
#             print(f"\nExtracting markets of match {match_number}/{len(links)} (attempt {attempt})")

#             match_page = await fetch(url, session)
#             json_data = get_json_data(match_page)

#             important_keys = [k for k in json_data if k.startswith("grpc:")]
#             main_key = important_keys[1] if len(important_keys) > 1 else None

#             payload = json_data.get(main_key, {}).get("response", {}).get("payload", {})
#             subcats = payload.get("match", {}).get("subCategories")  

#             if isinstance(subcats, list):
#                 markets = subcats[0].get("markets", [])
#                 await get_markets_from_other_tabs(browser_context, sport, match_number, url)
#                 break

#             # Logging errors
#             if not isinstance(subcats, list):
#                 error_msg = f"Invalid subCategories: ({type(subcats)}), retrying..."
#             else:
#                 error_msg = "Something unknown went wrong"

#             print(error_msg)
#         else:
#             raise RuntimeError(f"Failed to fetch markets for match {match_number}")

#         clean_market = All_markets(markets=[Market_details.from_raw(m) for m in markets])
#         clean_markets.append(clean_market)

#     return clean_markets

# Not importnat func (It's here just for testing and will be removed soon).
# async def save_raw_json_cards(links: list[str], session, sport_name) -> None:

#     for i, link in enumerate(links, start=1):
#         url = parse.urljoin(base_url, link)
#         file_path = f"({sport_name}) card {i}.json"

#         for attempt in range(1, 6):
#             print(f"Saving card {i} (attempt {attempt})")

#             card_page = await fetch(url, session)
#             json_data = get_json_data(card_page)
            

#             important_keys = [k for k in json_data if k.startswith("grpc:")]
#             main_key = important_keys[1] if len(important_keys) > 1 else None

#             payload = json_data.get(main_key, {}).get("response", {}).get("payload", {})
#             subcats = payload.get("match", {}).get("subCategories")

#             if isinstance(subcats, list):
#                 await save_json_file(file_path, json_data)
#                 break

#             print(f"subCategories invalid ({type(subcats)}), retrying...")
#         else:
#             raise RuntimeError(f"Failed to load markets for card {i}")
        

#     print("Saved everything!")
#     return None


# Main scraper logic and entry point of the script
async def main():
    log_file = setup_logging()
    logging.info("Initializing the scraper...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        max_match_workers = 3

        logging.info("Scraper started\n")
        if log_file:
            logging.info("Logging to %s", log_file)

        for sport_name, relative_path in sports.items():
            attempt = 0

            while True:
                attempt += 1

                try: 
                    logging.info("Scraping %s | attempt=%d", sport_name, attempt)

                    session_mgr = SessionManager(max_requests=1, rest_seconds=(0, 0.5))
                    url = parse.urljoin(base_url, relative_path)
                    response_html = await fetch(url, session_mgr)

                    logging.info("Extracting urls of all matches...\n")
                    urls, page_json = get_urls_and_json(response_html)

                    # print("Saving all raw cards...")
                    # await save_raw_json_cards(urls, session, sport_name)

                    logging.info("Extracting markets...")
                    # try:
                    all_clean_markets = await get_detailed_markets(sport_name, urls, session_mgr, browser, max_match_workers)
                    # finally:
                    #     for task in asyncio.all_tasks():
                    #         task.cancel()

                    important_keys = [k for k in page_json if k.startswith("grpc:")]
                    main_key = important_keys[1] if len(important_keys) > 1 else None
                    playload = page_json.get(main_key, {}).get("response", {}).get("payload", {})

                    logging.info("Scraped %d matches from %d URLs.", len(playload.get("matches", [])), len(urls))
                    clean_data = Sport_data(**playload)

                    if not clean_data.sport_name:
                        clean_data.sport_name = sport_name
                
                    for index, match in enumerate(clean_data.matches):
                        if all_clean_markets[index] != None:
                            match.all_Markets = all_clean_markets[index]

                    file_path = f"{sport_name}.json"
                    await save_json_file(file_path, clean_data.model_dump())

                    logging.info("\n")
                    break

                except KeyboardInterrupt:
                    logging.info("\nStopping scraper...")
                    return

                except Exception:
                    logging.exception("\nUnknown error | sport=%s | attempt=%d", sport_name, attempt)
                    logging.info("Retrying...\n")
                    await asyncio.sleep(2)

                finally:
                    await session_mgr.close()

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())