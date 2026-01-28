import json
from urllib import parse
from bs4 import BeautifulSoup
from curl_cffi import requests
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


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

class Selection(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
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
    selections: list[Selection] = Field(default_factory=list)

    @classmethod
    def from_raw(cls, raw: dict) -> "Market_details":
        selections: list[dict] = []
        selections.extend(raw.get("mainSelections", []))

        for group in raw.get("selectionMatrix", []):
            for item in group.get("selections", []):
                sel = item.get("selectionOneof", {}).get("selection")
                if sel:
                    selections.append(sel)

        for group in raw.get("splitCardGroups", []):
            selections.extend(group.get("selections", []))

        raw = dict(raw)
        raw["selections"] = selections

        return cls.model_validate(raw)

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
    all_Markets: list = Field(default_factory=list)

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

    sport_name: str = Field(alias="name")
    matches: list[Match] = Field(default_factory=list)

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    sport_name: str = Field(alias="name")
    matches: list[Match] = Field(default_factory=list)


# Helper funcs
def fetch(url: str) -> str:
    retries = 5
    try:
        for i in range(retries):
            response = requests.get(url, cookies=cookies, headers=headers, impersonate="chrome")

            if response.status_code == 200:
                return response.text
            
        raise Exception(f"Requests failed: Failed to get a response, status code: {response.status_code}")
    except Exception as e:
        print(e)

    return None

def save_json_file(file_path: str, data: dict) -> None:
    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=2)
        print(f"{file_path} saved successfully.")

def save_html_file(file_path: str, html: str) -> None:
    with open(file_path, "w", encoding="utf-8") as html_file:
        html_file.write(html)
        print(f"{file_path} saved successfully.")

def get_urls_and_json(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    card_tags = soup.select("a.cardEvent")
    card_urls = [tag['href'] for tag in card_tags]

    script_tag = soup.find("script", {"id": "ng-state"})
    json_data = json.loads(script_tag.string) if script_tag.string else {}

    return card_urls, json_data

def get_json_data(html: str) -> tuple:
    soup = BeautifulSoup(html, "html.parser")
    script_tag = soup.find("script", {"id": "ng-state"})
    json_data = json.loads(script_tag.string) if script_tag.string else {}

    return json_data

def get_detailed_markets(links: list[str]) -> list[All_markets]:
    clean_markets = []

    for i, link in enumerate(links, start=1):
        url = parse.urljoin(base_url, link)

        while True:
            print(f"Extracting makrets of card {i}")
            card_page = fetch(url)
            json_data = get_json_data(card_page)
            
            imporant_keys = [k for k in json_data if k.startswith("grpc:")]
            main_key = imporant_keys[1] if len(imporant_keys) > 1 else None
            
            payload = json_data.get(main_key, {}).get("response", {}).get("payload", {})
            subcats = payload.get("match", {}).get("subCategories")

            if isinstance(subcats, list):
                markets = subcats[0].get("markets", [])
                break
            else: 
                print(f"subCategories type is not list: {type(subcats)}\nTrying again...")

        # To skip the first and second summary card.
        filtered_markets = [m for m in markets if m["position"] != 1 and m["position"] != 2]

        clean_market = All_markets(markets=[Market_details.from_raw(m) for m in filtered_markets])
        clean_markets.append(clean_market)
        
    return clean_markets


cookies = {
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

headers = {
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

base_url = "https://www.betclic.fr"


def main():
    print("Scraper started!")
    response_html = fetch("https://www.betclic.fr/football-sfootball")

    print("Extracting urls of all cards...")
    urls, page_json = get_urls_and_json(response_html)

    print("Extracting markets...")
    all_clean_markets = get_detailed_markets(urls)

    imporant_keys = [k for k in page_json if k.startswith("grpc:")]
    main_key = imporant_keys[1] if len(imporant_keys) > 1 else None
    playload = page_json.get(main_key, {}).get("response", {}).get("payload", {})

    print(f'Total matches: {len(playload.get("matches", []))}\nTotal URLs: {len(urls)}')
    clean_data = Sport_data(**playload)
    
    for index, match in enumerate(clean_data.matches):
        match.all_Markets = all_clean_markets[index]
        
if __name__ == "__main__":
    main()