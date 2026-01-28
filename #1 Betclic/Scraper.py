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



