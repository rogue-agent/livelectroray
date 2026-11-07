"""
sources.py — Nord Pool scope, named live data sources, and the fetch tools.
harness.py imports these; keep edits to data sources in this file.
"""

import json, os, time, urllib.error, urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

# key.txt beside this module holds API keys: "NAME=value" lines, or a bare
# value which is taken as FINGRID_API_KEY. Env vars win; the file is a fallback.
_keyfile = Path(__file__).with_name("key.txt")
if _keyfile.exists():
    for _line in _keyfile.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#"):
            _k, _sep, _v = _line.partition("=")
            os.environ.setdefault(_k if _sep else "FINGRID_API_KEY", (_v if _sep else _k).strip())

MAX_BYTES   = 1_000_000             # fetch_url read cap
MAX_CHARS   = 20_000                # fetch_url reply cap

# ─── Nord Pool scope ──────────────────────────────────────
# Bidding zones of the Nord Pool market. Data about anything else is out of scope.
AREAS = ("FI", "SE1", "SE2", "SE3", "SE4", "NO1", "NO2", "NO3", "NO4", "NO5",
         "DK1", "DK2", "EE", "LV", "LT")

# Named sources the model may pull from. {placeholders} are filled by get_data().
# Everything here is Nord Pool relevant; hosts outside the set are refused.
SOURCES = {
    "nordpool_price": {          # day-ahead spot price, every area, keyless
        "url": "https://dashboard.elering.ee/api/nps/price",
        "params": "start,end = ISO8601 to window a past day (optional)",
        "note": "same Nord Pool spot for all areas; Fingrid sells no price in open data",
    },
    "spot_price_fi": {           # Finland current spot, keyless
        "url": "https://api.spot-hinta.fi/JustNow",
        "params": "",
    },
    # Statnett (Norway's TSO) open REST, keyless — Nordic-wide, not just NO.
    "statnett_overview": {       # latest Nordic production/consumption per area by source, MW
        "url": "https://driftsdata.statnett.no/restapi/ProductionConsumption/GetLatestDetailedOverview",
        "params": "",
        "note": "one call covers NO1-5 + SE + FI + DK by hydro/wind/nuclear/thermal + net exchange",
    },
    "statnett_prodcons": {       # Nordic production/consumption daily series, MW
        "url": "https://driftsdata.statnett.no/restapi/ProductionConsumption/GetData",
        "params": "From,To = YYYY-MM-DD (optional, default last 7 days; append via fetch_url)",
    },
    "statnett_flow": {           # Nordic net physical exchange daily series, MW
        "url": "https://driftsdata.statnett.no/restapi/PhysicalFlow/GetData",
        "params": "From = YYYY-MM-DD (optional, default last 7 days; append via fetch_url)",
    },
    "statnett_power_situation": {# Statnett's grid-capacity assessment per NO price area
        "url": "https://driftsdata.statnett.no/restapi/ElspotPowerSituation/GetPowerSituations",
        "params": "",
    },
    "statnett_reservoir": {      # Norwegian hydro reservoir fill % per NO area + total, weekly
        "url": "https://driftsdata.statnett.no/restapi/Reservoir/LastWeekData/{weeks}",
        "params": "weeks (optional, default 3; up to 52)",
        "defaults": {"weeks": 3},
        "note": "the multi-week hydro/price driver for the Norwegian zones",
    },
    "cloud_wind": {              # ICOWL weather+wind source (open-meteo): cloud cover, wind
        "url": "https://api.open-meteo.com/v1/forecast"
               "?latitude={lat}&longitude={lon}"
               "&current=cloud_cover,wind_speed_10m,wind_direction_10m,wind_gusts_10m,temperature_2m"
               "&hourly=cloud_cover,wind_speed_10m,wind_gusts_10m,temperature_2m"
               "&forecast_days=8&wind_speed_unit=ms&timezone=UTC",
        "params": "lat, lon (required)",
        "note": "current + hourly out to 8 days, the forward weather for wind prediction",
    },
    "sun_position": {            # solar irradiance + daylight window, for solar productivity (open-meteo)
        "url": "https://api.open-meteo.com/v1/forecast"
               "?latitude={lat}&longitude={lon}"
               "&current=shortwave_radiation,direct_normal_irradiance,diffuse_radiation,terrestrial_radiation"
               "&hourly=shortwave_radiation,direct_normal_irradiance,cloud_cover"
               "&daily=sunrise,sunset,daylight_duration,sunshine_duration"
               "&forecast_days=8&timezone=UTC",
        "params": "lat, lon (required)",
        "note": "W/m2 GHI/DNI + sunrise/sunset, current and hourly to 8 days; "
                "max potential PV ~ GHI * panel area * efficiency",
    },
    "warnings_areal": {          # areal disaster warnings, Nordics (MET Norway)
        "url": "https://api.met.no/weatherapi/metalerts/2.0/current.json?lat={lat}&lon={lon}",
        "params": "lat, lon (required)",
    },
    "warnings_global": {         # ICOWL hazard feed (GDACS)
        "url": "https://www.gdacs.org/gdacsapi/api/events/geteventlist/MAP?eventtype={eventtype}",
        "params": "eventtype (optional)",
        "defaults": {"eventtype": "WF"},
    },
    "outages_caruna": {          # Caruna DSO live network outages, keyless
        "url": "https://plus.caruna.fi/current-outages.json",
        "params": "",
    },
    "outages_finland_dso": {     # every Finnish DSO live outage summary, keyless
        "url": "https://sqtb-api.azureedge.net/outagemap/tailored/summary/",
        "params": "",
        "hdr": {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                             "(KHTML, like Gecko) Chrome/131.0 Safari/537.36",
                "Origin": "https://sahkotutka.fi", "Referer": "https://sahkotutka.fi/"},
        "note": "serves 403 without an Origin header",
    },
    "transfer_capacity": {       # regional transfer capacity, day-ahead (ENTSO-E docType A61)
        "url": "https://web-api.tp.entsoe.eu/api?securityToken={token}"
               "&documentType=A61&contract_MarketAgreement.Type=A01"
               "&in_Domain={in_domain}&out_Domain={out_domain}"
               "&periodStart={start}&periodEnd={end}",
        "params": "in_domain, out_domain = EIC codes (FI 10YFI-1--------U), "
                  "start, end = YYYYMMDDHHMM (required)",
        "key": "ENTSOE_API_KEY",
    },
    "fingrid_flow": {            # FI↔SE1 tie line, real-time MW (positive = export)
        "url": "https://data.fingrid.fi/api/datasets/87/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 2 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
    },
    "fingrid_net_import": {      # Finland net import/export, real-time MW
        "url": "https://data.fingrid.fi/api/datasets/194/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 2 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
    },
    "fingrid_frequency": {       # system frequency, real-time Hz
        "url": "https://data.fingrid.fi/api/datasets/177/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 2 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
    },
    "fingrid_consumption": {     # Finland consumption, real-time MW
        "url": "https://data.fingrid.fi/api/datasets/193/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 2 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
    },
    "fingrid_solar_capacity": {  # installed solar capacity behind the forecast, MW
        "url": "https://data.fingrid.fi/api/datasets/267/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 6 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
        "back": 6,               # hourly series published with a lag
    },
    "fingrid_wind_gen": {        # Finland wind production, real-time MW (3 min)
        "url": "https://data.fingrid.fi/api/datasets/181/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 2 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
    },
    "fingrid_imbalance_price": { # imbalance price, 15 min, EUR/MWh — the near-term price driver
        "url": "https://data.fingrid.fi/api/datasets/319/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 2 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
    },
    "fingrid_nuclear": {         # Finland nuclear production, real-time MW (OL3 is ~1600 MW)
        "url": "https://data.fingrid.fi/api/datasets/188/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 2 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
    },
    "fingrid_hydro": {           # Finland hydro production, real-time MW
        "url": "https://data.fingrid.fi/api/datasets/191/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 2 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
    },
    "fingrid_production": {      # total Finland production, real-time MW
        "url": "https://data.fingrid.fi/api/datasets/192/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 2 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
    },
    "fingrid_surplus": {         # Finland production surplus/deficit, real-time MW (negative = deficit)
        "url": "https://data.fingrid.fi/api/datasets/198/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 2 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
    },
    "fingrid_imbalance_power": { # imbalance power FI-SE, hourly MW
        "url": "https://data.fingrid.fi/api/datasets/176/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 2 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
    },
    "capacity_fi_se1": {         # intraday transfer capacity FI-SE1, 15 min MW, keyless
        "url": "https://data.fingrid.fi/api/datasets/44/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 2 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
    },
    "capacity_fi_ee": {          # intraday transfer capacity FI-EE, 15 min MW, keyless
        "url": "https://data.fingrid.fi/api/datasets/111/data"
               "?startTime={start}&endTime={end}&pageSize=20",
        "params": "start, end = ISO8601 (default: last 2 hours)",
        "key": "FINGRID_API_KEY",
        "headers": {"x-api-key": "FINGRID_API_KEY"},
    },
    "transfer_flow": {           # live amount crossing a border (ENTSO-E docType A11)
        "url": "https://web-api.tp.entsoe.eu/api?securityToken={token}"
               "&documentType=A11&in_Domain={in_domain}&out_Domain={out_domain}"
               "&periodStart={start}&periodEnd={end}",
        "params": "in_domain, out_domain = EIC codes, start, end = YYYYMMDDHHMM (required)",
        "key": "ENTSOE_API_KEY",
    },
}
ALLOWED_HOSTS = {urlsplit(s["url"]).hostname for s in SOURCES.values()}

SYSTEM = """\
You are an analyst for the Nord Pool electricity market area
(FI, SE1-4, NO1-5, DK1-2, EE, LV, LT). Only use data about this area;
ignore anything outside it.
Pull live values with `get_data` (named source below) or `fetch_url` (any
allowed host), then analyse with `run_python` (pandas, numpy, scipy,
statsmodels, sklearn). Print results clearly.

get_data sources (* = forward-looking weather feed):
  nordpool_price       day-ahead spot, EUR/MWh, every area (params: start,end)
  spot_price_fi        Finland current spot, with tax
* cloud_wind           cloud cover + wind m/s at a point, hourly to 8 days (params: lat, lon)
* sun_position         solar irradiance W/m2 + cloud cover + sunrise/sunset, hourly to 8 days (lat, lon)
  warnings_areal       Nordic areal disaster warnings (params: lat, lon)
  warnings_global      global hazards (params: eventtype = WF|EQ|FL|TC|VO|DR)
  outages_caruna       Caruna network outages, live
  outages_finland_dso  every Finnish DSO outage summary
  statnett_overview    Nordic production+consumption per area by source, latest (NO1-5, SE, FI, DK)
  statnett_prodcons    Nordic production/consumption daily series (params: From, To = YYYY-MM-DD)
  statnett_flow        Nordic net physical exchange daily series (params: From)
  statnett_power_situation  Statnett grid-capacity assessment per NO price area
  statnett_reservoir   Norwegian hydro reservoir fill % per NO area, weekly (params: weeks)
  transfer_capacity    day-ahead cross-border transfer capacity (in_domain, out_domain, start, end)
  transfer_flow        live cross-border flow (in_domain, out_domain, start, end)
  fingrid_flow         FI-SE1 tie line flow, real-time MW (params: start, end)
  fingrid_net_import   Finland net import/export, real-time MW
  fingrid_frequency    system frequency, real-time Hz
  fingrid_consumption  Finland consumption, real-time MW
  fingrid_solar_capacity  installed solar capacity, MW
  fingrid_wind_gen     Finland wind production, real-time MW (actual, for baseline)
  fingrid_imbalance_price  imbalance price EUR/MWh, 15 min — the near-term price driver
  fingrid_nuclear      Finland nuclear production, real-time MW
  fingrid_hydro        Finland hydro production, real-time MW
  fingrid_production   total Finland production, real-time MW
  fingrid_surplus      Finland production surplus/deficit, MW (negative = deficit)
  fingrid_imbalance_power  imbalance power FI-SE, hourly MW
  capacity_fi_se1      intraday transfer capacity FI-SE1, 15 min MW
  capacity_fi_ee       intraday transfer capacity FI-EE, 15 min MW
  (fingrid_* and capacity_* need FINGRID_API_KEY in the env; transfer_* need ENTSOE_API_KEY)

Sources are meant to be combined, not reported one by one:
  solar: sun_position(lat,lon) GHI W/m2 is the physical ceiling; potential MW =
         GHI/1000 * fingrid_solar_capacity. No measured solar production is
         published, so the potential IS your estimate — label it modelled, not
         observed, and use cloud_wind cloud_cover to explain the shortfall.
  wind:  cloud_wind wind_speed_10m + a power curve vs fingrid_wind_gen actual.
         The curve is your model; fit or state its coefficients.
  price: nordpool_price vs fingrid_consumption / fingrid_net_import / transfer_flow /
         fingrid_surplus to say whether scarcity, imports or production moves the spot;
         fingrid_imbalance_price is the nearest 15 min signal of where the spot is going,
         and capacity_fi_se1 / capacity_fi_ee bound how much import can relieve a squeeze.
  risk:  warnings_areal / outages_caruna / outages_finland_dso over a zone — an
         outage or warning removes load or production, so it belongs in any
         production estimate; outages_finland_dso gives the national summary.
  nordic: statnett_overview gives every Nordic zone's generation mix and load in
         ONE keyless call — use it where the fingrid_* sources only cover Finland,
         and statnett_reservoir (NO hydro fill %) is the multi-week price driver for
         the Norwegian and Swedish zones. statnett_power_situation flags a strained
         area; statnett_flow is the Nordic net exchange.

PREDICTION IS THE POINT — you build the forecast yourself; no published generation,
demand or price forecast is in scope. Drive it from recent actuals plus the weather feed:
  24 h  next-day price shape hour by hour, solar and wind output, demand, net import,
        and the hour the price peaks. Model solar from GHI and wind from the power
        curve; extend demand from the last actuals by the weekly load shape. The
        day-ahead auction already prices tomorrow — that is the market's forecast,
        not yours: use hours up to now as calibration and predict the rest.
  7 d   the trend plus the edge cases: a wind lull, a cloud build-up, a demand step,
        an outage or a standing warning. Use the hourly weather feed as the driver and
        your own fitted relations for the rest; there is no published price that far out,
        so give it as a trend with a range and say so. State any
assumption (weather point, panel kWp, power curve) explicitly. For each horizon give the
expected number or range, the sources and their timestamps behind it, and what would
falsify it. Label anything derived only from past values as a trend, not a forecast, and
say which missing source weakened the prediction.
Always state the timestamp of each live value; they have different lags.

OUTPUT CONTRACT — after the analysis, end with ONE fenced csv block of the predicted
values so it can be exported to csv/markdown. Columns, exactly:
  horizon,time,area,metric,value,unit,low,high,confidence,drivers
horizon = 24h|7d; time = ISO8601 UTC (hourly rows for 24h, daily for 7d); value = your
forecast number; low/high = your range; drivers = the sources behind that row. Every row
is your own forecast. Prose above the block is expected; nothing after it.
"""


# ─── Tool: fetch_url ──────────────────────────────────────
def fetch_url(url: str, headers: dict = None) -> str:
    """GET a URL, return the body (JSON pretty-printed). Nord Pool hosts only."""
    host = urlsplit(url).hostname
    if host not in ALLOWED_HOSTS:
        return (f"Error: {host} is out of the Nord Pool scope; "
                f"allowed hosts: {', '.join(sorted(h for h in ALLOWED_HOSTS if h))}")
    try:
        for attempt in range(5):        # Fingrid allows ~1 req/s; a 429 is transient, retry
            req = urllib.request.Request(url, headers={"User-Agent": "livelectroray-harness/1", **(headers or {})})
            try:
                with urllib.request.urlopen(req, timeout=15) as r:
                    body = r.read(MAX_BYTES).decode(r.headers.get_content_charset() or "utf-8", "replace")
                break
            except urllib.error.HTTPError as e:
                # Fingrid 429s at ~1 req/s; the outage CDN 403s on a random edge. Both are transient.
                if e.code not in (429, 403, 502, 503, 504) or attempt == 4:
                    raise
                time.sleep(1.5 * (attempt + 1))
        try:
            d = json.loads(body)
        except ValueError:
            return body[:MAX_CHARS]
        s = json.dumps(d, indent=2)
        while len(s) > MAX_CHARS and isinstance(d.get("data"), list) and len(d["data"]) > 8:
            d["data"] = d["data"][::2]        # thin rows, keep the whole time span
            s = json.dumps(d, indent=2)
        return s[:MAX_CHARS]
    except Exception as e:
        return f"Error: {e}"


# ─── Tool: get_data ───────────────────────────────────────
def get_data(source: str, **params) -> str:
    """Fetch a named Nord Pool source (see SOURCES), filling its {placeholders}."""
    s = SOURCES.get(source)
    if not s:
        return f"Error: unknown source {source!r}; use one of: {', '.join(SOURCES)}"
    key = s.get("key")
    if key and not os.environ.get(key):
        return f"Error: source {source!r} needs {key} in the environment"
    params = {**s.get("defaults", {}), **params}
    if "start" in s["url"] and "start" not in params:     # default window
        now = datetime.now(timezone.utc)
        ahead = s.get("ahead")                            # forward feeds look ahead, the rest look back
        back = s.get("back", 2)                           # laggy series look further back
        params["start"] = (now if ahead else now - timedelta(hours=back)).strftime("%Y-%m-%dT%H:%M:%SZ")
        params["end"] = (now + timedelta(hours=ahead) if ahead else now).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        return fetch_url(s["url"].format(
            token=os.environ.get("ENTSOE_API_KEY", ""),
            **params), headers={**s.get("hdr", {}),
                                **{h: os.environ.get(v, "") for h, v in s.get("headers", {}).items()}})
    except KeyError as e:
        return f"Error: source {source!r} needs parameter {e} ({s['params']})"

# shortcut: 1 MB read / 20 k char reply cap, raise MAX_BYTES|MAX_CHARS if a source is bigger
