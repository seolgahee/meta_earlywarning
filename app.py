"""
Meta Ads 조기경보 시스템
- ASC 캠페인 구조 기반 action_type 분기
- Gemini AI 인사이트
- Office365 SMTP 이메일 발송
"""

import os
import json
import smtplib
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from google import genai

load_dotenv()

# ─────────────────────────────────────────
# 설정값
# ─────────────────────────────────────────
ACCESS_TOKEN  = os.getenv("META_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID")
API_VERSION   = os.getenv("META_API_VERSION", "v19.0")

SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE", "PU_PF")
SNOWFLAKE_TABLE     = os.getenv("SNOWFLAKE_TABLE", "META_AD_SNAPSHOT")

SMTP_SERVER      = os.getenv("SMTP_SERVER", "smtp.office365.com")
SMTP_PORT        = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER        = os.getenv("SMTP_USER")
SMTP_PASSWORD    = os.getenv("SMTP_PASSWORD")
ALERT_RECIPIENTS = os.getenv("ALERT_RECIPIENTS", "")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

BRAND          = "SERGIO_TACCHINI"
ALERT_LOG_FILE = "alert_sent_log.json"

if not ACCESS_TOKEN or not AD_ACCOUNT_ID:
    print("[오류] .env에 META_ACCESS_TOKEN, META_AD_ACCOUNT_ID 값이 없습니다.")
    exit(1)

_gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None


# ─────────────────────────────────────────
# 운영 시간 체크 (KST 01:00 ~ 07:00 실행 금지)
# ─────────────────────────────────────────
def check_operating_hours() -> None:
    kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
    hour    = kst_now.hour
    if 1 <= hour < 7:
        print(f"[종료] 새벽 시간대이므로 실행하지 않음 (현재 KST: {kst_now.strftime('%Y-%m-%d %H:%M')})")
        exit(0)
    print(f"[정보] 운영 시간 확인 완료 (현재 KST: {kst_now.strftime('%Y-%m-%d %H:%M')})")


# ─────────────────────────────────────────
# Alert 조건 (운영 기준)
# ─────────────────────────────────────────
# Opportunity 공통 필터
OPP_FILTER = {
    "purchases_6h_min": 3,
    "spend_6h_min":     100_000,
    "roas_6h_min":      3.0,   # 300%
    # roas_6h >= roas_12h 는 코드에서 직접 비교
}

# action_type 분기 조건 (우선순위: CAMPAIGN_SCALE > PRODUCT_EXTRACTION > CREATIVE_EXPANSION)
ACTION_CONDITIONS = {
    "CAMPAIGN_SCALE": {
        "roas_6h_min":      3.0,   # 300%
        "purchases_6h_min": 5,
        "guide": "ASC 캠페인 일예산 10~15% 증액 검토",
    },
    "PRODUCT_EXTRACTION": {
        "roas_6h_min":  3.0,       # 300%
        "spend_6h_min": 100_000,
        "guide": "해당 상품 분리 후 별도 ASC 테스트 캠페인 운영 권장",
    },
    "CREATIVE_EXPANSION": {
        "roas_6h_min":      2.5,   # 250%
        "purchases_6h_min": 2,
        "guide": "동일 상품 기반 신규 소재 2~3종 제작 권장",
    },
}

# Kill Alert 조건
KILL_CONDITION = {
    "roas_12h_max":  1.2,     # 120%
    "spend_12h_min": 150_000,
}


# ─────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────
def extract_purchase_count(actions: list) -> int:
    if not actions:
        return 0
    for item in actions:
        if item.get("action_type") == "purchase":
            return int(float(item.get("value", 0)))
    return 0


def extract_purchase_revenue(action_values: list) -> float:
    if not action_values:
        return 0.0
    for item in action_values:
        if item.get("action_type") == "purchase":
            return float(item.get("value", 0.0))
    return 0.0


def detect_channel(campaign_name: str, adset_name: str) -> str:
    text = f"{campaign_name or ''} {adset_name or ''}".lower()
    if "무신사" in text or "musinsa" in text:
        return "MUSINSA"
    return "OFFICIAL"


def determine_action_type(roas_6h: float, spend_6h: float, purchases_6h: float) -> str | None:
    """우선순위 순으로 action_type 결정. 해당 없으면 None."""
    c = ACTION_CONDITIONS
    if roas_6h >= c["CAMPAIGN_SCALE"]["roas_6h_min"] and purchases_6h >= c["CAMPAIGN_SCALE"]["purchases_6h_min"]:
        return "CAMPAIGN_SCALE"
    if roas_6h >= c["PRODUCT_EXTRACTION"]["roas_6h_min"] and spend_6h >= c["PRODUCT_EXTRACTION"]["spend_6h_min"]:
        return "PRODUCT_EXTRACTION"
    if roas_6h >= c["CREATIVE_EXPANSION"]["roas_6h_min"] and purchases_6h >= c["CREATIVE_EXPANSION"]["purchases_6h_min"]:
        return "CREATIVE_EXPANSION"
    return None


# ─────────────────────────────────────────
# 중복 발송 방지 + repeat_count (로컬 JSON)
# ─────────────────────────────────────────
def load_alert_log() -> dict:
    if not os.path.exists(ALERT_LOG_FILE):
        return {}
    with open(ALERT_LOG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_alert_log(log: dict) -> None:
    with open(ALERT_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)


def is_recently_alerted(ad_id: str, hours: int = 12) -> bool:
    log = load_alert_log()
    entry = log.get(str(ad_id), {})
    last_sent_str = entry.get("last_sent") if isinstance(entry, dict) else entry
    if not last_sent_str:
        return False
    last_sent = datetime.fromisoformat(last_sent_str)
    return datetime.now(timezone.utc) - last_sent < timedelta(hours=hours)


def get_repeat_count(ad_id: str, days: int = 7) -> int:
    log = load_alert_log()
    entry = log.get(str(ad_id), {})
    if not isinstance(entry, dict):
        return 1 if entry else 0
    history = entry.get("history", [])
    cutoff  = datetime.now(timezone.utc) - timedelta(days=days)
    return sum(1 for ts in history if datetime.fromisoformat(ts) >= cutoff)


def mark_alert_sent(ad_id: str) -> None:
    log   = load_alert_log()
    now   = datetime.now(timezone.utc).isoformat()
    key   = str(ad_id)
    entry = log.get(key, {})
    if not isinstance(entry, dict):
        entry = {}
    history = entry.get("history", [])
    history.append(now)
    # 최근 30일 이력만 유지
    cutoff  = datetime.now(timezone.utc) - timedelta(days=30)
    history = [ts for ts in history if datetime.fromisoformat(ts) >= cutoff]
    log[key] = {"last_sent": now, "history": history}
    save_alert_log(log)


# ─────────────────────────────────────────
# Gemini AI 인사이트
# ─────────────────────────────────────────
FALLBACK = {
    "CAMPAIGN_SCALE":    ("6시간 기준 ROAS 및 구매 건수가 기준치를 상회하며 예산 확장 구간으로 판단됩니다.",
                          "ASC 캠페인 일예산 10~15% 증액 검토를 권장합니다."),
    "PRODUCT_EXTRACTION": ("해당 소재의 소진 및 ROAS가 기준치를 초과하여 상품 분리 테스트 구간으로 판단됩니다.",
                            "동일 상품으로 별도 ASC 테스트 캠페인 운영을 권장합니다."),
    "CREATIVE_EXPANSION": ("해당 소재의 전환 반응이 확대되는 구간으로 소재 확장 필요성이 감지됩니다.",
                            "동일 상품 기반 신규 소재 2~3종 제작을 권장합니다."),
}


def generate_ai_insight(alert: dict) -> tuple[str, str]:
    action_type = alert["action_type"]
    fallback    = FALLBACK.get(action_type, FALLBACK["CREATIVE_EXPANSION"])

    if not _gemini_client:
        return fallback

    action_context = {
        "CAMPAIGN_SCALE":    "예산 확장 구간 판단 근거를 설명하고, ASC 일예산 증액의 적절성을 짧게 서술하세요.",
        "PRODUCT_EXTRACTION": "상품 분리 테스트가 필요한 근거를 데이터 기반으로 설명하세요.",
        "CREATIVE_EXPANSION": "소재 확장이 필요한 근거를 전환 데이터 기반으로 설명하세요.",
    }

    prompt = f"""
당신은 디지털 광고 퍼포먼스 마케터입니다.
아래 Meta 광고 데이터를 보고 AI 인사이트와 액션 가이드를 각각 한 문장으로 작성하세요.

[광고 정보]
- 캠페인: {alert['campaign_name']}
- 광고세트: {alert['adset_name']}
- 광고소재: {alert['ad_name']}
- 채널: {alert['channel']}
- action_type: {action_type}

[최근 6시간 성과]
- Spend_6h: {alert['spend_6h']:,.0f}원
- Purchases_6h: {int(alert['purchases_6h'])}건
- Revenue_6h: {alert['revenue_6h']:,.0f}원
- ROAS_6h: {alert['roas_6h']:.1%}
- ROAS_12h: {alert['roas_12h']:.1%}
- CTR_6h: {alert.get('ctr_6h', 0):.2%}

[작성 지침]
- {action_context.get(action_type, '')}
- 입력 데이터만 근거로 해석, 외부 요인 추정 금지
- purchases_6h > 0, revenue_6h > 0인 경우에만 긍정적 인사이트 허용
- 숫자 과장 금지, 한국어, 짧고 실무적인 톤

[출력 형식] (반드시 아래 형식 그대로)
AI_INSIGHT: (한 문장)
ACTION_GUIDE: (한 문장)
""".strip()

    try:
        response = _gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
        )
        text    = response.text.strip()
        insight = fallback[0]
        guide   = fallback[1]
        for line in text.splitlines():
            if line.startswith("AI_INSIGHT:"):
                insight = line.replace("AI_INSIGHT:", "").strip()
            elif line.startswith("ACTION_GUIDE:"):
                guide = line.replace("ACTION_GUIDE:", "").strip()
        return insight, guide
    except Exception as e:
        print(f"[경고] Gemini 호출 실패 ({e}) - fallback 사용")
        return fallback


# ─────────────────────────────────────────
# 이메일 발송
# ─────────────────────────────────────────
ACTION_TYPE_KO = {
    "CAMPAIGN_SCALE":    "캠페인 예산 확장",
    "PRODUCT_EXTRACTION": "상품 분리 테스트",
    "CREATIVE_EXPANSION": "소재 확장",
}
ACTION_TYPE_COLOR = {
    "CAMPAIGN_SCALE":    "#1a73e8",
    "PRODUCT_EXTRACTION": "#e67e22",
    "CREATIVE_EXPANSION": "#27ae60",
}


def build_email_html(alerts: list) -> str:
    now_kst = datetime.now(timezone.utc) + timedelta(hours=9)
    blocks  = ""

    for a in alerts:
        action_type  = a["action_type"]
        color        = ACTION_TYPE_COLOR.get(action_type, "#1a73e8")
        action_ko    = ACTION_TYPE_KO.get(action_type, action_type)
        repeat_label = f"{a['repeat_count']}회" if a["repeat_count"] > 1 else "첫 발생"

        blocks += f"""
        <div style="border:1px solid #e0e0e0;border-radius:8px;padding:20px;margin-bottom:24px;">
          <div style="display:flex;align-items:center;margin-bottom:12px;">
            <span style="background:{color};color:#fff;padding:4px 10px;border-radius:4px;
                         font-size:12px;font-weight:bold;margin-right:10px;">
              {action_type}
            </span>
            <span style="color:#666;font-size:13px;">{action_ko}</span>
            <span style="margin-left:auto;color:#999;font-size:12px;">최근 7일 {repeat_label}</span>
          </div>

          <table style="width:100%;font-size:13px;border-collapse:collapse;margin-bottom:12px;">
            <tr>
              <td style="padding:4px 8px;color:#888;width:110px;">Campaign</td>
              <td style="padding:4px 8px;font-family:monospace;font-size:12px;">{a['campaign_name']}</td>
            </tr>
            <tr style="background:#f9f9f9;">
              <td style="padding:4px 8px;color:#888;">Ad Set</td>
              <td style="padding:4px 8px;font-family:monospace;font-size:12px;">{a['adset_name']}</td>
            </tr>
            <tr>
              <td style="padding:4px 8px;color:#888;">Creative</td>
              <td style="padding:4px 8px;font-family:monospace;font-size:12px;">{a['ad_name']}</td>
            </tr>
            <tr style="background:#f9f9f9;">
              <td style="padding:4px 8px;color:#888;">Ad ID</td>
              <td style="padding:4px 8px;color:#555;font-size:12px;">{a['ad_id']}</td>
            </tr>
          </table>

          <h4 style="margin:12px 0 8px;color:#333;font-size:13px;">최근 6시간 성과</h4>
          <table style="border-collapse:collapse;width:100%;font-size:13px;">
            <thead>
              <tr style="background:#f0f4ff;">
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:left;">지표</th>
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:right;">값</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">Spend_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{a['spend_6h']:,.0f}원</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">Purchases_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{int(a['purchases_6h'])}건</td>
              </tr>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">Revenue_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{a['revenue_6h']:,.0f}원</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">ROAS_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:{color};font-weight:bold;">{a['roas_6h']:.1%}</td>
              </tr>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">ROAS_12h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{a['roas_12h']:.1%}</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">CTR_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{a.get('ctr_6h', 0):.2%}</td>
              </tr>
            </tbody>
          </table>

          <div style="margin-top:12px;padding:12px;background:#f0f7ff;border-left:4px solid {color};border-radius:4px;">
            <p style="margin:0 0 4px;font-size:11px;color:#888;font-weight:bold;">AI 인사이트</p>
            <p style="margin:0;font-size:13px;color:#333;">{a['ai_insight']}</p>
          </div>
          <div style="margin-top:8px;padding:12px;background:#fff8e1;border-left:4px solid #f9a825;border-radius:4px;">
            <p style="margin:0 0 4px;font-size:11px;color:#888;font-weight:bold;">액션 가이드</p>
            <p style="margin:0;font-size:13px;color:#333;">{a['action_guide']}</p>
          </div>
        </div>
        """

    return f"""
    <html><body style="font-family:Arial,sans-serif;color:#333;max-width:700px;margin:0 auto;padding:20px;">
      <h2 style="color:#1a73e8;margin-bottom:4px;">Meta Ads Opportunity Alert</h2>
      <p style="color:#888;font-size:13px;margin-top:0;">
        {BRAND} &nbsp;|&nbsp; {now_kst.strftime('%Y-%m-%d %H:%M')} KST
      </p>
      {blocks}
      <p style="margin-top:24px;font-size:11px;color:#aaa;border-top:1px solid #eee;padding-top:12px;">
        * 동일 광고(ad_id)는 12시간 내 중복 발송되지 않습니다.
      </p>
    </body></html>
    """


def send_alert_email(alerts: list) -> None:
    if not SMTP_USER or not SMTP_PASSWORD:
        print("[경고] SMTP 설정 없음 - 이메일 발송 건너뜀")
        return

    recipients = [r.strip() for r in ALERT_RECIPIENTS.split(",") if r.strip()]
    if not recipients:
        print("[경고] ALERT_RECIPIENTS 없음 - 이메일 발송 건너뜀")
        return

    # action_type 목록을 제목에 표시
    types_str = ", ".join(sorted({a["action_type"] for a in alerts}))
    subject   = f"[Opportunity Alert - {types_str}] {BRAND} ({len(alerts)}개 광고)"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(build_email_html(alerts), "html", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, recipients, msg.as_string())
        print(f"[완료] 이메일 발송 성공 -> {', '.join(recipients)}")
    except Exception as e:
        print(f"[오류] 이메일 발송 실패: {e}")


# ─────────────────────────────────────────
# Meta API 호출
# ─────────────────────────────────────────
def fetch_insights() -> list:
    url    = f"https://graph.facebook.com/{API_VERSION}/{AD_ACCOUNT_ID}/insights"
    params = {
        "access_token": ACCESS_TOKEN,
        "level":        "ad",
        "date_preset":  "today",
        "fields": ",".join([
            "campaign_id", "campaign_name",
            "adset_id", "adset_name",
            "ad_id", "ad_name",
            "impressions", "clicks", "spend",
            "actions", "action_values",
        ]),
        "limit": 500,
    }

    all_data = []
    print(f"[정보] Meta API 호출 중... (버전: {API_VERSION})")

    while url:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            err = response.json().get("error", {})
            print(f"[오류] API 호출 실패 (HTTP {response.status_code}): {err.get('message')}")
            return []
        body = response.json()
        all_data.extend(body.get("data", []))
        url    = body.get("paging", {}).get("next")
        params = {}

    return all_data


# ─────────────────────────────────────────
# 데이터 가공 -> DataFrame
# ─────────────────────────────────────────
def build_dataframe(raw_data: list) -> pd.DataFrame:
    snapshot_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for item in raw_data:
        campaign_name = item.get("campaign_name", "")
        adset_name    = item.get("adset_name", "")

        rows.append({
            "SNAPSHOT_TS":     snapshot_ts,
            "BRAND":           BRAND,
            "CHANNEL":         detect_channel(campaign_name, adset_name),
            "AD_ACCOUNT_ID":   AD_ACCOUNT_ID,
            "CAMPAIGN_ID":     item.get("campaign_id"),
            "CAMPAIGN_NAME":   campaign_name,
            "ADSET_ID":        item.get("adset_id"),
            "ADSET_NAME":      adset_name,
            "AD_ID":           item.get("ad_id"),
            "AD_NAME":         item.get("ad_name"),
            "IMPRESSIONS_CUM": int(item.get("impressions", 0)),
            "CLICKS_CUM":      int(item.get("clicks", 0)),
            "SPEND_CUM":       float(item.get("spend", 0.0)),
            "PURCHASES_CUM":   extract_purchase_count(item.get("actions", [])),
            "REVENUE_CUM":     extract_purchase_revenue(item.get("action_values", [])),
        })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────
# Snowflake
# ─────────────────────────────────────────
def get_snowflake_conn():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )


def load_to_snowflake(df: pd.DataFrame) -> None:
    required = [SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
                SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA]
    if not all(required):
        print("[경고] Snowflake 환경변수 누락 - 적재 건너뜀")
        return

    insert_cols = [
        "SNAPSHOT_TS", "BRAND", "AD_ACCOUNT_ID",
        "CAMPAIGN_ID", "CAMPAIGN_NAME",
        "ADSET_ID", "ADSET_NAME",
        "AD_ID", "AD_NAME",
        "IMPRESSIONS_CUM", "CLICKS_CUM", "SPEND_CUM", "PURCHASES_CUM", "REVENUE_CUM",
        "CHANNEL",
    ]
    df_insert = df[insert_cols]

    print(f"[정보] Snowflake 연결 중... ({SNOWFLAKE_ACCOUNT})")
    conn = get_snowflake_conn()
    try:
        cursor       = conn.cursor()
        cols         = ", ".join(insert_cols)
        placeholders = ", ".join(["%s"] * len(insert_cols))
        sql          = f"INSERT INTO {SNOWFLAKE_TABLE.upper()} ({cols}) VALUES ({placeholders})"
        rows         = [tuple(row) for row in df_insert.itertuples(index=False)]
        cursor.executemany(sql, rows)
        print(f"[완료] Snowflake 적재 성공 - {len(rows)}행")
    finally:
        conn.close()


# ─────────────────────────────────────────
# Alert 판단
# ─────────────────────────────────────────
def evaluate_alerts(df_now: pd.DataFrame) -> None:
    required = [SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
                SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA]
    if not all(required):
        return

    # ── Snowflake에서 6h / 12h 전 스냅샷 조회 ──
    conn = get_snowflake_conn()
    try:
        cursor = conn.cursor()
        for hours, label in [(6, "6H"), (12, "12H")]:
            query = f"""
                SELECT ad_id, channel,
                       spend_cum, purchases_cum, revenue_cum,
                       clicks_cum, impressions_cum
                FROM (
                    SELECT ad_id, channel,
                           spend_cum, purchases_cum, revenue_cum,
                           clicks_cum, impressions_cum,
                           ROW_NUMBER() OVER (
                               PARTITION BY ad_id
                               ORDER BY ABS(DATEDIFF('minute',
                                   snapshot_ts,
                                   DATEADD('hour', -{hours},
                                       CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
                               ))
                           ) AS rn
                    FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE.upper()}
                    WHERE brand = '{BRAND}'
                      AND snapshot_ts >= DATEADD('hour', -{hours + 2},
                              CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
                      AND snapshot_ts <= DATEADD('hour', -{hours - 2},
                              CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
                ) WHERE rn = 1
            """
            cursor.execute(query)
            df_past = pd.DataFrame(
                cursor.fetchall(),
                columns=["AD_ID", "CHANNEL",
                         f"SPEND_{label}_PAST", f"PURCHASES_{label}_PAST", f"REVENUE_{label}_PAST",
                         f"CLICKS_{label}_PAST", f"IMPRESSIONS_{label}_PAST"],
            )
            df_now = df_now.merge(df_past, on=["AD_ID", "CHANNEL"], how="left")
    finally:
        conn.close()

    # ── 델타 / ROAS / CTR 계산 ──
    for label in ["6H", "12H"]:
        lbl = label.lower()
        ps  = pd.to_numeric(df_now[f"SPEND_{label}_PAST"],       errors="coerce").fillna(0.0)
        pp  = pd.to_numeric(df_now[f"PURCHASES_{label}_PAST"],   errors="coerce").fillna(0.0)
        pr  = pd.to_numeric(df_now[f"REVENUE_{label}_PAST"],     errors="coerce").fillna(0.0)
        pc  = pd.to_numeric(df_now[f"CLICKS_{label}_PAST"],      errors="coerce").fillna(0.0)
        pi  = pd.to_numeric(df_now[f"IMPRESSIONS_{label}_PAST"], errors="coerce").fillna(0.0)

        df_now[f"spend_{lbl}"]       = df_now["SPEND_CUM"]       - ps
        df_now[f"purchases_{lbl}"]   = df_now["PURCHASES_CUM"]   - pp
        df_now[f"revenue_{lbl}"]     = df_now["REVENUE_CUM"]     - pr
        df_now[f"clicks_{lbl}"]      = df_now["CLICKS_CUM"]      - pc
        df_now[f"impressions_{lbl}"] = df_now["IMPRESSIONS_CUM"] - pi

        df_now[f"roas_{lbl}"] = df_now.apply(
            lambda r, l=lbl: r[f"revenue_{l}"] / r[f"spend_{l}"] if r[f"spend_{l}"] > 0 else 0,
            axis=1,
        )
        df_now[f"ctr_{lbl}"] = df_now.apply(
            lambda r, l=lbl: r[f"clicks_{l}"] / r[f"impressions_{l}"] if r[f"impressions_{l}"] > 0 else 0,
            axis=1,
        )

    # ── Alert 판단 ──
    print("\n" + "=" * 65)
    print("  ALERT 리포트")
    print("=" * 65)

    opp_alerts = []
    kill_found = False

    for _, row in df_now.iterrows():
        ad_info = f"[{row.get('CHANNEL','OFFICIAL')}] {row['AD_NAME']} (ad_id: {row['AD_ID']})"

        # ── 필터: purchases / revenue 가 0이면 skip ──
        if row["purchases_6h"] <= 0 or row["revenue_6h"] <= 0:
            continue

        # ── Opportunity Alert 공통 조건 ──
        roas_improving = row["roas_6h"] >= row["roas_12h"]
        opp_gate = (
            row["roas_6h"]     >= OPP_FILTER["roas_6h_min"]
            and row["spend_6h"]     >= OPP_FILTER["spend_6h_min"]
            and row["purchases_6h"] >= OPP_FILTER["purchases_6h_min"]
            and roas_improving
        )

        if opp_gate:
            action_type = determine_action_type(
                row["roas_6h"], row["spend_6h"], row["purchases_6h"]
            )
            if action_type is None:
                action_type = "CREATIVE_EXPANSION"   # 게이트 통과 시 최소 분류

            print(f"[{action_type}] {ad_info}")
            print(f"  roas_6h={row['roas_6h']:.1%}  spend_6h={row['spend_6h']:,.0f}원"
                  f"  purchases_6h={int(row['purchases_6h'])}건"
                  f"  roas_12h={row['roas_12h']:.1%}  ctr_6h={row['ctr_6h']:.2%}")

            if not is_recently_alerted(row["AD_ID"]):
                repeat_count = get_repeat_count(row["AD_ID"])
                alert_data   = {
                    "action_type":   action_type,
                    "channel":       row.get("CHANNEL", "OFFICIAL"),
                    "campaign_name": row["CAMPAIGN_NAME"],
                    "adset_name":    row["ADSET_NAME"],
                    "ad_name":       row["AD_NAME"],
                    "ad_id":         row["AD_ID"],
                    "roas_6h":       row["roas_6h"],
                    "roas_12h":      row["roas_12h"],
                    "spend_6h":      row["spend_6h"],
                    "purchases_6h":  row["purchases_6h"],
                    "revenue_6h":    row["revenue_6h"],
                    "ctr_6h":        row["ctr_6h"],
                    "repeat_count":  repeat_count,
                }
                print(f"  -> Gemini 인사이트 생성 중...")
                insight, guide = generate_ai_insight(alert_data)
                alert_data["ai_insight"]   = insight
                alert_data["action_guide"] = guide
                print(f"  AI: {insight}")
                print(f"  가이드: {guide}")
                opp_alerts.append(alert_data)
            else:
                print(f"  -> 12시간 내 발송 이력 있음. 건너뜀.")

        # ── Kill Alert ──
        kl = KILL_CONDITION
        if row["roas_12h"] <= kl["roas_12h_max"] and row["spend_12h"] >= kl["spend_12h_min"]:
            print(f"[KILL] {ad_info}")
            print(f"  roas_12h={row['roas_12h']:.1%}  spend_12h={row['spend_12h']:,.0f}원")
            kill_found = True

    if not opp_alerts and not kill_found:
        print("  현재 alert 조건에 해당하는 광고 없음")

    print("=" * 65)

    if opp_alerts:
        send_alert_email(opp_alerts)
        for a in opp_alerts:
            mark_alert_sent(a["ad_id"])


# ─────────────────────────────────────────
# 메인 실행
# ─────────────────────────────────────────
if __name__ == "__main__":
    check_operating_hours()
    raw_data = fetch_insights()
    if not raw_data:
        print("[결과] 데이터 없음. 오늘 활성 광고가 없거나 API 오류.")
        exit(0)

    df = build_dataframe(raw_data)
    print(f"\n[결과] 전체 행 수: {len(df)}")
    print("\n[결과] 상위 5개 행:")
    print(df[["CHANNEL", "AD_NAME", "SPEND_CUM", "PURCHASES_CUM", "REVENUE_CUM"]].head())

    load_to_snowflake(df)
    evaluate_alerts(df)
