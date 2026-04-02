"""
Meta Ads 조기경보 시스템
- ASC 캠페인 구조 기반 action_type 분기
- Gemini AI 인사이트
- Office365 SMTP 이메일 발송
"""

import os
import re
import json
import unicodedata
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

GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL      = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

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
# Opportunity 공통 필터 (Performance 캠페인 전용 — BR은 BR_ALERT_CONDITIONS 사용)
OPP_FILTER = {
    "purchases_6h_min":   5,
    "spend_6h_min":       30_000,
    "roas_6h_min":        3.0,   # 300%
    # roas_6h >= roas_12h 는 코드에서 직접 비교
}

# action_type 분기 조건 (우선순위: CAMPAIGN_SCALE > PRODUCT_EXTRACTION > CREATIVE_EXPANSION)
# ASC 통합 구조 기준: ad_id 단위 개별 소재 예산 조정 불가 → 캠페인 일cap/일예산 조정만 가능
ACTION_CONDITIONS = {
    "CAMPAIGN_SCALE": {
        "roas_6h_min":      3.0,   # 300%
        "purchases_6h_min": 5,
        "guide": "전환 효율이 급증한 구간입니다. ASC 캠페인 일cap 상향을 검토하세요.",
    },
    "PRODUCT_EXTRACTION": {
        "roas_6h_min":  3.0,       # 300%
        "spend_6h_min": 100_000,
        "guide": "해당 소재 내 상품을 확인하여 동일 상품 기반 신규 소재 2~3종 추가 제작을 권장합니다.",
    },
    "CREATIVE_EXPANSION": {
        "roas_6h_min":      2.5,   # 250%
        "purchases_6h_min": 2,
        "guide": "해당 소재 내 상품을 확인하여 동일 상품 기반 신규 소재 2~3종 추가 제작을 권장합니다.",
    },
}

# BR(브랜딩) 캠페인 전용 알럿 조건 — 전환 지표 사용 안 함
BR_ALERT_CONDITIONS = {
    "impressions_6h_min": 10_000,
    "clicks_6h_min":      200,
}

# Kill Alert 조건
KILL_CONDITION = {
    "roas_12h_max":  1.2,     # 120%
    "spend_12h_min": 150_000,
}


# ─────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────
def dw(s: str) -> int:
    """터미널/슬랙 코드블록 기준 표시 너비 (CJK 2칸, 나머지 1칸)."""
    return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in str(s))


def rjust_dw(s: str, width: int) -> str:
    """display width 기준 우측 정렬 패딩."""
    return " " * max(0, width - dw(s)) + str(s)


def ljust_dw(s: str, width: int) -> str:
    """display width 기준 좌측 정렬 패딩."""
    return str(s) + " " * max(0, width - dw(s))


PURCHASE_ACTION_TYPES = [
    "omni_purchase",
    "offsite_conversion.fb_pixel_purchase",
    "purchase",
]


def extract_purchase_count(actions: list) -> int:
    if not actions:
        return 0
    action_map = {item.get("action_type"): item for item in actions}
    for atype in PURCHASE_ACTION_TYPES:
        if atype in action_map:
            return int(float(action_map[atype].get("value", 0)))
    return 0


def extract_purchase_revenue(action_values: list) -> float:
    if not action_values:
        return 0.0
    action_map = {item.get("action_type"): item for item in action_values}
    for atype in PURCHASE_ACTION_TYPES:
        if atype in action_map:
            return float(action_map[atype].get("value", 0.0))
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


def determine_alert_subtype(
    ctr_6h: float, ctr_12h: float,
    purchases_6h: float, roas_6h: float, roas_12h: float,
    purchases_prev_6h: float = 0, clicks_6h: float = 0,
    clicks_prev_6h: float = 0, roas_prev_6h: float = 0,
) -> str:
    """
    alert 성격 분류 (전환 단계 기준)
    CONVERSION_SURGE_COLD: 최근 6h 전환 >= 5건, clicks >= 100, 직전 6h 전환 == 0건 (첫 발생)
    CONVERSION_SURGE:      최근 6h 전환 >= 5건, clicks >= 100, CVR/ROAS 모두 직전 6h 대비 개선
    CONVERSION_EARLY:      1 <= 전환 < 5건, ROAS >= 기준치 (초기 전환 감지형)
    CLICK_TO_CONVERT_GAP:  1 <= 전환 < 5건, CTR 상승 + ROAS < 기준치 (전환 미흡형)
    CLICK_SURGE:           전환 0건, CTR_6h > CTR_12h (순수 클릭 반응형)
    DEFAULT:               위 조건에 해당하지 않는 경우
    """
    roas_threshold = OPP_FILTER["roas_6h_min"]

    # purchases >= 5: Winner 판단
    if purchases_6h >= 5 and clicks_6h >= 100:
        if purchases_prev_6h == 0:
            return "CONVERSION_SURGE_COLD"
        cvr_recent = purchases_6h      / clicks_6h      if clicks_6h      > 0 else 0
        cvr_prev   = purchases_prev_6h / clicks_prev_6h if clicks_prev_6h > 0 else 0
        if (
            (purchases_6h - purchases_prev_6h) >= 2
            and roas_6h > roas_prev_6h
            and cvr_recent > cvr_prev
        ):
            return "CONVERSION_SURGE"

    # purchases 1~4: 초기 전환 단계
    if 1 <= purchases_6h < 5:
        if roas_6h >= roas_threshold:
            return "CONVERSION_EARLY"
        if ctr_6h > ctr_12h:
            return "CLICK_TO_CONVERT_GAP"

    # purchases 0: 순수 클릭 반응
    if purchases_6h == 0 and ctr_6h > ctr_12h:
        return "CLICK_SURGE"

    return "DEFAULT"


def determine_br_subtype(ctr_6h: float, ctr_12h: float) -> str | None:
    """
    BR(브랜딩) 캠페인 전용 subtype 판정.
    CTR_SURGE: ctr_6h > ctr_12h
    CTR_DROP:  ctr_6h < ctr_12h * 0.8  (20% 이상 하락)
    None:      조건 미해당
    """
    if ctr_6h > ctr_12h:
        return "BR_CTR_SURGE"
    if ctr_12h > 0 and ctr_6h < ctr_12h * 0.8:
        return "BR_CTR_DROP"
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
# FALLBACK은 alert_subtype 기준으로 분기 (ASC 구조 전제)
FALLBACK = {
    # ── Performance 알럿 ──
    "CLICK_SURGE": (
        "클릭 반응이 급증한 소재로 썸네일·카피 반응이 좋은 구간입니다. 다만 전환은 아직 발생하지 않은 상태입니다.",
        "썸네일·카피 컨셉을 유지하고 랜딩 페이지 및 상품 적합성을 점검하세요. 전환 유도형 카피/오퍼 변형 테스트를 권장합니다.",
    ),
    "CONVERSION_EARLY": (
        "전환이 발생하며 ROAS가 기준치를 상회하는 초기 전환 신호입니다. 모수는 작지만 전환 가능성이 확인된 구간입니다.",
        "6시간 추가 관찰하여 ROAS 유지 여부를 확인하세요. 동일 썸네일·카피 컨셉 기반 유사 소재 2~3종 제작을 권장하며, 즉시 예산 증액은 보류합니다.",
    ),
    "CLICK_TO_CONVERT_GAP": (
        "클릭 반응과 일부 전환은 발생했으나 ROAS가 기준치에 미달하는 구간입니다. 유입 대비 전환 효율이 낮은 상태입니다.",
        "상세페이지, 가격, 혜택 등 전환 요소를 점검하세요. 썸네일 유지 + 카피/오퍼 중심으로 수정 테스트 후 재검증하세요.",
    ),
    "CONVERSION_SURGE": (
        "직전 6시간 대비 전환율과 ROAS가 모두 개선된 실질적 전환 급증 구간입니다.",
        "ASC 캠페인 일cap 상향을 검토하고, 해당 소재 내 상품 기반 전환형 신규 소재 2~3종 추가 제작을 권장합니다.",
    ),
    "CONVERSION_SURGE_COLD": (
        "직전 6시간 전환이 없다가 최근 6시간 내 첫 전환이 발생한 구간입니다. 지속성은 추가 관찰이 필요합니다.",
        "첫 전환 발생 구간이므로 일cap 상향은 보류하고, 해당 소재와 상품을 메모해두고 다음 6시간 추이를 확인하세요.",
    ),
    "DEFAULT": (
        "해당 소재의 6시간 성과가 기준치를 초과하여 기회 구간으로 판단됩니다.",
        "캠페인 일cap 상향을 검토하고, 해당 소재 내 상품으로 신규 소재 2~3종 추가 제작을 권장합니다.",
    ),
    # ── BR(브랜딩) 알럿 ──
    "BR_CTR_SURGE": (
        "브랜딩 캠페인의 CTR이 직전 대비 상승한 구간입니다. 썸네일·카피 반응이 좋아지고 있습니다.",
        "반응이 좋은 이 소재의 썸네일·카피 컨셉을 기반으로 유사 소재 2~3종 추가 제작을 권장합니다.",
    ),
    "BR_CTR_DROP": (
        "브랜딩 캠페인의 CTR이 직전 대비 20% 이상 하락한 구간입니다. 소재 피로도를 확인하세요.",
        "CTR이 하락한 소재는 새로운 썸네일·카피 컨셉으로 소재를 교체하거나 신규 소재 투입을 검토하세요.",
    ),
}


def generate_ai_insight(alert: dict) -> tuple[str, str]:
    action_type   = alert["action_type"]
    alert_subtype = alert.get("alert_subtype", "DEFAULT")
    fallback      = FALLBACK.get(alert_subtype, FALLBACK["DEFAULT"])

    if not _gemini_client:
        return fallback

    is_br = alert.get("alert_type") == "BR"

    # subtype별 Gemini 작성 지침
    subtype_context = {
        # ── Performance ──
        "CLICK_SURGE": (
            "AI 인사이트: CTR이 급등한 이유를 썸네일·카피 반응 관점에서 해석하세요. 전환은 아직 0건임을 반영하세요. "
            "ACTION 가이드: 랜딩 페이지 점검과 전환 유도형 카피/오퍼 변형 테스트를 안내하세요."
        ),
        "CONVERSION_EARLY": (
            "AI 인사이트: 소량이지만 전환이 발생하고 ROAS가 기준치를 상회하는 이유를 데이터 기반으로 해석하세요. "
            "ACTION 가이드: 6시간 추가 관찰, 유사 소재 2~3종 제작, 즉시 예산 증액 보류를 안내하세요."
        ),
        "CLICK_TO_CONVERT_GAP": (
            "AI 인사이트: 클릭은 발생했지만 전환 효율이 낮은 이유를 랜딩·상품·오퍼 관점에서 해석하세요. "
            "ACTION 가이드: 상세페이지·가격·혜택 점검과 카피/오퍼 수정 테스트를 안내하세요."
        ),
        "CONVERSION_SURGE": (
            "AI 인사이트: 직전 6시간 대비 전환율과 ROAS가 모두 개선된 이유를 데이터 기반으로 해석하세요. "
            "ACTION 가이드: ASC 캠페인 일cap 상향 검토와 해당 소재 내 상품 기반 전환형 신규 소재 2~3종 제작을 안내하세요."
        ),
        "CONVERSION_SURGE_COLD": (
            "AI 인사이트: 직전 6시간 전환이 없다가 첫 전환이 발생한 이유를 소재/상품 관점에서 해석하세요. "
            "ACTION 가이드: 첫 전환 발생이므로 일cap 상향은 보류하고, 다음 6시간 추이 관찰과 소재 저장을 안내하세요."
        ),
        "DEFAULT": (
            "AI 인사이트: 성과 개선 요인을 데이터 기반으로 해석하세요. "
            "ACTION 가이드: 캠페인 일cap 상향 검토와 소재 확장 방향으로 운영 액션을 안내하세요."
        ),
        # ── BR(브랜딩) ──
        "BR_CTR_SURGE": (
            "AI 인사이트: 브랜딩 소재의 CTR이 상승한 이유를 썸네일·카피 반응 관점에서 해석하세요. "
            "ACTION 가이드: 반응이 좋은 소재 컨셉을 기반으로 유사 소재 2~3종 추가 제작을 안내하세요. 전환 지표는 언급하지 마세요."
        ),
        "BR_CTR_DROP": (
            "AI 인사이트: 브랜딩 소재의 CTR이 하락한 이유를 소재 피로도·노출 포화 관점에서 해석하세요. "
            "ACTION 가이드: 새로운 썸네일·카피 컨셉으로 소재 교체 또는 신규 소재 투입을 안내하세요. 전환 지표는 언급하지 마세요."
        ),
    }

    if is_br:
        prompt = f"""
당신은 디지털 광고 브랜딩 마케터입니다.
아래 Meta 브랜딩 광고 데이터를 보고 AI 인사이트와 액션 가이드를 작성하세요.
전환(구매), ROAS, 매출 관련 내용은 절대 언급하지 마세요.

[광고 정보]
- 캠페인: {alert['campaign_name']}
- 광고세트: {alert['adset_name']}
- 광고소재: {alert['ad_name']}
- 채널: {alert['channel']}
- alert 유형: {alert_subtype}

[성과 데이터]
- Impressions_6h: {int(alert.get('impressions_6h', 0)):,}회
- Clicks_6h: {int(alert.get('clicks_6h', 0)):,}회
- CTR_6h: {alert.get('ctr_6h', 0):.2%} / CTR_12h: {alert.get('ctr_12h', 0):.2%}

[작성 지침]
- {subtype_context.get(alert_subtype, subtype_context['BR_CTR_SURGE'])}
- AI_INSIGHT는 "왜" CTR 변화가 발생했는지 한 문장으로 해석
- ACTION_GUIDE는 소재 확장·교체 중심으로 한~두 문장 구체적 지시
- 입력 데이터만 근거로 해석, 외부 요인 추정 금지
- 숫자 과장 금지, 한국어, 짧고 실무적인 톤

[출력 형식] (반드시 아래 형식 그대로)
AI_INSIGHT: (한 문장)
ACTION_GUIDE: (한~두 문장)
""".strip()
    else:
        prompt = f"""
당신은 디지털 광고 퍼포먼스 마케터입니다.
아래 Meta 광고 데이터를 보고 AI 인사이트(왜 반응이 좋아졌는지 해석)와 액션 가이드(운영자가 당장 해야 할 행동)를 각각 작성하세요.

[광고 정보]
- 캠페인: {alert['campaign_name']}
- 광고세트: {alert['adset_name']}
- 광고소재: {alert['ad_name']}
- 채널: {alert['channel']}
- alert 유형: {alert_subtype} / {action_type}

[성과 데이터]
- Spend_6h: {alert['spend_6h']:,.0f}원
- Clicks_6h: {int(alert.get('clicks_6h', 0))}회
- Purchases_6h: {int(alert['purchases_6h'])}건
- Revenue_6h: {alert['revenue_6h']:,.0f}원
- ROAS_6h: {alert['roas_6h']:.1%} / ROAS_12h: {alert['roas_12h']:.1%}
- CTR_6h: {alert.get('ctr_6h', 0):.2%} / CTR_12h: {alert.get('ctr_12h', 0):.2%}

[작성 지침]
- {subtype_context.get(alert_subtype, subtype_context['DEFAULT'])}
- AI_INSIGHT는 "왜" 반응이 좋아졌는지 한 문장으로 해석
- ACTION_GUIDE는 운영자가 "무엇을" 해야 하는지 한~두 문장으로 구체적 지시
- 입력 데이터만 근거로 해석, 외부 요인 추정 금지
- 숫자 과장 금지, 한국어, 짧고 실무적인 톤

[출력 형식] (반드시 아래 형식 그대로)
AI_INSIGHT: (한 문장)
ACTION_GUIDE: (한~두 문장)
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
        alert_subtype = a.get("alert_subtype", "DEFAULT")
        is_br         = a.get("alert_type") == "BR"
        repeat_label  = f"{a['repeat_count']}회" if a["repeat_count"] > 1 else "첫 발생"
        ctr_6h        = a.get("ctr_6h", 0)
        ctr_12h       = a.get("ctr_12h", 0)
        ctr_diff_pp   = (ctr_6h - ctr_12h) * 100
        clicks_6h     = int(a.get("clicks_6h", 0))

        # ── BR 브랜딩 알럿 블록 ──
        if is_br:
            br_color      = "#8e44ad" if alert_subtype == "BR_CTR_SURGE" else "#e74c3c"
            subtype_label = "CTR 상승형" if alert_subtype == "BR_CTR_SURGE" else "CTR 하락형"
            impressions_6h = int(a.get("impressions_6h", 0))
            ctr_diff_color = "#27ae60" if ctr_diff_pp >= 0 else "#e74c3c"
            blocks += f"""
        <div style="border:1px solid #e0e0e0;border-radius:8px;padding:20px;margin-bottom:24px;">
          <div style="display:flex;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:4px;">
            <span style="background:{br_color};color:#fff;padding:4px 10px;border-radius:4px;
                         font-size:12px;font-weight:bold;">BR 브랜딩</span>
            <span style="background:#6c757d;color:#fff;padding:3px 8px;border-radius:4px;
                         font-size:11px;font-weight:bold;margin-left:6px;">{subtype_label}</span>
            <span style="margin-left:auto;color:#999;font-size:12px;">최근 7일 {repeat_label}</span>
          </div>
          {(
              '<div style="margin-bottom:14px;text-align:center;">'
              f'<img src="{a["creative_image_url"]}" width="250" '
              'style="max-width:100%;border-radius:6px;border:1px solid #e0e0e0;" '
              'alt="소재 이미지" />'
              '</div>'
          ) if a.get("creative_image_url") else ""}
          <table style="width:100%;font-size:13px;border-collapse:collapse;margin-bottom:12px;">
            <tr><td style="padding:4px 8px;color:#888;width:110px;">Campaign</td>
                <td style="padding:4px 8px;font-family:monospace;font-size:12px;">{a['campaign_name']}</td></tr>
            <tr style="background:#f9f9f9;"><td style="padding:4px 8px;color:#888;">Ad Set</td>
                <td style="padding:4px 8px;font-family:monospace;font-size:12px;">{a['adset_name']}</td></tr>
            <tr><td style="padding:4px 8px;color:#888;">Creative</td>
                <td style="padding:4px 8px;font-family:monospace;font-size:12px;">{a['ad_name']}</td></tr>
            <tr style="background:#f9f9f9;"><td style="padding:4px 8px;color:#888;">Ad ID</td>
                <td style="padding:4px 8px;color:#555;font-size:12px;">{a['ad_id']}</td></tr>
          </table>
          <h4 style="margin:12px 0 8px;color:#333;font-size:13px;">최근 6시간 성과 (브랜딩 지표)</h4>
          <table style="border-collapse:collapse;width:100%;font-size:13px;">
            <thead>
              <tr style="background:#f0f4ff;">
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:left;">지표</th>
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:right;">현재값</th>
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:right;">12h 대비</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">Impressions_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{impressions_6h:,}회</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">Clicks_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{clicks_6h:,}회</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">CTR_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{ctr_6h:.2%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;
                    color:{ctr_diff_color};font-weight:bold;">{'+' if ctr_diff_pp >= 0 else ''}{ctr_diff_pp:.1f}%p</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">CTR_12h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{ctr_12h:.2%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
            </tbody>
          </table>
          <div style="margin-top:10px;padding:12px;background:#f0f7ff;border-left:4px solid {br_color};border-radius:4px;">
            <p style="margin:0 0 4px;font-size:11px;color:#888;font-weight:bold;">AI 인사이트</p>
            <p style="margin:0;font-size:13px;color:#333;">{a['ai_insight']}</p>
          </div>
          <div style="margin-top:8px;padding:12px;background:#fff8e1;border-left:4px solid #f9a825;border-radius:4px;">
            <p style="margin:0 0 4px;font-size:11px;color:#888;font-weight:bold;">액션 가이드</p>
            <p style="margin:0;font-size:13px;color:#333;">{a['action_guide']}</p>
          </div>
        </div>
        """
            continue

        # ── Performance 알럿 블록 ──
        action_type   = a["action_type"]
        color         = ACTION_TYPE_COLOR.get(action_type, "#1a73e8")
        action_ko     = ACTION_TYPE_KO.get(action_type, action_type)

        # 기준치 계산
        roas_base   = ACTION_CONDITIONS[action_type]["roas_6h_min"]
        purch_base  = ACTION_CONDITIONS[action_type].get("purchases_6h_min", OPP_FILTER["purchases_6h_min"])
        roas_diff_pp  = (a["roas_6h"] - roas_base) * 100
        purch_diff    = int(a["purchases_6h"]) - purch_base

        # alert_subtype 뱃지 텍스트
        subtype_label = {
            "CLICK_SURGE":           "클릭 급증형",
            "CONVERSION_EARLY":      "초기 전환 감지형",
            "CLICK_TO_CONVERT_GAP":  "전환 미흡형",
            "CONVERSION_SURGE":      "전환 급증형",
            "CONVERSION_SURGE_COLD": "첫 전환 급등형",
            "DEFAULT":               "",
        }.get(alert_subtype, "")

        subtype_badge = (
            f'<span style="background:#6c757d;color:#fff;padding:3px 8px;border-radius:4px;'
            f'font-size:11px;font-weight:bold;margin-left:6px;">{subtype_label}</span>'
            if subtype_label else ""
        )

        blocks += f"""
        <div style="border:1px solid #e0e0e0;border-radius:8px;padding:20px;margin-bottom:24px;">
          <div style="display:flex;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:4px;">
            <span style="background:{color};color:#fff;padding:4px 10px;border-radius:4px;
                         font-size:12px;font-weight:bold;">{action_type}</span>
            {subtype_badge}
            <span style="color:#666;font-size:13px;margin-left:6px;">{action_ko}</span>
            <span style="margin-left:auto;color:#999;font-size:12px;">최근 7일 {repeat_label}</span>
          </div>

          {(
              '<div style="margin-bottom:14px;text-align:center;">'
              f'<img src="{a["creative_image_url"]}" width="250" '
              'style="max-width:100%;border-radius:6px;border:1px solid #e0e0e0;" '
              'alt="소재 이미지" />'
              '</div>'
          ) if a.get("creative_image_url") else ""}

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
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:right;">기준치</th>
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:right;">현재값</th>
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:right;">기준 대비</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">Spend_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">100,000원</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{a['spend_6h']:,.0f}원</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#27ae60;font-weight:bold;">+{(a['spend_6h']/100_000-1)*100:.0f}%</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">Clicks_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{clicks_6h:,}회</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">Purchases_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">{purch_base}건</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{int(a['purchases_6h'])}건</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#27ae60;font-weight:bold;">+{purch_diff}건</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">Revenue_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{a['revenue_6h']:,.0f}원</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">ROAS_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">{roas_base:.0%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:{color};font-weight:bold;">{a['roas_6h']:.1%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#27ae60;font-weight:bold;">+{roas_diff_pp:.0f}%p</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">ROAS_12h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{a['roas_12h']:.1%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">CTR_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">CTR_12h 기준</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{ctr_6h:.2%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:{'#27ae60' if ctr_diff_pp >= 0 else '#e74c3c'};font-weight:bold;">{'+' if ctr_diff_pp >= 0 else ''}{ctr_diff_pp:.1f}%p</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">CTR_12h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{ctr_12h:.2%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
            </tbody>
          </table>

          <div style="margin-top:12px;padding:12px;background:#f8f9fa;border:1px solid #dee2e6;border-radius:6px;">
            <p style="margin:0 0 8px;font-size:12px;font-weight:bold;color:#495057;">기준 대비 상승폭</p>
            <p style="margin:0 0 4px;font-size:13px;color:#333;">
              &#8226; ROAS: 기준 {roas_base:.0%} 대비
              <strong style="color:{color};">+{roas_diff_pp:.0f}%p</strong>
              ({a['roas_6h']:.1%} vs {roas_base:.0%})
            </p>
            <p style="margin:0 0 4px;font-size:13px;color:#333;">
              &#8226; 구매건수: 기준 {purch_base}건 대비
              <strong style="color:{color};">+{purch_diff}건</strong>
              ({int(a['purchases_6h'])}건 vs {purch_base}건)
            </p>
            <p style="margin:0;font-size:13px;color:#333;">
              &#8226; CTR: 12시간 대비
              <strong style="color:{'#27ae60' if ctr_diff_pp >= 0 else '#e74c3c'};">{'+' if ctr_diff_pp >= 0 else ''}{ctr_diff_pp:.1f}%p</strong>
              (CTR_6h: {ctr_6h:.2%} vs CTR_12h: {ctr_12h:.2%})
            </p>
          </div>

          <div style="margin-top:10px;padding:12px;background:#f0f7ff;border-left:4px solid {color};border-radius:4px;">
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
# Slack 알림
# ─────────────────────────────────────────
def send_slack_alert(alerts: list) -> None:
    if not SLACK_WEBHOOK_URL:
        print("[경고] SLACK_WEBHOOK_URL 없음 - 슬랙 발송 건너뜀")
        return

    for a in alerts:
        alert_subtype = a.get("alert_subtype", "DEFAULT")
        is_br         = a.get("alert_type") == "BR"
        repeat_label  = f"{a['repeat_count']}회" if a["repeat_count"] > 1 else "첫 발생"
        ctr_6h        = a.get("ctr_6h", 0)
        ctr_12h       = a.get("ctr_12h", 0)
        ctr_diff_pp   = (ctr_6h - ctr_12h) * 100

        if is_br:
            # ── BR 브랜딩 슬랙 블록 ──
            br_color      = "#8e44ad" if alert_subtype == "BR_CTR_SURGE" else "#e74c3c"
            subtype_label = "CTR 상승형" if alert_subtype == "BR_CTR_SURGE" else "CTR 하락형"
            header_text   = f":bar_chart: *BR 브랜딩 Alert* — `{subtype_label}`"
            blocks = [
                {"type": "header", "text": {"type": "plain_text", "text": f"Meta Ads BR 브랜딩 Alert · {BRAND}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": header_text}},
                {"type": "section", "fields": [
                    {"type": "mrkdwn", "text": f"*분류*\nBR 브랜딩  ·  최근 7일 {repeat_label}"},
                    {"type": "mrkdwn", "text": f"*채널*\n{a['channel']}"},
                    {"type": "mrkdwn", "text": f"*Campaign*\n`{a['campaign_name']}`"},
                    {"type": "mrkdwn", "text": f"*Ad Set*\n`{a['adset_name']}`"},
                    {"type": "mrkdwn", "text": f"*Creative*\n`{a['ad_name']}`"},
                    {"type": "mrkdwn", "text": f"*Ad ID*\n`{a['ad_id']}`"},
                ]},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": (
                    "*최근 6시간 브랜딩 지표*\n"
                    "```\n" +
                    ljust_dw("지표", 16) + rjust_dw("현재값", 12) + rjust_dw("12h 대비", 12) + "\n" +
                    "─" * 42 + "\n" +
                    ljust_dw("Impressions_6h", 16) + rjust_dw(f"{int(a.get('impressions_6h',0)):,}회", 12) + rjust_dw("─", 12) + "\n" +
                    ljust_dw("Clicks_6h", 16)      + rjust_dw(f"{int(a.get('clicks_6h',0)):,}회", 12)     + rjust_dw("─", 12) + "\n" +
                    ljust_dw("CTR_6h", 16)         + rjust_dw(f"{ctr_6h:.2%}", 12)                        + rjust_dw(('+' if ctr_diff_pp>=0 else '')+f"{ctr_diff_pp:.1f}%p", 12) + "\n" +
                    ljust_dw("CTR_12h", 16)        + rjust_dw(f"{ctr_12h:.2%}", 12)                       + rjust_dw("─", 12) + "\n" +
                    "```"
                )}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f":bulb: *AI 인사이트*\n{a.get('ai_insight', '')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f":dart: *액션 가이드*\n{a.get('action_guide', '')}"}},
            ]
        else:
            # ── Performance 슬랙 블록 ──
            action_type   = a["action_type"]
            color         = ACTION_TYPE_COLOR.get(action_type, "#1a73e8")
            action_ko     = ACTION_TYPE_KO.get(action_type, action_type)
            roas_base     = ACTION_CONDITIONS[action_type]["roas_6h_min"]
            purch_base    = ACTION_CONDITIONS[action_type].get("purchases_6h_min", OPP_FILTER["purchases_6h_min"])
            roas_diff_pp  = (a["roas_6h"] - roas_base) * 100
            purch_diff    = int(a["purchases_6h"]) - purch_base
            subtype_label = {
                "CLICK_SURGE":           "클릭 급증형",
                "CONVERSION_EARLY":      "초기 전환 감지형",
                "CLICK_TO_CONVERT_GAP":  "전환 미흡형",
                "CONVERSION_SURGE":      "전환 급증형",
                "CONVERSION_SURGE_COLD": "첫 전환 급등형",
            }.get(alert_subtype, "")
            header_text = f":mega: *Opportunity Alert* — {action_type}" + (f"  `{subtype_label}`" if subtype_label else "")
            blocks = [
                {"type": "header", "text": {"type": "plain_text", "text": f"Meta Ads Opportunity Alert · {BRAND}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": header_text}},
                {"type": "section", "fields": [
                    {"type": "mrkdwn", "text": f"*분류*\n{action_ko}  ·  최근 7일 {repeat_label}"},
                    {"type": "mrkdwn", "text": f"*채널*\n{a['channel']}"},
                    {"type": "mrkdwn", "text": f"*Campaign*\n`{a['campaign_name']}`"},
                    {"type": "mrkdwn", "text": f"*Ad Set*\n`{a['adset_name']}`"},
                    {"type": "mrkdwn", "text": f"*Creative*\n`{a['ad_name']}`"},
                    {"type": "mrkdwn", "text": f"*Ad ID*\n`{a['ad_id']}`"},
                ]},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": (
                    "*최근 6시간 성과*\n"
                    "```\n" +
                    ljust_dw("지표", 16) + rjust_dw("기준", 11) + rjust_dw("현재값", 13) + rjust_dw("대비", 11) + "\n" +
                    "─" * 53 + "\n" +
                    ljust_dw("Spend_6h", 16)    + rjust_dw("100,000원", 11) + rjust_dw(f"{a['spend_6h']:,.0f}원", 13)              + rjust_dw("─", 11) + "\n" +
                    ljust_dw("Clicks_6h", 16)   + rjust_dw("─", 11)         + rjust_dw(f"{int(a.get('clicks_6h',0)):,}회", 13)    + rjust_dw("─", 11) + "\n" +
                    ljust_dw("Purchases_6h", 16)+ rjust_dw(f"{purch_base}건", 11) + rjust_dw(f"{int(a['purchases_6h'])}건", 13)   + rjust_dw(('+' if purch_diff>=0 else '')+f"{purch_diff}건", 11) + "\n" +
                    ljust_dw("Revenue_6h", 16)  + rjust_dw("─", 11)         + rjust_dw(f"{a['revenue_6h']:,.0f}원", 13)           + rjust_dw("─", 11) + "\n" +
                    ljust_dw("ROAS_6h", 16)     + rjust_dw(f"{roas_base:.0%}", 11) + rjust_dw(f"{a['roas_6h']:.1%}", 13)         + rjust_dw(('+' if roas_diff_pp>=0 else '')+f"{roas_diff_pp:.0f}%p", 11) + "\n" +
                    ljust_dw("ROAS_12h", 16)    + rjust_dw("─", 11)         + rjust_dw(f"{a['roas_12h']:.1%}", 13)               + rjust_dw("─", 11) + "\n" +
                    ljust_dw("CTR_6h", 16)      + rjust_dw("12h기준", 11)   + rjust_dw(f"{ctr_6h:.2%}", 13)                      + rjust_dw(('+' if ctr_diff_pp>=0 else '')+f"{ctr_diff_pp:.1f}%p", 11) + "\n" +
                    ljust_dw("CTR_12h", 16)     + rjust_dw("─", 11)         + rjust_dw(f"{ctr_12h:.2%}", 13)                     + rjust_dw("─", 11) + "\n" +
                    "```"
                )}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": (
                    "*기준 대비 상승폭*\n"
                    f"• ROAS: 기준 {roas_base:.0%} 대비 *{'+' if roas_diff_pp>=0 else ''}{roas_diff_pp:.0f}%p* ({a['roas_6h']:.1%} vs {roas_base:.0%})\n"
                    f"• 구매건수: 기준 {purch_base}건 대비 *{'+' if purch_diff>=0 else ''}{purch_diff}건* ({int(a['purchases_6h'])}건 vs {purch_base}건)\n"
                    f"• CTR: 12시간 대비 *{'+' if ctr_diff_pp>=0 else ''}{ctr_diff_pp:.1f}%p* (CTR_6h: {ctr_6h:.2%} vs CTR_12h: {ctr_12h:.2%})"
                )}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f":bulb: *AI 인사이트*\n{a.get('ai_insight', '')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f":dart: *액션 가이드*\n{a.get('action_guide', '')}"}},
            ]

        # 소재 이미지가 있으면 상단에 추가
        if a.get("creative_image_url"):
            blocks.insert(2, {
                "type": "image",
                "image_url": a["creative_image_url"],
                "alt_text": a["ad_name"],
            })

        try:
            resp = requests.post(
                SLACK_WEBHOOK_URL,
                json={"blocks": blocks},
                timeout=10,
            )
            if resp.status_code == 200:
                print(f"[완료] 슬랙 발송 성공 -> {a['ad_name']}")
            else:
                print(f"[오류] 슬랙 발송 실패 (HTTP {resp.status_code}): {resp.text}")
        except Exception as e:
            print(f"[오류] 슬랙 발송 실패: {e}")


# ─────────────────────────────────────────
# Meta API 호출
# ─────────────────────────────────────────
def fetch_creative_image(ad_id: str) -> str:
    """
    ad_id 기준으로 소재 이미지 URL 반환.
    우선순위: thumbnail_url > image_url > image_hash → adimages API
    조회 실패 또는 이미지 없으면 빈 문자열 반환.
    """
    try:
        resp = requests.get(
            f"https://graph.facebook.com/{API_VERSION}/{ad_id}",
            params={
                "access_token": ACCESS_TOKEN,
                "fields": "creative{thumbnail_url,image_url,image_hash}",
            },
            timeout=10,
        )
        if resp.status_code != 200:
            return ""
        creative = resp.json().get("creative", {})

        if creative.get("thumbnail_url"):
            return creative["thumbnail_url"]
        if creative.get("image_url"):
            return creative["image_url"]

        image_hash = creative.get("image_hash")
        if image_hash:
            img_resp = requests.get(
                f"https://graph.facebook.com/{API_VERSION}/{AD_ACCOUNT_ID}/adimages",
                params={
                    "access_token": ACCESS_TOKEN,
                    "hashes": image_hash,
                    "fields": "url",
                },
                timeout=10,
            )
            if img_resp.status_code == 200:
                data = img_resp.json().get("data", [])
                if data and data[0].get("url"):
                    return data[0]["url"]

        return ""
    except Exception as e:
        print(f"[경고] creative 이미지 조회 실패 (ad_id: {ad_id}): {e}")
        return ""


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

    # DEBUG: 전체 광고에서 purchase 관련 action_type 스캔
    all_action_types = set()
    purchase_found = []
    for item in raw_data:
        for a in item.get("actions", []):
            all_action_types.add(a.get("action_type"))
        for a in item.get("action_values", []):
            all_action_types.add(a.get("action_type"))
        if any("purchase" in str(a.get("action_type", "")).lower() for a in item.get("actions", [])):
            purchase_found.append({
                "ad_id": item.get("ad_id"),
                "actions": [(a["action_type"], a.get("value")) for a in item.get("actions", []) if "purchase" in a.get("action_type","").lower()],
                "action_values": [(a["action_type"], a.get("value")) for a in item.get("action_values", []) if "purchase" in a.get("action_type","").lower()],
            })
    print("[DEBUG] 전체 action_types:", sorted(all_action_types))
    print(f"[DEBUG] purchase 포함 광고 수: {len(purchase_found)}")
    for p in purchase_found[:3]:
        print(f"  ad_id={p['ad_id']} actions={p['actions']} values={p['action_values']}")

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

    # ── prev_6h (직전 6시간 = 12h델타 - 6h델타) 계산 ──
    df_now["purchases_prev_6h"] = (df_now["purchases_12h"] - df_now["purchases_6h"]).clip(lower=0)
    df_now["clicks_prev_6h"]    = (df_now["clicks_12h"]    - df_now["clicks_6h"]).clip(lower=0)
    df_now["spend_prev_6h"]     = (df_now["spend_12h"]     - df_now["spend_6h"]).clip(lower=0)
    df_now["revenue_prev_6h"]   = (df_now["revenue_12h"]   - df_now["revenue_6h"]).clip(lower=0)
    df_now["roas_prev_6h"] = df_now.apply(
        lambda r: r["revenue_prev_6h"] / r["spend_prev_6h"] if r["spend_prev_6h"] > 0 else 0,
        axis=1,
    )

    # ── Alert 판단 ──
    print("\n" + "=" * 65)
    print("  ALERT 리포트")
    print("=" * 65)

    opp_alerts = []
    br_alerts  = []
    kill_found = False

    for _, row in df_now.iterrows():
        ad_info          = f"[{row.get('CHANNEL','OFFICIAL')}] {row['AD_NAME']} (ad_id: {row['AD_ID']})"
        campaign_tokens  = re.split(r'[\s_\-|/]+', str(row.get("CAMPAIGN_NAME", "")))
        is_br_campaign   = "BR" in [t.upper() for t in campaign_tokens]

        # ════════════════════════════════════
        # BR 브랜딩 캠페인 분기
        # ════════════════════════════════════
        if is_br_campaign:
            bc = BR_ALERT_CONDITIONS
            br_gate = (
                row["impressions_6h"] >= bc["impressions_6h_min"]
                and row["clicks_6h"]  >= bc["clicks_6h_min"]
            )
            if not br_gate:
                continue

            br_subtype = determine_br_subtype(row["ctr_6h"], row["ctr_12h"])
            if br_subtype is None:
                continue

            print(f"[BR/{br_subtype}] {ad_info}")
            print(f"  impressions_6h={int(row['impressions_6h']):,}  clicks_6h={int(row['clicks_6h']):,}"
                  f"  ctr_6h={row['ctr_6h']:.2%}  ctr_12h={row['ctr_12h']:.2%}")

            if not is_recently_alerted(row["AD_ID"]):
                repeat_count       = get_repeat_count(row["AD_ID"])
                creative_image_url = fetch_creative_image(row["AD_ID"])
                br_alert_data = {
                    "alert_type":         "BR",
                    "action_type":        "BR",
                    "alert_subtype":      br_subtype,
                    "channel":            row.get("CHANNEL", "OFFICIAL"),
                    "campaign_name":      row["CAMPAIGN_NAME"],
                    "adset_name":         row["ADSET_NAME"],
                    "ad_name":            row["AD_NAME"],
                    "ad_id":              row["AD_ID"],
                    "impressions_6h":     row["impressions_6h"],
                    "clicks_6h":          row["clicks_6h"],
                    "ctr_6h":             row["ctr_6h"],
                    "ctr_12h":            row["ctr_12h"],
                    "repeat_count":       repeat_count,
                    "creative_image_url": creative_image_url,
                }
                print(f"  -> Gemini 인사이트 생성 중...")
                insight, guide = generate_ai_insight(br_alert_data)
                br_alert_data["ai_insight"]   = insight
                br_alert_data["action_guide"] = guide
                print(f"  AI: {insight}")
                print(f"  가이드: {guide}")
                br_alerts.append(br_alert_data)
            else:
                print(f"  -> 12시간 내 발송 이력 있음. 건너뜀.")
            continue

        # ════════════════════════════════════
        # Performance 캠페인 분기
        # ════════════════════════════════════

        # ── Opportunity Alert 공통 진입 조건 ──
        roas_improving = row["roas_6h"] >= row["roas_12h"]
        opp_gate = (
            row["roas_6h"]      >= OPP_FILTER["roas_6h_min"]
            and row["spend_6h"] >= OPP_FILTER["spend_6h_min"]
            and row["purchases_6h"] >= OPP_FILTER["purchases_6h_min"]
            and roas_improving
        )

        if opp_gate:
            action_type = determine_action_type(
                row["roas_6h"], row["spend_6h"], row["purchases_6h"]
            )
            if action_type is None:
                action_type = "CREATIVE_EXPANSION"

            _subtype_preview = determine_alert_subtype(
                row["ctr_6h"], row["ctr_12h"],
                row["purchases_6h"], row["roas_6h"], row["roas_12h"],
                row["purchases_prev_6h"], row["clicks_6h"],
                row["clicks_prev_6h"], row["roas_prev_6h"],
            )
            print(f"[{action_type}/{_subtype_preview}] {ad_info}")
            print(f"  roas_6h={row['roas_6h']:.1%}  spend_6h={row['spend_6h']:,.0f}원"
                  f"  purchases_6h={int(row['purchases_6h'])}건  purchases_prev_6h={int(row['purchases_prev_6h'])}건"
                  f"  roas_prev_6h={row['roas_prev_6h']:.1%}"
                  f"  ctr_6h={row['ctr_6h']:.2%}  ctr_12h={row['ctr_12h']:.2%}")

            if not is_recently_alerted(row["AD_ID"]):
                repeat_count  = get_repeat_count(row["AD_ID"])
                alert_subtype = determine_alert_subtype(
                    row["ctr_6h"], row["ctr_12h"],
                    row["purchases_6h"], row["roas_6h"], row["roas_12h"],
                    row["purchases_prev_6h"], row["clicks_6h"],
                    row["clicks_prev_6h"], row["roas_prev_6h"],
                )
                print(f"  -> 소재 이미지 조회 중...")
                creative_image_url = fetch_creative_image(row["AD_ID"])
                if creative_image_url:
                    print(f"  -> 이미지 확보: {creative_image_url[:60]}...")
                else:
                    print(f"  -> 이미지 없음 (파트너십 광고 또는 영상 소재)")

                alert_data = {
                    "alert_type":         "PERFORMANCE",
                    "action_type":        action_type,
                    "alert_subtype":      alert_subtype,
                    "channel":            row.get("CHANNEL", "OFFICIAL"),
                    "campaign_name":      row["CAMPAIGN_NAME"],
                    "adset_name":         row["ADSET_NAME"],
                    "ad_name":            row["AD_NAME"],
                    "ad_id":              row["AD_ID"],
                    "roas_6h":            row["roas_6h"],
                    "roas_12h":           row["roas_12h"],
                    "roas_prev_6h":       row["roas_prev_6h"],
                    "spend_6h":           row["spend_6h"],
                    "purchases_6h":       row["purchases_6h"],
                    "purchases_prev_6h":  row["purchases_prev_6h"],
                    "revenue_6h":         row["revenue_6h"],
                    "clicks_6h":          row["clicks_6h"],
                    "clicks_prev_6h":     row["clicks_prev_6h"],
                    "ctr_6h":             row["ctr_6h"],
                    "ctr_12h":            row["ctr_12h"],
                    "repeat_count":       repeat_count,
                    "creative_image_url": creative_image_url,
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

    if not opp_alerts and not br_alerts and not kill_found:
        print("  현재 alert 조건에 해당하는 광고 없음")

    print("=" * 65)

    all_alerts = opp_alerts + br_alerts
    if all_alerts:
        send_alert_email(all_alerts)
        send_slack_alert(all_alerts)
        for a in all_alerts:
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
