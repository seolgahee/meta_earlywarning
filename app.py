"""
Meta Ads 조기경보 시스템 - API 연결 테스트
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv

# .env 파일에서 환경변수 로드
load_dotenv()

# ─────────────────────────────────────────
# 설정값 로드
# ─────────────────────────────────────────
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID")  # act_XXXXXXXX 형식
API_VERSION = os.getenv("META_API_VERSION", "v19.0")

# 토큰 누락 확인
if not ACCESS_TOKEN or not AD_ACCOUNT_ID:
    print("[오류] .env 파일에 META_ACCESS_TOKEN, META_AD_ACCOUNT_ID 값이 없습니다.")
    print("       .env 파일을 확인하고 다시 실행해 주세요.")
    exit(1)


# ─────────────────────────────────────────
# actions / action_values에서 purchase 값 추출
# ─────────────────────────────────────────
def extract_purchase_count(actions: list) -> int:
    """actions 리스트에서 purchase(구매) 횟수 반환"""
    if not actions:
        return 0
    for item in actions:
        if item.get("action_type") == "purchase":
            return int(float(item.get("value", 0)))
    return 0


def extract_purchase_revenue(action_values: list) -> float:
    """action_values 리스트에서 purchase(구매) 매출 반환"""
    if not action_values:
        return 0.0
    for item in action_values:
        if item.get("action_type") == "purchase":
            return float(item.get("value", 0.0))
    return 0.0


# ─────────────────────────────────────────
# Meta API 호출
# ─────────────────────────────────────────
def fetch_insights() -> list:
    """
    /{ad_account_id}/insights 엔드포인트 호출
    오늘 날짜 기준 / 광고(ad) 레벨 데이터 반환
    """
    url = f"https://graph.facebook.com/{API_VERSION}/{AD_ACCOUNT_ID}/insights"

    params = {
        "access_token": ACCESS_TOKEN,
        "level": "ad",
        "date_preset": "today",
        "fields": ",".join([
            "campaign_id",
            "campaign_name",
            "adset_id",
            "adset_name",
            "ad_id",
            "ad_name",
            "impressions",
            "clicks",
            "spend",
            "actions",
            "action_values",
        ]),
        "limit": 500,  # 한 번에 가져올 최대 행 수
    }

    all_data = []

    print(f"[정보] Meta API 호출 중... (버전: {API_VERSION})")

    while url:
        response = requests.get(url, params=params)

        # HTTP 오류 확인
        if response.status_code != 200:
            error_info = response.json().get("error", {})
            print(f"[오류] API 호출 실패 (HTTP {response.status_code})")
            print(f"       메시지: {error_info.get('message', '알 수 없는 오류')}")
            print(f"       코드:   {error_info.get('code', '-')}")
            return []

        body = response.json()

        # 데이터 수집
        all_data.extend(body.get("data", []))

        # 페이지네이션 처리 (다음 페이지가 있으면 계속 호출)
        next_url = body.get("paging", {}).get("next")
        url = next_url
        params = {}  # next URL에는 파라미터가 이미 포함됨

    return all_data


# ─────────────────────────────────────────
# 데이터 가공 → DataFrame
# ─────────────────────────────────────────
def build_dataframe(raw_data: list) -> pd.DataFrame:
    """API 응답 데이터를 pandas DataFrame으로 변환"""
    rows = []

    for item in raw_data:
        row = {
            "campaign_id":      item.get("campaign_id"),
            "campaign_name":    item.get("campaign_name"),
            "adset_id":         item.get("adset_id"),
            "adset_name":       item.get("adset_name"),
            "ad_id":            item.get("ad_id"),
            "ad_name":          item.get("ad_name"),
            "impressions":      int(item.get("impressions", 0)),
            "clicks":           int(item.get("clicks", 0)),
            "spend":            float(item.get("spend", 0.0)),
            "purchase_count":   extract_purchase_count(item.get("actions", [])),
            "purchase_revenue": extract_purchase_revenue(item.get("action_values", [])),
        }
        rows.append(row)

    return pd.DataFrame(rows)


# ─────────────────────────────────────────
# 메인 실행
# ─────────────────────────────────────────
if __name__ == "__main__":
    # 1. API 호출
    raw_data = fetch_insights()

    if not raw_data:
        print("[결과] 데이터가 없습니다. 오늘 활성화된 광고가 없거나 API 오류가 발생했습니다.")
        exit(0)

    # 2. DataFrame 변환
    df = build_dataframe(raw_data)

    # 3. 결과 출력
    print(f"\n[결과] 전체 행 수: {len(df)}")
    print("\n[결과] 상위 5개 행:")
    print(df.head())
