# Meta 조기경보 시스템 — SERGIO TACCHINI

## 프로젝트 개요
Meta Ads API에서 광고 성과를 매시간 수집해 Snowflake에 적재하고, 6h/12h 롤링 윈도우 기준으로 이상 신호를 감지하면 슬랙·이메일로 알럿을 발송하는 시스템.

- **브랜드**: SERGIO_TACCHINI
- **Meta 광고 계정**: act_743386137406824
- **실행 스케줄**: 매시 정각, KST 07:00~01:00 (UTC 22:00~16:00), 새벽 01:00~07:00 자동 스킵

---

## 핵심 파일
| 파일 | 역할 |
|---|---|
| `app.py` | 전체 로직 단일 파일 |
| `.github/workflows/meta_alert.yml` | GitHub Actions 스케줄 실행 |
| `requirements.txt` | Python 패키지 목록 |
| `alert_sent_log.json` | 중복 발송 방지 로그 (런타임 생성, 12시간 중복 차단) |

---

## 실행 흐름
```
check_operating_hours()       # 새벽 시간 체크
→ fetch_insights()            # Meta API에서 오늘 광고 성과 수집 (ad 레벨)
→ build_dataframe()           # DataFrame 변환, BRAND/CHANNEL 컬럼 추가
→ load_to_snowflake()         # META_AD_SNAPSHOT 테이블에 적재
→ evaluate_alerts()           # 6h/12h 델타 계산 → 알럿 판단 → 발송
```

---

## 알럿 종류 및 조건

### Performance 알럿 (Opportunity Alert)
공통 진입 조건: `roas_6h >= 300%` AND `spend_6h >= 10,000원` AND `purchases_6h >= 2건` AND `roas_6h >= roas_12h`

| action_type | 조건 | 의미 |
|---|---|---|
| CAMPAIGN_SCALE | roas_6h >= 300%, purchases_6h >= 3건 | 캠페인 일cap 상향 검토 |
| PRODUCT_EXTRACTION | roas_6h >= 300%, spend_6h >= 50,000원 | 상품 기반 신규 소재 제작 |
| CREATIVE_EXPANSION | roas_6h >= 250%, purchases_6h >= 2건 | 동일 상품 소재 확장 |

우선순위: CAMPAIGN_SCALE > PRODUCT_EXTRACTION > CREATIVE_EXPANSION

### alert_subtype (성격 분류)
- `CONVERSION_SURGE_COLD`: 최근 6h 전환 >= 3건, 직전 6h 전환 = 0 (첫 발생)
- `CONVERSION_SURGE`: 최근 6h 전환 >= 3건, CVR/ROAS 모두 직전 6h 대비 개선
- `CLICK_SURGE`: CTR_6h > CTR_12h * 1.2, 클릭 >= 50회
- `CONVERSION_EARLY`: 구매 >= 2건, ROAS >= 400%
- `CLICK_TO_CONVERT_GAP`: 클릭 급증 but 구매 미흡

### BR 브랜딩 알럿
캠페인명에 "BR" 토큰 포함 시 분기. 전환 지표 미사용.
- 진입 조건: impressions_6h >= 10,000 AND clicks_6h >= 200
- `BR_CTR_SURGE`: CTR_6h > CTR_12h * 1.1
- `BR_CTR_DROP`: CTR_6h < CTR_12h * 0.9

### Kill Alert (콘솔 출력만, 발송 없음)
roas_12h <= 120% AND spend_12h >= 150,000원

---

## 채널 감지 (detect_channel)
캠페인명/광고세트명에 "무신사" 또는 "musinsa" 포함 → `MUSINSA`, 나머지 → `OFFICIAL`

---

## Snowflake 구조
- **적재 테이블**: `FNF.ORG_PF.META_AD_SNAPSHOT`
- **조회 방식**: 현재 스냅샷에서 6h/12h 전 스냅샷의 누적값을 빼서 델타 계산
- **브랜드 필터**: `WHERE brand = 'SERGIO_TACCHINI'`
- **재고 조회**: `FNF.PRCS.DW_SCS_DACUM` (물류재고), `FNF.PRCS.DW_SH_SCS_D` (자사몰 판매)
- **재고 브랜드 코드**: `ST`

---

## 알림 발송
- **슬랙**: 단일 채널 (`SLACK_WEBHOOK_URL`)
- **이메일**: Office365 SMTP, `ALERT_RECIPIENTS` 수신자 목록
- **중복 차단**: `alert_sent_log.json` 기준 동일 ad_id 12시간 내 재발송 차단

---

## AI 인사이트
Gemini API (`gemini-2.0-flash`)로 각 알럿에 한국어 인사이트 + 액션 가이드 생성.

---

## 환경변수 (GitHub Secrets)
```
META_ACCESS_TOKEN, META_AD_ACCOUNT_ID, META_API_VERSION
SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY
SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_ROLE, SNOWFLAKE_TABLE
SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, ALERT_RECIPIENTS
GEMINI_API_KEY, GEMINI_MODEL
SLACK_WEBHOOK_URL
```

---

## 주의사항
- 테스트 메일 발송 시 campaign/adset/ad 이름은 반드시 Meta API 실제 조회값 사용 (임의 작성 금지)
- `alert_sent_log.json`은 GitHub Actions cache로 실행 간 유지됨
- ASC 캠페인 구조상 ad_id 단위 소재 예산 조정 불가 → 캠페인 일cap 조정만 가능
