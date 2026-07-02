{{ config(materialized='table') }}

-- 계좌(nameOrig)별 누적 위험도. 사기에 1건이라도 연루된 계좌만(HAVING) 남긴다.
SELECT
    nameOrig                                           AS account_id,
    COUNT(*)                                           AS total_tx_count,
    SUM(amount)                                        AS total_amount,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END)       AS fraud_tx_count,
    SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END)     AS suspicious_tx_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END)  AS fraud_amount,
    ROUND(
        SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2
    )                                                  AS fraud_rate_pct,
    MAX(tx_timestamp)                                  AS last_tx_at
FROM {{ ref('stg_silver_transactions') }}
GROUP BY 1
HAVING SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) > 0
ORDER BY fraud_amount DESC
