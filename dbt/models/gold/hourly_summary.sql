{{ config(materialized='table') }}

-- 시간대별 거래량/사기 집계. 거래 유형(type)별로도 분리.
SELECT
    DATE_TRUNC('hour', tx_timestamp)                   AS tx_hour,
    type,
    COUNT(*)                                           AS tx_count,
    SUM(amount)                                        AS total_amount,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END)       AS fraud_count,
    SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END)     AS suspicious_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END)  AS fraud_amount
FROM {{ ref('stg_silver_transactions') }}
GROUP BY 1, 2
ORDER BY 1, 2
