{{ config(materialized='table') }}

-- 🎯 핵심 스토리: 룰 기반 시스템이 놓친 사기 (isFraud=1 AND isFlaggedFraud=0).
-- 잔액 변화(orig_balance_drop / dest_balance_gain)를 함께 노출해 사기 패턴 분석을 돕는다.
SELECT
    row_id,
    tx_timestamp,
    tx_date,
    type,
    amount,
    nameOrig,
    oldbalanceOrg,
    newbalanceOrig,
    nameDest,
    oldbalanceDest,
    newbalanceDest,
    isFraud,
    isFlaggedFraud,
    is_suspicious,
    (oldbalanceOrg - newbalanceOrig)  AS orig_balance_drop,
    (newbalanceDest - oldbalanceDest) AS dest_balance_gain
FROM {{ ref('stg_silver_transactions') }}
WHERE is_suspicious = TRUE
