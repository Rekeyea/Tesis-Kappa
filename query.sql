-- Comprehensive query for gdNEWS2 patient monitoring dashboard
WITH patient_trends AS (
    SELECT
        patient_id,
        window_start,
        window_end,
        -- Calculate trends over 5-minute windows
        AVG(gdnews2_total) OVER (
            PARTITION BY patient_id
            ORDER BY window_start
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
            ) as gdnews2_trend,
        LAG(gdnews2_total, 5, 0) OVER (
            PARTITION BY patient_id
            ORDER BY window_start
            ) as previous_gdnews2
    FROM kappa.gdnews2_scores
    WHERE window_start >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
)

SELECT
    gs.patient_id,
    gs.window_start as measurement_time,

    -- Vital Signs with Clinical Context
    ROUND(gs.respiratory_rate_value, 1) as respiratory_rate,
    CASE
        WHEN gs.respiratory_rate_value <= 8 THEN 'Severe Bradypnea'
        WHEN gs.respiratory_rate_value <= 11 THEN 'Mild Bradypnea'
        WHEN gs.respiratory_rate_value <= 20 THEN 'Normal'
        WHEN gs.respiratory_rate_value <= 24 THEN 'Mild Tachypnea'
        ELSE 'Severe Tachypnea'
        END as respiratory_status,

    ROUND(gs.oxygen_saturation_value, 1) as spo2,
    CASE
        WHEN gs.oxygen_saturation_value <= 91 THEN 'Severe Hypoxemia'
        WHEN gs.oxygen_saturation_value <= 93 THEN 'Moderate Hypoxemia'
        WHEN gs.oxygen_saturation_value <= 95 THEN 'Mild Hypoxemia'
        ELSE 'Normal'
        END as oxygenation_status,

    ROUND(gs.blood_pressure_value, 1) as systolic_bp,
    CASE
        WHEN gs.blood_pressure_value <= 90 THEN 'Severe Hypotension'
        WHEN gs.blood_pressure_value <= 100 THEN 'Moderate Hypotension'
        WHEN gs.blood_pressure_value <= 110 THEN 'Mild Hypotension'
        WHEN gs.blood_pressure_value <= 219 THEN 'Normal'
        ELSE 'Severe Hypertension'
        END as bp_status,

    ROUND(gs.heart_rate_value, 1) as heart_rate,
    CASE
        WHEN gs.heart_rate_value <= 40 THEN 'Severe Bradycardia'
        WHEN gs.heart_rate_value <= 50 THEN 'Mild Bradycardia'
        WHEN gs.heart_rate_value <= 90 THEN 'Normal'
        WHEN gs.heart_rate_value <= 110 THEN 'Mild Tachycardia'
        WHEN gs.heart_rate_value <= 130 THEN 'Moderate Tachycardia'
        ELSE 'Severe Tachycardia'
        END as heart_rate_status,

    ROUND(gs.temperature_value, 1) as temperature,
    CASE
        WHEN gs.temperature_value <= 35.0 THEN 'Severe Hypothermia'
        WHEN gs.temperature_value <= 36.0 THEN 'Mild Hypothermia'
        WHEN gs.temperature_value <= 38.0 THEN 'Normal'
        WHEN gs.temperature_value <= 39.0 THEN 'Mild Fever'
        ELSE 'High Fever'
        END as temperature_status,

    gs.consciousness_value,

    -- Scoring Information
    ROUND(gs.raw_news2_total, 1) as raw_news2_score,
    ROUND(gs.gdnews2_total, 1) as gdnews2_score,

    -- Clinical Risk Categories
    CASE
        WHEN gs.gdnews2_total >= 7 THEN 'High Risk'
        WHEN gs.gdnews2_total >= 5 THEN 'Medium Risk'
        WHEN gs.gdnews2_total >= 3 THEN 'Low Risk'
        ELSE 'Minimal Risk'
        END as risk_category,

    -- Trend Analysis
    ROUND(pt.gdnews2_trend, 1) as score_trend_5min,
    CASE
        WHEN (pt.gdnews2_trend - pt.previous_gdnews2) > 2 THEN 'Rapidly Deteriorating'
        WHEN (pt.gdnews2_trend - pt.previous_gdnews2) > 0 THEN 'Deteriorating'
        WHEN (pt.gdnews2_trend - pt.previous_gdnews2) < -2 THEN 'Rapidly Improving'
        WHEN (pt.gdnews2_trend - pt.previous_gdnews2) < 0 THEN 'Improving'
        ELSE 'Stable'
        END as trend_status,

    -- Data Quality Metrics
    ROUND(gs.overall_confidence * 100, 1) as confidence_percentage,
    gs.valid_parameters,
    gs.degraded_parameters,
    gs.invalid_parameters,

    -- Quality Status for Each Parameter
    CASE
        WHEN gs.adjusted_respiratory_rate_score > 0 THEN 'Valid'
        ELSE 'Invalid'
        END as rr_quality,
    CASE
        WHEN gs.adjusted_oxygen_saturation_score > 0 THEN 'Valid'
        ELSE 'Invalid'
        END as spo2_quality,
    CASE
        WHEN gs.adjusted_blood_pressure_score > 0 THEN 'Valid'
        ELSE 'Invalid'
        END as bp_quality,
    CASE
        WHEN gs.adjusted_heart_rate_score > 0 THEN 'Valid'
        ELSE 'Invalid'
        END as hr_quality,
    CASE
        WHEN gs.adjusted_temperature_score > 0 THEN 'Valid'
        ELSE 'Invalid'
        END as temp_quality,
    CASE
        WHEN gs.adjusted_consciousness_score > 0 THEN 'Valid'
        ELSE 'Invalid'
        END as consciousness_quality,

    -- Timing Information
    TIMESTAMPDIFF(MINUTE, gs.measurement_timestamp, gs.flink_timestamp) as processing_latency_minutes,
    DATE_FORMAT(gs.measurement_timestamp, '%Y-%m-%d %H:%i:%s') as measurement_time_formatted,
    DATE_FORMAT(gs.flink_timestamp, '%Y-%m-%d %H:%i:%s') as processing_time_formatted

FROM kappa.gdnews2_scores gs
         LEFT JOIN patient_trends pt
                   ON gs.patient_id = pt.patient_id
                       AND gs.window_start = pt.window_start
ORDER BY
    gs.patient_id,
    gs.window_start DESC;