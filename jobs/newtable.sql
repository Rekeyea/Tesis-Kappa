INSERT INTO gdnews2_scores
SELECT * 
FROM (
    WITH respiratory_rate_window AS (
        SELECT 
            patient_id,
            TUMBLE_START(measurement_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(measurement_timestamp, INTERVAL '1' MINUTE) AS window_end,
            AVG(measured_value) as respiratory_rate_value,
            COUNT(*) as measurement_count,
            MIN(ingestion_timestamp) as ingestion_timestamp,
            MIN(measurement_status) as measurement_status,
            AVG(raw_news2_score) as raw_news2_score,
            AVG(adjusted_score) as adjusted_score,
            AVG(confidence) as confidence,
            MIN(measurement_timestamp) as measurement_timestamp,
            MIN(enrichment_timestamp) as enrichment_timestamp,
            MIN(routing_timestamp) as routing_timestamp,
            MIN(scoring_timestamp) as scoring_timestamp
        FROM scores_respiratory_rate
        GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)
    ),
    oxygen_saturation_window AS (
        SELECT 
            patient_id,
            TUMBLE_START(measurement_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(measurement_timestamp, INTERVAL '1' MINUTE) AS window_end,
            AVG(measured_value) as oxygen_saturation_value,
            COUNT(*) as measurement_count,
            MIN(ingestion_timestamp) as ingestion_timestamp,
            MIN(measurement_status) as measurement_status,
            AVG(raw_news2_score) as raw_news2_score,
            AVG(adjusted_score) as adjusted_score,
            AVG(confidence) as confidence,
            MIN(measurement_timestamp) as measurement_timestamp,
            MIN(enrichment_timestamp) as enrichment_timestamp,
            MIN(routing_timestamp) as routing_timestamp,
            MIN(scoring_timestamp) as scoring_timestamp
        FROM scores_oxygen_saturation
        GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)
    ),
    blood_pressure_window AS (
        SELECT 
            patient_id,
            TUMBLE_START(measurement_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(measurement_timestamp, INTERVAL '1' MINUTE) AS window_end,
            AVG(measured_value) as blood_pressure_value,
            COUNT(*) as measurement_count,
            MIN(ingestion_timestamp) as ingestion_timestamp,
            MIN(measurement_status) as measurement_status,
            AVG(raw_news2_score) as raw_news2_score,
            AVG(adjusted_score) as adjusted_score,
            AVG(confidence) as confidence,
            MIN(measurement_timestamp) as measurement_timestamp,
            MIN(enrichment_timestamp) as enrichment_timestamp,
            MIN(routing_timestamp) as routing_timestamp,
            MIN(scoring_timestamp) as scoring_timestamp
        FROM scores_blood_pressure_systolic
        GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)
    ),
    heart_rate_window AS (
        SELECT 
            patient_id,
            TUMBLE_START(measurement_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(measurement_timestamp, INTERVAL '1' MINUTE) AS window_end,
            AVG(measured_value) as heart_rate_value,
            COUNT(*) as measurement_count,
            MIN(ingestion_timestamp) as ingestion_timestamp,
            MIN(measurement_status) as measurement_status,
            AVG(raw_news2_score) as raw_news2_score,
            AVG(adjusted_score) as adjusted_score,
            AVG(confidence) as confidence,
            MIN(measurement_timestamp) as measurement_timestamp,
            MIN(enrichment_timestamp) as enrichment_timestamp,
            MIN(routing_timestamp) as routing_timestamp,
            MIN(scoring_timestamp) as scoring_timestamp
        FROM scores_heart_rate
        GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)
    ),
    temperature_window AS (
        SELECT 
            patient_id,
            TUMBLE_START(measurement_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(measurement_timestamp, INTERVAL '1' MINUTE) AS window_end,
            AVG(measured_value) as temperature_value,
            COUNT(*) as measurement_count,
            MIN(ingestion_timestamp) as ingestion_timestamp,
            MIN(measurement_status) as measurement_status,
            AVG(raw_news2_score) as raw_news2_score,
            AVG(adjusted_score) as adjusted_score,
            AVG(confidence) as confidence,
            MIN(measurement_timestamp) as measurement_timestamp,
            MIN(enrichment_timestamp) as enrichment_timestamp,
            MIN(routing_timestamp) as routing_timestamp,
            MIN(scoring_timestamp) as scoring_timestamp
        FROM scores_temperature
        GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)
    ),
    consciousness_window AS (
        SELECT 
            patient_id,
            TUMBLE_START(measurement_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(measurement_timestamp, INTERVAL '1' MINUTE) AS window_end,
            MIN(measured_value) as consciousness_value,
            COUNT(*) as measurement_count,
            MIN(ingestion_timestamp) as ingestion_timestamp,
            MIN(measurement_status) as measurement_status,
            AVG(raw_news2_score) as raw_news2_score,
            AVG(adjusted_score) as adjusted_score,
            AVG(confidence) as confidence,
            MIN(measurement_timestamp) as measurement_timestamp,
            MIN(enrichment_timestamp) as enrichment_timestamp,
            MIN(routing_timestamp) as routing_timestamp,
            MIN(scoring_timestamp) as scoring_timestamp
        FROM scores_consciousness
        GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)
    )
    
    SELECT 
        COALESCE(rr.patient_id, os.patient_id, bp_val.patient_id, hr.patient_id, temp.patient_id, cons.patient_id) AS patient_id,
        COALESCE(rr.window_start, os.window_start, bp_val.window_start, hr.window_start, temp.window_start, cons.window_start) AS window_start,
        COALESCE(rr.window_end, os.window_end, bp_val.window_end, hr.window_end, temp.window_end, cons.window_end) AS window_end,
        -- Raw measurements
        MAX(rr.respiratory_rate_value) as respiratory_rate_value,
        MAX(os.oxygen_saturation_value) as oxygen_saturation_value,
        MAX(bp_val.blood_pressure_value) as blood_pressure_value,
        MAX(hr.heart_rate_value) as heart_rate_value,
        MAX(temp.temperature_value) as temperature_value,
        MAX(cons.consciousness_value) as consciousness_value,
        
        -- Raw NEWS2 scores
        MAX(rr.raw_news2_score) as respiratory_rate_score,
        MAX(os.raw_news2_score) as oxygen_saturation_score,
        MAX(bp_val.raw_news2_score) as blood_pressure_score,
        MAX(hr.raw_news2_score) as heart_rate_score,
        MAX(temp.raw_news2_score) as temperature_score,
        MAX(cons.raw_news2_score) as consciousness_score,
        
        -- Calculate raw NEWS2 total
        (COALESCE(MAX(rr.raw_news2_score), 0) + 
        COALESCE(MAX(os.raw_news2_score), 0) + 
        COALESCE(MAX(bp_val.raw_news2_score), 0) + 
        COALESCE(MAX(hr.raw_news2_score), 0) + 
        COALESCE(MAX(temp.raw_news2_score), 0) + 
        COALESCE(MAX(cons.raw_news2_score), 0)) as raw_news2_total,
        
        -- Adjusted scores
        MAX(rr.adjusted_score) as adjusted_respiratory_rate_score,
        MAX(os.adjusted_score) as adjusted_oxygen_saturation_score,
        MAX(bp_val.adjusted_score) as adjusted_blood_pressure_score,
        MAX(hr.adjusted_score) as adjusted_heart_rate_score,
        MAX(temp.adjusted_score) as adjusted_temperature_score,
        MAX(cons.adjusted_score) as adjusted_consciousness_score,
        
        -- Calculate gdNEWS2 total
        (COALESCE(MAX(rr.adjusted_score), 0) + 
        COALESCE(MAX(os.adjusted_score), 0) + 
        COALESCE(MAX(bp_val.adjusted_score), 0) + 
        COALESCE(MAX(hr.adjusted_score), 0) + 
        COALESCE(MAX(temp.adjusted_score), 0) + 
        COALESCE(MAX(cons.adjusted_score), 0)) as gdnews2_total,
        
        -- Quality and confidence
        (COALESCE(MAX(rr.confidence), 0) + 
        COALESCE(MAX(os.confidence), 0) + 
        COALESCE(MAX(bp_val.confidence), 0) + 
        COALESCE(MAX(hr.confidence), 0) + 
        COALESCE(MAX(temp.confidence), 0) + 
        COALESCE(MAX(cons.confidence), 0)) / 6 as overall_confidence,
        
        -- Status counts
        (CASE WHEN MAX(rr.measurement_status) = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(os.measurement_status) = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(bp_val.measurement_status) = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(hr.measurement_status) = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(temp.measurement_status) = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(cons.measurement_status) = 'VALID' THEN 1 ELSE 0 END) as valid_parameters,
        
        (CASE WHEN MAX(rr.measurement_status) = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN MAX(os.measurement_status) = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN MAX(bp_val.measurement_status) = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN MAX(hr.measurement_status) = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN MAX(temp.measurement_status) = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN MAX(cons.measurement_status) = 'DEGRADED' THEN 1 ELSE 0 END) as degraded_parameters,
        
        (CASE WHEN MAX(rr.measurement_status) = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(os.measurement_status) = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(bp_val.measurement_status) = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(hr.measurement_status) = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(temp.measurement_status) = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(cons.measurement_status) = 'INVALID' THEN 1 ELSE 0 END) as invalid_parameters,
        
        -- Timestamps
        MIN(COALESCE(
            rr.measurement_timestamp,
            os.measurement_timestamp,
            bp_val.measurement_timestamp,
            hr.measurement_timestamp,
            temp.measurement_timestamp,
            cons.measurement_timestamp
        )) as measurement_timestamp,
        
        MIN(COALESCE(
            cons.ingestion_timestamp,
            temp.ingestion_timestamp,
            hr.ingestion_timestamp,
            bp_val.ingestion_timestamp,
            os.ingestion_timestamp,
            rr.ingestion_timestamp
        )) as ingestion_timestamp,
        
        MIN(COALESCE(
            cons.enrichment_timestamp,
            temp.enrichment_timestamp,
            hr.enrichment_timestamp,
            bp_val.enrichment_timestamp,
            os.enrichment_timestamp,
            rr.enrichment_timestamp
        )) as enrichment_timestamp,
        
        MIN(COALESCE(
            cons.routing_timestamp,
            temp.routing_timestamp,
            hr.routing_timestamp,
            bp_val.routing_timestamp,
            os.routing_timestamp,
            rr.routing_timestamp
        )) as routing_timestamp,
        
        MIN(COALESCE(
            cons.scoring_timestamp,
            temp.scoring_timestamp,
            hr.scoring_timestamp,
            bp_val.scoring_timestamp,
            os.scoring_timestamp,
            rr.scoring_timestamp
        )) as scoring_timestamp,
        
        MAX(CAST(LOCALTIMESTAMP AS TIMESTAMP(3))) as aggregation_timestamp
    FROM respiratory_rate_window rr
    FULL JOIN oxygen_saturation_window os
        ON rr.patient_id = os.patient_id
        AND rr.window_start = os.window_start
        AND rr.window_end = os.window_end
    FULL JOIN blood_pressure_window bp_val
        ON rr.patient_id = bp_val.patient_id
        AND rr.window_start = bp_val.window_start
        AND rr.window_end = bp_val.window_end
    FULL JOIN heart_rate_window hr
        ON rr.patient_id = hr.patient_id
        AND rr.window_start = hr.window_start
        AND rr.window_end = hr.window_end
    FULL JOIN temperature_window temp
        ON rr.patient_id = temp.patient_id
        AND rr.window_start = temp.window_start
        AND rr.window_end = temp.window_end
    FULL JOIN consciousness_window cons
        ON rr.patient_id = cons.patient_id
        AND rr.window_start = cons.window_start
        AND rr.window_end = cons.window_end
    GROUP BY
        COALESCE(rr.patient_id, os.patient_id, bp_val.patient_id, hr.patient_id, temp.patient_id, cons.patient_id),
        COALESCE(rr.window_start, os.window_start, bp_val.window_start, hr.window_start, temp.window_start, cons.window_start),
        COALESCE(rr.window_end, os.window_end, bp_val.window_end, hr.window_end, temp.window_end, cons.window_end)
) v;