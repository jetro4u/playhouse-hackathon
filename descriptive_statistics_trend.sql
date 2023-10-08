
CREATE TABLE descriptive_statistics_trends (
    metric VARCHAR(255),
    network VARCHAR(255),
    content_type VARCHAR(255),
    hour INT,
    day INT,
    month INT,
    year INT,
    total_value INT,
    mean_value NUMERIC(10, 1),
    median_value NUMERIC(10, 1),
    std_dev NUMERIC(10, 1),
    skewness NUMERIC(10, 1),
    kurtosis NUMERIC(10, 1),
    coefficient_of_variation NUMERIC(10, 1),
    range_value NUMERIC(10, 1),
    percentile_25 NUMERIC(10, 1),
    percentile_75 NUMERIC(10, 1)
);
INSERT INTO descriptive_statistics_trends
WITH MedianCalculations AS (
    SELECT
        network,
        content_type,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY likes) AS median_likes,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY comments) AS median_comments,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY shares) AS median_shares,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY engagements) AS median_engagements,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY impressions) AS median_impressions
    FROM rawdata_stage
    GROUP BY network, content_type
)
SELECT
    'Likes' AS metric,
    m.network,
    m.content_type,
    EXTRACT(HOUR FROM date_trunc('hour', date_column)) AS hour,
    EXTRACT(DAY FROM date_trunc('day', date_column)) AS day,
    EXTRACT(MONTH FROM date_trunc('month', date_column)) AS month,
    EXTRACT(YEAR FROM date_trunc('year', date_column)) AS year,
    SUM(likes) AS total_value,
    ROUND(AVG(r.likes)::numeric, 1) AS mean_value,
    ROUND(m.median_likes::numeric, 1) AS median_value,
    ROUND(STDDEV(r.likes)::numeric, 1) AS std_dev,
    CASE
        WHEN STDDEV(r.likes) = 0 THEN NULL
        ELSE ROUND(((3 * (AVG(r.likes) - m.median_likes)) / NULLIF(STDDEV(r.likes), 0))::numeric, 1)
    END AS skewness,
    CASE
        WHEN STDDEV(r.likes) = 0 OR POW(STDDEV(r.likes), 4) = 0 THEN NULL
        ELSE ROUND(((AVG(r.likes) - m.median_likes) / NULLIF(POW(STDDEV(r.likes), 4), 0))::numeric, 1)
    END AS kurtosis,
    ROUND(((NULLIF(STDDEV(r.likes), 0) / AVG(r.likes)) * 100)::numeric, 1) AS coefficient_of_variation,
    MAX(r.likes) - MIN(r.likes) AS range_value,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY r.likes) AS percentile_25,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY r.likes) AS percentile_75
FROM rawdata_stage r
INNER JOIN MedianCalculations m ON r.network = m.network AND r.content_type = m.content_type
GROUP BY m.network, m.content_type, m.median_likes, date_trunc('hour', date_column), date_trunc('day', date_column), date_trunc('month', date_column), date_trunc('year', date_column)
UNION ALL

-- Insert data into the new table for Comments
SELECT
    'Comments' AS metric,
    m.network,
    m.content_type,
    EXTRACT(HOUR FROM date_trunc('hour', date_column)) AS hour,
    EXTRACT(DAY FROM date_trunc('day', date_column)) AS day,
    EXTRACT(MONTH FROM date_trunc('month', date_column)) AS month,
    EXTRACT(YEAR FROM date_trunc('year', date_column)) AS year,
    SUM(comments) AS total_value,
    ROUND(AVG(r.comments)::numeric, 1) AS mean_value,
    ROUND(m.median_comments::numeric, 1) AS median_value,
    ROUND(STDDEV(r.comments)::numeric, 1) AS std_dev,
    CASE
        WHEN STDDEV(r.comments) = 0 THEN NULL
        ELSE ROUND(((3 * (AVG(r.comments) - m.median_comments)) / NULLIF(STDDEV(r.comments), 0))::numeric, 1)
    END AS skewness,
    CASE
        WHEN STDDEV(r.comments) = 0 OR POW(STDDEV(r.comments), 4) = 0 THEN NULL
        ELSE ROUND(((AVG(r.comments) - m.median_comments) / NULLIF(POW(STDDEV(r.comments), 4), 0))::numeric, 1)
    END AS kurtosis,
    ROUND(((NULLIF(STDDEV(r.comments), 0) / AVG(r.comments)) * 100)::numeric, 1) AS coefficient_of_variation,
    MAX(r.comments) - MIN(r.comments) AS range_value,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY r.comments) AS percentile_25,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY r.comments) AS percentile_75
FROM rawdata_stage r
INNER JOIN MedianCalculations m ON r.network = m.network AND r.content_type = m.content_type
GROUP BY m.network, m.content_type, m.median_comments, date_trunc('hour', date_column), date_trunc('day', date_column), date_trunc('month', date_column), date_trunc('year', date_column)
UNION ALL

-- Insert data into the new table for Shares
SELECT
    'Shares' AS metric,
    m.network,
    m.content_type,
    EXTRACT(HOUR FROM date_trunc('hour', date_column)) AS hour,
    EXTRACT(DAY FROM date_trunc('day', date_column)) AS day,
    EXTRACT(MONTH FROM date_trunc('month', date_column)) AS month,
    EXTRACT(YEAR FROM date_trunc('year', date_column)) AS year,
    SUM(shares) AS total_value,
    ROUND(AVG(r.shares)::numeric, 1) AS mean_value,
    ROUND(m.median_shares::numeric, 1) AS median_value,
    ROUND(STDDEV(r.shares)::numeric, 1) AS std_dev,
    CASE
        WHEN STDDEV(r.shares) = 0 THEN NULL
        ELSE ROUND(((3 * (AVG(r.shares) - m.median_shares)) / NULLIF(STDDEV(r.shares), 0))::numeric, 1)
    END AS skewness,
    CASE
        WHEN STDDEV(r.shares) = 0 OR POW(STDDEV(r.shares), 4) = 0 THEN NULL
        ELSE ROUND(((AVG(r.shares) - m.median_shares) / NULLIF(POW(STDDEV(r.shares), 4), 0))::numeric, 1)
    END AS kurtosis,
    ROUND(((NULLIF(STDDEV(r.shares), 0) / AVG(r.shares)) * 100)::numeric, 1) AS coefficient_of_variation,
    MAX(r.shares) - MIN(r.shares) AS range_value,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY r.shares) AS percentile_25,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY r.shares) AS percentile_75
FROM rawdata_stage r
INNER JOIN MedianCalculations m ON r.network = m.network AND r.content_type = m.content_type
GROUP BY m.network, m.content_type, m.median_shares, date_trunc('hour', date_column), date_trunc('day', date_column), date_trunc('month', date_column), date_trunc('year', date_column)
UNION ALL

-- Insert data into the new table for Engagements
SELECT
    'Engagements' AS metric,
    m.network,
    m.content_type,
    EXTRACT(HOUR FROM date_trunc('hour', date_column)) AS hour,
    EXTRACT(DAY FROM date_trunc('day', date_column)) AS day,
    EXTRACT(MONTH FROM date_trunc('month', date_column)) AS month,
    EXTRACT(YEAR FROM date_trunc('year', date_column)) AS year,
    SUM(engagements) AS total_value,
    ROUND(AVG(r.engagements)::numeric, 1) AS mean_value,
    ROUND(m.median_engagements::numeric, 1) AS median_value,
    ROUND(STDDEV(r.engagements)::numeric, 1) AS std_dev,
    CASE
        WHEN STDDEV(r.engagements) = 0 THEN NULL
        ELSE ROUND(((3 * (AVG(r.engagements) - m.median_engagements)) / NULLIF(STDDEV(r.engagements), 0))::numeric, 1)
    END AS skewness,
    CASE
        WHEN STDDEV(r.engagements) = 0 OR POW(STDDEV(r.engagements), 4) = 0 THEN NULL
        ELSE ROUND(((AVG(r.engagements) - m.median_engagements) / NULLIF(POW(STDDEV(r.engagements), 4), 0))::numeric, 1)
    END AS kurtosis,
    ROUND(((NULLIF(STDDEV(r.engagements), 0) / AVG(r.engagements)) * 100)::numeric, 1) AS coefficient_of_variation,
    MAX(r.engagements) - MIN(r.engagements) AS range_value,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY r.engagements) AS percentile_25,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY r.engagements) AS percentile_75
FROM rawdata_stage r
INNER JOIN MedianCalculations m ON r.network = m.network AND r.content_type = m.content_type
GROUP BY m.network, m.content_type, m.median_engagements, date_trunc('hour', date_column), date_trunc('day', date_column), date_trunc('month', date_column), date_trunc('year', date_column)
UNION ALL

-- Insert data into the new table for Impressions
SELECT
    'Impressions' AS metric,
    m.network,
    m.content_type,
    EXTRACT(HOUR FROM date_trunc('hour', date_column)) AS hour,
    EXTRACT(DAY FROM date_trunc('day', date_column)) AS day,
    EXTRACT(MONTH FROM date_trunc('month', date_column)) AS month,
    EXTRACT(YEAR FROM date_trunc('year', date_column)) AS year,
    SUM(impressions) AS total_value,
    ROUND(AVG(r.impressions)::numeric, 1) AS mean_value,
    ROUND(m.median_impressions::numeric, 1) AS median_value,
    ROUND(STDDEV(r.impressions)::numeric, 1) AS std_dev,
    CASE
        WHEN STDDEV(r.impressions) = 0 THEN NULL
        ELSE ROUND(((3 * (AVG(r.impressions) - m.median_impressions)) / NULLIF(STDDEV(r.impressions), 0))::numeric, 1)
    END AS skewness,
    CASE
        WHEN STDDEV(r.impressions) = 0 OR POW(STDDEV(r.impressions), 4) = 0 THEN NULL
        ELSE ROUND(((AVG(r.impressions) - m.median_impressions) / NULLIF(POW(STDDEV(r.impressions), 4), 0))::numeric, 1)
    END AS kurtosis,
    ROUND(((NULLIF(STDDEV(r.impressions), 0) / AVG(r.impressions)) * 100)::numeric, 1) AS coefficient_of_variation,
    MAX(r.impressions) - MIN(r.impressions) AS range_value,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY r.impressions) AS percentile_25,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY r.impressions) AS percentile_75
FROM rawdata_stage r
INNER JOIN MedianCalculations m ON r.network = m.network AND r.content_type = m.content_type
GROUP BY m.network, m.content_type, m.median_impressions, date_trunc('hour', date_column), date_trunc('day', date_column), date_trunc('month', date_column), date_trunc('year', date_column);

\copy (Select * From descriptive_statistics_trends) To '/tmp/descriptive_statistics_trends.csv' With CSV DELIMITER ',' HEADER

-- Create a new table to store the correlation coefficients
CREATE TABLE correlation_coefficient (
    metric1 VARCHAR(255),
    metric2 VARCHAR(255),
    correlation_coefficient NUMERIC(10, 4)
);