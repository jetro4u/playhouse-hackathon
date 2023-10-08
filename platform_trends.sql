/* calculate  noticeable trends over time (e.g., increasing likes, decreasing shares) */
/* By hour */
CREATE TABLE platform_hour_trends (
    id SERIAL PRIMARY KEY,
	network VARCHAR (100),
	fullhour timestamp,
    hour INT,
    total_likes BIGINT,
    total_shares BIGINT,
    total_comments BIGINT,
    total_saves BIGINT,
    total_engagements BIGINT,
    total_impressions BIGINT,
    mean_likes DECIMAL (9,2),
    mean_shares DECIMAL (9,2),
    mean_comments DECIMAL (9,2),
    mean_saves DECIMAL (9,2),
    mean_engagements DECIMAL (9,2),
    mean_impressions DECIMAL (9,2),
    sd_likes DECIMAL (9,2),
    sd_shares DECIMAL (9,2),
    sd_comments DECIMAL (9,2),
    sd_saves DECIMAL (9,2),
    sd_engagements DECIMAL (9,2),
    sd_impressions DECIMAL (9,2)
)
INSERT INTO platform_hour_trends (
	network,
	fullhour,
    hour,
    total_likes,
    total_shares,
    total_comments,
    total_saves,
    total_engagements,
    total_impressions,
    mean_likes,
    mean_shares,
    mean_comments,
    mean_saves,
    mean_engagements,
    mean_impressions,
    sd_likes,
    sd_shares,
    sd_comments,
    sd_saves,
    sd_engagements,
    sd_impressions
)
SELECT
	network,
    date_trunc('hour', date_column) AS fullhour,
    EXTRACT(HOUR FROM date_trunc('hour', date_column)) AS hour,
    SUM(likes) AS total_likes,
    SUM(shares) AS total_shares,
    SUM(comments) AS total_comments,
    SUM(saves) AS total_saves,
    SUM(engagements) AS total_engagements,
    SUM(impressions) AS total_impressions,
    ROUND(SUM(likes) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_likes,
    ROUND(SUM(shares) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_shares,
    ROUND(SUM(comments) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_comments,
    ROUND(SUM(saves) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_saves,
    ROUND(SUM(engagements) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_engagements,
    ROUND(SUM(impressions) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_impressions,
    ROUND(STDDEV(likes)::numeric, 2) AS sd_likes,
    ROUND(STDDEV(shares)::numeric, 2) AS  sd_shares,
    ROUND(STDDEV(comments)::numeric, 2) AS  sd_comments,
    ROUND(STDDEV(saves)::numeric, 2) AS  sd_saves,
    ROUND(STDDEV(engagements)::numeric, 2) AS  sd_engagements,
    ROUND(STDDEV(impressions)::numeric, 2) AS  sd_impressions
FROM
    rawdata_stage
WHERE
    date_column BETWEEN '1/15/2013' AND '7/13/2023'
GROUP BY
    network, date_trunc('hour', date_column)
ORDER BY
    hour;

\copy (Select * From platform_hour_trends) To '/tmp/platform_hour_trends.csv' With CSV DELIMITER ',' HEADER

/* By day */
CREATE TABLE platform_day_trends (
    id SERIAL PRIMARY KEY,
	network VARCHAR (100),
	fullday timestamp,
    day INT,
    total_likes BIGINT,
    total_shares BIGINT,
    total_comments BIGINT,
    total_saves BIGINT,
    total_engagements BIGINT,
    total_impressions BIGINT,
    mean_likes DECIMAL (9,2),
    mean_shares DECIMAL (9,2),
    mean_comments DECIMAL (9,2),
    mean_saves DECIMAL (9,2),
    mean_engagements DECIMAL (9,2),
    mean_impressions DECIMAL (9,2),
    sd_likes DECIMAL (9,2),
    sd_shares DECIMAL (9,2),
    sd_comments DECIMAL (9,2),
    sd_saves DECIMAL (9,2),
    sd_engagements DECIMAL (9,2),
    sd_impressions DECIMAL (9,2)
)
INSERT INTO platform_day_trends (
	network,
	fullday,
    day,
    total_likes,
    total_shares,
    total_comments,
    total_saves,
    total_engagements,
    total_impressions,
    mean_likes,
    mean_shares,
    mean_comments,
    mean_saves,
    mean_engagements,
    mean_impressions,
    sd_likes,
    sd_shares,
    sd_comments,
    sd_saves,
    sd_engagements,
    sd_impressions
)
SELECT
	network,
    date_trunc('day', date_column) AS fullday,
    EXTRACT(DAY FROM date_trunc('day', date_column)) AS day,
    SUM(likes) AS total_likes,
    SUM(shares) AS total_shares,
    SUM(comments) AS total_comments,
    SUM(saves) AS total_saves,
    SUM(engagements) AS total_engagements,
    SUM(impressions) AS total_impressions,
    ROUND(SUM(likes) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_likes,
    ROUND(SUM(shares) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_shares,
    ROUND(SUM(comments) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_comments,
    ROUND(SUM(saves) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_saves,
    ROUND(SUM(engagements) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_engagements,
    ROUND(SUM(impressions) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_impressions,
    ROUND(STDDEV(likes)::numeric, 2) AS sd_likes,
    ROUND(STDDEV(shares)::numeric, 2) AS  sd_shares,
    ROUND(STDDEV(comments)::numeric, 2) AS  sd_comments,
    ROUND(STDDEV(saves)::numeric, 2) AS  sd_saves,
    ROUND(STDDEV(engagements)::numeric, 2) AS  sd_engagements,
    ROUND(STDDEV(impressions)::numeric, 2) AS  sd_impressions
FROM
    rawdata_stage
WHERE
    date_column BETWEEN '1/15/2013' AND '7/13/2023'
GROUP BY
    network, date_trunc('day', date_column)
ORDER BY
    fullday;

\copy (Select * From platform_day_trends) To '/tmp/platform_day_trends.csv' With CSV DELIMITER ',' HEADER

/* by month */
CREATE TABLE platform_month_trends (
    id SERIAL PRIMARY KEY,
	network VARCHAR (100),
	fullmonth timestamp,
    month INT,
    total_likes BIGINT,
    total_shares BIGINT,
    total_comments BIGINT,
    total_saves BIGINT,
    total_engagements BIGINT,
    total_impressions BIGINT,
    mean_likes DECIMAL (9,2),
    mean_shares DECIMAL (9,2),
    mean_comments DECIMAL (9,2),
    mean_saves DECIMAL (9,2),
    mean_engagements DECIMAL (9,2),
    mean_impressions DECIMAL (9,2),
    sd_likes DECIMAL (9,2),
    sd_shares DECIMAL (9,2),
    sd_comments DECIMAL (9,2),
    sd_saves DECIMAL (9,2),
    sd_engagements DECIMAL (9,2),
    sd_impressions DECIMAL (9,2)
)
INSERT INTO platform_month_trends (
	network,
	fullmonth,
    month,
    total_likes,
    total_shares,
    total_comments,
    total_saves,
    total_engagements,
    total_impressions,
    mean_likes,
    mean_shares,
    mean_comments,
    mean_saves,
    mean_engagements,
    mean_impressions,
    sd_likes,
    sd_shares,
    sd_comments,
    sd_saves,
    sd_engagements,
    sd_impressions
)
SELECT
	network,
    date_trunc('month', date_column) AS fullmonth,
    EXTRACT(MONTH FROM date_trunc('month', date_column)) AS month,
    SUM(likes) AS total_likes,
    SUM(shares) AS total_shares,
    SUM(comments) AS total_comments,
    SUM(saves) AS total_saves,
    SUM(engagements) AS total_engagements,
    SUM(impressions) AS total_impressions,
    ROUND(SUM(likes) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_likes,
    ROUND(SUM(shares) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_shares,
    ROUND(SUM(comments) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_comments,
    ROUND(SUM(saves) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_saves,
    ROUND(SUM(engagements) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_engagements,
    ROUND(SUM(impressions) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_impressions,
    ROUND(STDDEV(likes)::numeric, 2) AS sd_likes,
    ROUND(STDDEV(shares)::numeric, 2) AS  sd_shares,
    ROUND(STDDEV(comments)::numeric, 2) AS  sd_comments,
    ROUND(STDDEV(saves)::numeric, 2) AS  sd_saves,
    ROUND(STDDEV(engagements)::numeric, 2) AS  sd_engagements,
    ROUND(STDDEV(impressions)::numeric, 2) AS  sd_impressions
FROM
    rawdata_stage
WHERE
    date_column BETWEEN '1/15/2013' AND '7/13/2023'
GROUP BY
    network, date_trunc('month', date_column)
ORDER BY
    fullmonth;

\copy (Select * From platform_month_trends) To '/tmp/platform_month_trends.csv' With CSV DELIMITER ',' HEADER

/* By quarter */
CREATE TABLE platform_quarter_trends (
    id SERIAL PRIMARY KEY,
	network VARCHAR (100),
	fullquarter timestamp,
    quarter INT,
    total_likes BIGINT,
    total_shares BIGINT,
    total_comments BIGINT,
    total_saves BIGINT,
    total_engagements BIGINT,
    total_impressions BIGINT,
    mean_likes DECIMAL (9,2),
    mean_shares DECIMAL (9,2),
    mean_comments DECIMAL (9,2),
    mean_saves DECIMAL (9,2),
    mean_engagements DECIMAL (9,2),
    mean_impressions DECIMAL (9,2),
    sd_likes DECIMAL (9,2),
    sd_shares DECIMAL (9,2),
    sd_comments DECIMAL (9,2),
    sd_saves DECIMAL (9,2),
    sd_engagements DECIMAL (9,2),
    sd_impressions DECIMAL (9,2)
)
INSERT INTO platform_quarter_trends (
	network,
	fullquarter,
    quarter,
    total_likes,
    total_shares,
    total_comments,
    total_saves,
    total_engagements,
    total_impressions,
    mean_likes,
    mean_shares,
    mean_comments,
    mean_saves,
    mean_engagements,
    mean_impressions,
    sd_likes,
    sd_shares,
    sd_comments,
    sd_saves,
    sd_engagements,
    sd_impressions
)
SELECT
	network,
    date_trunc('quarter', date_column) AS fullquarter,
    EXTRACT(QUARTER FROM date_trunc('quarter', date_column)) AS quarter,
    SUM(likes) AS total_likes,
    SUM(shares) AS total_shares,
    SUM(comments) AS total_comments,
    SUM(saves) AS total_saves,
    SUM(engagements) AS total_engagements,
    SUM(impressions) AS total_impressions,
    ROUND(SUM(likes) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_likes,
    ROUND(SUM(shares) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_shares,
    ROUND(SUM(comments) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_comments,
    ROUND(SUM(saves) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_saves,
    ROUND(SUM(engagements) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_engagements,
    ROUND(SUM(impressions) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_impressions,
    ROUND(STDDEV(likes)::numeric, 2) AS sd_likes,
    ROUND(STDDEV(shares)::numeric, 2) AS  sd_shares,
    ROUND(STDDEV(comments)::numeric, 2) AS  sd_comments,
    ROUND(STDDEV(saves)::numeric, 2) AS  sd_saves,
    ROUND(STDDEV(engagements)::numeric, 2) AS  sd_engagements,
    ROUND(STDDEV(impressions)::numeric, 2) AS  sd_impressions
FROM
    rawdata_stage
WHERE
    date_column BETWEEN '1/15/2013' AND '7/13/2023'
GROUP BY
    network, date_trunc('quarter', date_column)
ORDER BY
    fullquarter;

\copy (Select * From platform_quarter_trends) To '/tmp/platform_quarter_trends.csv' With CSV DELIMITER ',' HEADER

/* By year */
CREATE TABLE platform_year_trends (
    id SERIAL PRIMARY KEY,
	network VARCHAR (100),
	fullyear timestamp,
    year INT,
    total_likes BIGINT,
    total_shares BIGINT,
    total_comments BIGINT,
    total_saves BIGINT,
    total_engagements BIGINT,
    total_impressions BIGINT,
    mean_likes DECIMAL (9,2),
    mean_shares DECIMAL (9,2),
    mean_comments DECIMAL (9,2),
    mean_saves DECIMAL (9,2),
    mean_engagements DECIMAL (9,2),
    mean_impressions DECIMAL (9,2),
    sd_likes DECIMAL (9,2),
    sd_shares DECIMAL (9,2),
    sd_comments DECIMAL (9,2),
    sd_saves DECIMAL (9,2),
    sd_engagements DECIMAL (9,2),
    sd_impressions DECIMAL (9,2)
)
INSERT INTO platform_year_trends (
    network,
	fullyear,
    year,
    total_likes,
    total_shares,
    total_comments,
    total_saves,
    total_engagements,
    total_impressions,
    mean_likes,
    mean_shares,
    mean_comments,
    mean_saves,
    mean_engagements,
    mean_impressions,
    sd_likes,
    sd_shares,
    sd_comments,
    sd_saves,
    sd_engagements,
    sd_impressions
)
SELECT
    network,
    date_trunc('year', date_column) AS fullyear,
    EXTRACT(YEAR FROM date_trunc('year', date_column)) AS year,
    SUM(likes) AS total_likes,
    SUM(shares) AS total_shares,
    SUM(comments) AS total_comments,
    SUM(saves) AS total_saves,
    SUM(engagements) AS total_engagements,
    SUM(impressions) AS total_impressions,
    ROUND(SUM(likes) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_likes,
    ROUND(SUM(shares) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_shares,
    ROUND(SUM(comments) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_comments,
    ROUND(SUM(saves) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_saves,
    ROUND(SUM(engagements) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_engagements,
    ROUND(SUM(impressions) / NULLIF(COUNT(*), 0)::numeric, 2) AS mean_impressions,
    ROUND(STDDEV(likes)::numeric, 2) AS sd_likes,
    ROUND(STDDEV(shares)::numeric, 2) AS  sd_shares,
    ROUND(STDDEV(comments)::numeric, 2) AS  sd_comments,
    ROUND(STDDEV(saves)::numeric, 2) AS  sd_saves,
    ROUND(STDDEV(engagements)::numeric, 2) AS  sd_engagements,
    ROUND(STDDEV(impressions)::numeric, 2) AS  sd_impressions
FROM
    rawdata_stage
WHERE
    date_column BETWEEN '1/15/2013' AND '7/13/2023'
GROUP BY
    network, date_trunc('year', date_column)
ORDER BY
    fullyear;

\copy (Select * From platform_year_trends) To '/tmp/platform_year_trends.csv' With CSV DELIMITER ',' HEADER
