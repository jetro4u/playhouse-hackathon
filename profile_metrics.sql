CREATE TABLE profile_metrics (
    id SERIAL PRIMARY KEY,
	profile VARCHAR(20),
    total_likes BIGINT,
    total_comments BIGINT,
    total_shares BIGINT,
    total_engagements BIGINT,
    total_impressions BIGINT,
    total_interactions BIGINT,
	engagement_rate_per_impression DECIMAL (9,2),
	engagement_rate_per_reach DECIMAL (9,2),
	click_through_rate DECIMAL (9,2),
	mean_engagement_per_post DECIMAL (9,2),
	mean_impression_per_post DECIMAL (9,2),
	mean_likes_per_post DECIMAL (9,2),
	mean_comments_per_post DECIMAL (9,2),
	mean_shares_per_post DECIMAL (9,2),
    sd_likes DECIMAL (9,2),
    sd_comments DECIMAL (9,2),
    sd_shares DECIMAL (9,2),
    sd_engagements DECIMAL (9,2),
    sd_impressions DECIMAL (9,2)
)
INSERT INTO profile_metrics (
	profile,
    total_likes,
    total_comments,
    total_shares,
    total_engagements,
    total_impressions,
    total_interactions,
	engagement_rate_per_impression,
	engagement_rate_per_reach,
	click_through_rate,
	mean_engagement_per_post,
	mean_impression_per_post,
	mean_likes_per_post,
	mean_comments_per_post,
	mean_shares_per_post,
    sd_likes,
    sd_comments,
    sd_shares,
    sd_engagements,
    sd_impressions
)
SELECT
    profile,
    SUM(likes) AS total_likes,
    SUM(comments) AS total_comments,
    SUM(shares) AS total_shares,
    SUM(engagements) AS total_engagements,
    SUM(impressions) AS total_impressions,
    ROUND(SUM(likes + comments + shares)::DECIMAL(9,2)) AS total_interactions,
    ROUND(AVG(engagement_rate_per_impression)::DECIMAL(9,2)) AS engagement_rate_per_impression,
    ROUND(AVG(engagement_rate_per_reach)::DECIMAL(9,2)) AS engagement_rate_per_reach,
    ROUND(AVG(click_through_rate)::DECIMAL(9,2)) AS click_through_rate,
    ROUND(SUM(engagements) / NULLIF(COUNT(*) FILTER (WHERE profile = profile), 0)::DECIMAL(9,2)) AS mean_engagement_per_post,
    ROUND(SUM(impressions) / NULLIF(COUNT(*) FILTER (WHERE profile = profile), 0)::DECIMAL(9,2)) AS mean_impression_per_post,
    ROUND(SUM(likes) / NULLIF(COUNT(*) FILTER (WHERE profile = profile), 0)::DECIMAL(9,2)) AS mean_likes_per_post,
    ROUND(SUM(comments) / NULLIF(COUNT(*) FILTER (WHERE profile = profile), 0)::DECIMAL(9,2)) AS mean_comments_per_post,
    ROUND(SUM(shares) / NULLIF(COUNT(*) FILTER (WHERE profile = profile), 0)::DECIMAL(9,2)) AS mean_shares_per_post,
    ROUND(STDDEV(likes)::DECIMAL(9,2)) AS sd_likes,
    ROUND(STDDEV(comments)::DECIMAL(9,2)) AS  sd_comments,
    ROUND(STDDEV(shares)::DECIMAL(9,2)) AS  sd_shares,
    ROUND(STDDEV(engagements)::DECIMAL(9,2)) AS  sd_engagements,
    ROUND(STDDEV(impressions)::DECIMAL(9,2)) AS  sd_impressions
FROM
    rawdata_stage
GROUP BY
    profile
ORDER BY
    profile;


\copy (Select * From profile_metrics) To '/tmp/profile_metrics.csv' With CSV DELIMITER ',' HEADER
