/* STEP 2 Data Cleaning and Pre-processing: 
Handle missing values, either by imputing or removing them based on 
their relevance.

Convert data types if necessary (e.g., dates, categorical data).
Remove any duplicate entries.
*/

/* CREATE DIMENSION TABLE AND POPULATE DATA*/
CREATE TABLE ContentType (
    ContentTypeID SERIAL PRIMARY KEY,
    Content_Type VARCHAR(20) UNIQUE
);
INSERT INTO ContentType (content_type)
SELECT 
    content_type
FROM (
    SELECT DISTINCT content_type FROM facebook_stage
    UNION
    SELECT DISTINCT content_type FROM instagram_stage
    UNION
    SELECT DISTINCT content_type FROM linkedin_stage
    UNION
    SELECT DISTINCT content_type FROM twitter_stage
) AS content_type;

CREATE TABLE PostType (
    PostTypeID SERIAL PRIMARY KEY,
    Post_Type VARCHAR(20) UNIQUE
);
INSERT INTO PostType (post_type)
SELECT 
    post_type
FROM (
    SELECT DISTINCT post_type FROM facebook_stage
    UNION
    SELECT DISTINCT post_type FROM instagram_stage
    UNION
    SELECT DISTINCT post_type FROM linkedin_stage
    UNION
    SELECT DISTINCT post_type FROM twitter_stage
) AS post_type;

CREATE TABLE Network (
    NetworkID SERIAL PRIMARY KEY,
    Network VARCHAR(20) UNIQUE
);
INSERT INTO Network (Network)
SELECT DISTINCT network FROM rawdata_stage;

CREATE TABLE SentBy (
    SentByID SERIAL PRIMARY KEY,
    Sent_By TEXT UNIQUE
);
INSERT INTO SentBy (Sent_By)
SELECT DISTINCT Sent_By FROM rawdata_stage where Sent_By != ' '; 

CREATE TABLE Profile (
    ProfileID SERIAL PRIMARY KEY,
    Profile TEXT UNIQUE
);
INSERT INTO Profile (Profile)
SELECT DISTINCT profile FROM rawdata_stage;

/* ADD CALENDER TABLE */
ALTER TABLE rawdata_stage ADD COLUMN date DATE;
ALTER TABLE rawdata_stage ADD COLUMN time text;
ALTER TABLE rawdata_stage ADD COLUMN day INT;
ALTER TABLE rawdata_stage ADD COLUMN month INT;
ALTER TABLE rawdata_stage ADD COLUMN quarter INT;
ALTER TABLE rawdata_stage ADD COLUMN year INT;

UPDATE rawdata_stage
SET date = DATE(date_column);

UPDATE rawdata_stage
SET time = EXTRACT(HOUR FROM date_column)::TEXT || ':' || EXTRACT(MINUTE FROM date_column)::TEXT;

UPDATE rawdata_stage
SET day = EXTRACT(DAY FROM date_column);

UPDATE rawdata_stage
SET month = EXTRACT(MONTH FROM date_column);

UPDATE rawdata_stage
SET quarter = EXTRACT(QUARTER FROM date_column);

UPDATE rawdata_stage
SET year = EXTRACT(YEAR FROM date_column);

/* CHECK FOR EMPTY VALUES */
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'rawdata_stage'
  AND table_catalog = current_database()
  AND column_name IN (
    SELECT column_name
    FROM rawdata_stage
    WHERE column_name IS NOT NULL AND column_name = '';

/* CHANGE STRING TO NULL */
UPDATE rawdata_stage SET impressions = NULL WHERE impressions = '';


/* REMOVE , FROM value */
UPDATE rawdata_stage SET impressions = REPLACE(impressions, ',', '');
UPDATE rawdata_stage SET organic_impressions = REPLACE(organic_impressions, ',', '');
UPDATE rawdata_stage SET viral_impressions = REPLACE(viral_impressions, ',', '');
UPDATE rawdata_stage SET non_viral_impressions = REPLACE(non_viral_impressions, ',', '');
UPDATE rawdata_stage SET paid_impressions = REPLACE(paid_impressions, ',', '');
UPDATE rawdata_stage SET fan_impressions = REPLACE(fan_impressions, ',', '');
UPDATE rawdata_stage SET fan_organic_impressions = REPLACE(fan_organic_impressions, ',', '');
UPDATE rawdata_stage SET fan_paid_impressions = REPLACE(fan_paid_impressions, ',', '');
UPDATE rawdata_stage SET non_fan_impressions = REPLACE(non_fan_impressions, ',', '');
UPDATE rawdata_stage SET non_fan_organic_impressions = REPLACE(non_fan_organic_impressions, ',', '');
UPDATE rawdata_stage SET non_fan_paid_impressions = REPLACE(non_fan_paid_impressions, ',', '');
UPDATE rawdata_stage SET reach = REPLACE(reach, ',', '');
UPDATE rawdata_stage SET organic_reach = REPLACE(organic_reach, ',', '');
UPDATE rawdata_stage SET viral_reach = REPLACE(viral_reach, ',', '');
UPDATE rawdata_stage SET non_viral_reach = REPLACE(non_viral_reach, ',', '');
UPDATE rawdata_stage SET paid_reach = REPLACE(paid_reach, ',', '');
UPDATE rawdata_stage SET paid_reach = REPLACE(paid_reach, ',', '');
UPDATE rawdata_stage SET fan_reach = REPLACE(fan_reach, ',', '');
UPDATE rawdata_stage SET fan_paid_reach = REPLACE(fan_paid_reach, ',', '');
UPDATE rawdata_stage SET potential_reach = REPLACE(potential_reach, ',', '');
UPDATE rawdata_stage SET engagement_rate_per_impression = REPLACE(engagement_rate_per_impression, ',', '');
UPDATE rawdata_stage SET engagement_rate_per_reach = REPLACE(engagement_rate_per_reach, ',', '');
UPDATE rawdata_stage SET engagements = REPLACE(engagements, ',', '');
UPDATE rawdata_stage SET reactions = REPLACE(reactions, ',', '');
UPDATE rawdata_stage SET likes = REPLACE(likes, ',', '');
UPDATE rawdata_stage SET dislikes = REPLACE(dislikes, ',', '');
UPDATE rawdata_stage SET love_reactions = REPLACE(love_reactions, ',', '');
UPDATE rawdata_stage SET haha_reactions = REPLACE(haha_reactions, ',', '');
UPDATE rawdata_stage SET wow_reactions = REPLACE(wow_reactions, ',', '');
UPDATE rawdata_stage SET sad_reactions = REPLACE(sad_reactions, ',', '');
UPDATE rawdata_stage SET angry_reactions = REPLACE(angry_reactions, ',', '');
UPDATE rawdata_stage SET comments = REPLACE(comments, ',', '');
UPDATE rawdata_stage SET shares = REPLACE(shares, ',', '');
UPDATE rawdata_stage SET saves = REPLACE(saves, ',', '');
UPDATE rawdata_stage SET post_link_clicks = REPLACE(post_link_clicks, ',', '');
UPDATE rawdata_stage SET other_post_clicks = REPLACE(other_post_clicks, ',', '');
UPDATE rawdata_stage SET post_clicks_all = REPLACE(post_clicks_all, ',', '');
UPDATE rawdata_stage SET post_media_clicks = REPLACE(post_media_clicks, ',', '');
UPDATE rawdata_stage SET post_hashtag_clicks = REPLACE(post_hashtag_clicks, ',', '');
UPDATE rawdata_stage SET post_detail_expand_clicks = REPLACE(post_detail_expand_clicks, ',', '');
UPDATE rawdata_stage SET profile_clicks = REPLACE(profile_clicks, ',', '');
UPDATE rawdata_stage SET post_photo_view_clicks = REPLACE(post_photo_view_clicks, ',', '');
UPDATE rawdata_stage SET post_video_play_clicks = REPLACE(post_video_play_clicks, ',', '');
UPDATE rawdata_stage SET other_engagements = REPLACE(other_engagements, ',', '');
UPDATE rawdata_stage SET answers = REPLACE(answers, ',', '');
UPDATE rawdata_stage SET app_engagements = REPLACE(app_engagements, ',', '');
UPDATE rawdata_stage SET app_install_attempts = REPLACE(app_install_attempts, ',', '');
UPDATE rawdata_stage SET app_opens = REPLACE(app_opens, ',', '');
UPDATE rawdata_stage SET follows_from_post = REPLACE(follows_from_post, ',', '');
UPDATE rawdata_stage SET unfollows_from_post = REPLACE(unfollows_from_post, ',', '');
UPDATE rawdata_stage SET negative_feedback = REPLACE(negative_feedback, ',', '');
UPDATE rawdata_stage SET bit_ly_link_clicks = REPLACE(bit_ly_link_clicks, ',', '');
UPDATE rawdata_stage SET engaged_users = REPLACE(engaged_users, ',', '');
UPDATE rawdata_stage SET engaged_fans = REPLACE(engaged_fans, ',', '');
UPDATE rawdata_stage SET users_talking_about_this = REPLACE(users_talking_about_this, ',', '');
UPDATE rawdata_stage SET unique_reactions = REPLACE(unique_reactions, ',', '');
UPDATE rawdata_stage SET unique_comments = REPLACE(unique_comments, ',', '');
UPDATE rawdata_stage SET unique_shares = REPLACE(unique_shares, ',', '');
UPDATE rawdata_stage SET unique_answers = REPLACE(unique_answers, ',', '');
UPDATE rawdata_stage SET unique_post_clicks = REPLACE(unique_post_clicks, ',', '');
UPDATE rawdata_stage SET unique_post_link_clicks = REPLACE(unique_post_link_clicks, ',', '');
UPDATE rawdata_stage SET unique_post_photo_view_clicks = REPLACE(unique_post_photo_view_clicks, ',', '');
UPDATE rawdata_stage SET unique_post_video_play_clicks = REPLACE(unique_post_video_play_clicks, ',', '');
UPDATE rawdata_stage SET unique_other_post_clicks = REPLACE(unique_other_post_clicks, ',', '');
UPDATE rawdata_stage SET unique_negative_feedback = REPLACE(unique_negative_feedback, ',', '');
UPDATE rawdata_stage SET subscribers_gained_from_video = REPLACE(subscribers_gained_from_video, ',', '');
UPDATE rawdata_stage SET annotation_clicks = REPLACE(annotation_clicks, ',', '');
UPDATE rawdata_stage SET card_clicks = REPLACE(card_clicks, ',', '');
UPDATE rawdata_stage SET video_views = REPLACE(video_views, ',', '');
UPDATE rawdata_stage SET media_views = REPLACE(media_views, ',', '');
UPDATE rawdata_stage SET organic_video_views = REPLACE(organic_video_views, ',', '');
UPDATE rawdata_stage SET paid_video_views = REPLACE(paid_video_views, ',', '');

UPDATE rawdata_stage SET impressions = 0 WHERE impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN impressions SET DATA TYPE BIGINT USING impressions::bigint;
UPDATE rawdata_stage SET organic_impressions = 0 WHERE organic_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN organic_impressions SET DATA TYPE BIGINT USING organic_impressions::bigint;
UPDATE rawdata_stage SET viral_impressions = 0 WHERE viral_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN viral_impressions SET DATA TYPE BIGINT USING viral_impressions::bigint;
UPDATE rawdata_stage SET non_viral_impressions = 0 WHERE non_viral_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN non_viral_impressions SET DATA TYPE BIGINT USING non_viral_impressions::bigint;
UPDATE rawdata_stage SET paid_impressions = 0 WHERE paid_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN paid_impressions SET DATA TYPE BIGINT USING paid_impressions::bigint;
UPDATE rawdata_stage SET fan_impressions = 0 WHERE fan_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN fan_impressions SET DATA TYPE BIGINT USING fan_impressions::bigint;
UPDATE rawdata_stage SET fan_organic_impressions = 0 WHERE fan_organic_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN fan_organic_impressions SET DATA TYPE BIGINT USING fan_organic_impressions::bigint;
UPDATE rawdata_stage SET fan_paid_impressions = 0 WHERE fan_paid_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN fan_paid_impressions SET DATA TYPE BIGINT USING fan_paid_impressions::bigint;
UPDATE rawdata_stage SET non_fan_impressions = 0 WHERE non_fan_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN non_fan_impressions SET DATA TYPE BIGINT USING non_fan_impressions::bigint;
UPDATE rawdata_stage SET non_fan_organic_impressions = 0 WHERE non_fan_organic_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN non_fan_organic_impressions SET DATA TYPE BIGINT USING non_fan_organic_impressions::bigint;
UPDATE rawdata_stage SET non_fan_paid_impressions = 0 WHERE non_fan_paid_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN non_fan_paid_impressions SET DATA TYPE BIGINT USING non_fan_paid_impressions::bigint;
UPDATE rawdata_stage SET reach = 0 WHERE reach = '';
ALTER TABLE rawdata_stage ALTER COLUMN reach SET DATA TYPE BIGINT USING reach::bigint;
UPDATE rawdata_stage SET organic_reach = 0 WHERE organic_reach = '';
ALTER TABLE rawdata_stage ALTER COLUMN organic_reach SET DATA TYPE BIGINT USING organic_reach::bigint;
UPDATE rawdata_stage SET viral_reach = 0 WHERE viral_reach = '';
ALTER TABLE rawdata_stage ALTER COLUMN viral_reach SET DATA TYPE BIGINT USING viral_reach::bigint;
UPDATE rawdata_stage SET non_viral_reach = 0 WHERE non_viral_reach = '';
ALTER TABLE rawdata_stage ALTER COLUMN non_viral_reach SET DATA TYPE BIGINT USING non_viral_reach::bigint;
UPDATE rawdata_stage SET paid_reach = 0 WHERE paid_reach = '';
ALTER TABLE rawdata_stage ALTER COLUMN paid_reach SET DATA TYPE BIGINT USING paid_reach::bigint;
UPDATE rawdata_stage SET fan_reach = 0 WHERE fan_reach = '';
ALTER TABLE rawdata_stage ALTER COLUMN fan_reach SET DATA TYPE BIGINT USING fan_reach::bigint;
UPDATE rawdata_stage SET fan_paid_reach = 0 WHERE fan_paid_reach = '';
ALTER TABLE rawdata_stage ALTER COLUMN fan_paid_reach SET DATA TYPE BIGINT USING fan_paid_reach::bigint;
UPDATE rawdata_stage SET potential_reach = 0 WHERE potential_reach = '';
ALTER TABLE rawdata_stage ALTER COLUMN potential_reach SET DATA TYPE BIGINT USING potential_reach::bigint;
UPDATE rawdata_stage SET engagement_rate_per_impression = 0 WHERE engagement_rate_per_impression = '';
ALTER TABLE rawdata_stage ALTER COLUMN engagement_rate_per_impression SET DATA TYPE BIGINT USING engagement_rate_per_impression::bigint;
UPDATE rawdata_stage SET engagements = 0 WHERE engagements = '';
ALTER TABLE rawdata_stage ALTER COLUMN engagements SET DATA TYPE BIGINT USING engagements::bigint;
UPDATE rawdata_stage SET reactions = 0 WHERE reactions = '';
ALTER TABLE rawdata_stage ALTER COLUMN reactions SET DATA TYPE BIGINT USING reactions::bigint;
UPDATE rawdata_stage SET likes = 0 WHERE likes = '';
ALTER TABLE rawdata_stage ALTER COLUMN likes SET DATA TYPE BIGINT USING likes::bigint;
UPDATE rawdata_stage SET dislikes = 0 WHERE dislikes = '';
ALTER TABLE rawdata_stage ALTER COLUMN dislikes SET DATA TYPE BIGINT USING dislikes::bigint;
UPDATE rawdata_stage SET love_reactions = 0 WHERE love_reactions = '';
ALTER TABLE rawdata_stage ALTER COLUMN love_reactions SET DATA TYPE BIGINT USING love_reactions::bigint;
UPDATE rawdata_stage SET haha_reactions = 0 WHERE haha_reactions = '';
ALTER TABLE rawdata_stage ALTER COLUMN haha_reactions SET DATA TYPE BIGINT USING haha_reactions::bigint;
UPDATE rawdata_stage SET wow_reactions = 0 WHERE wow_reactions = '';
ALTER TABLE rawdata_stage ALTER COLUMN wow_reactions SET DATA TYPE BIGINT USING wow_reactions::bigint;
UPDATE rawdata_stage SET sad_reactions = 0 WHERE sad_reactions = '';
ALTER TABLE rawdata_stage ALTER COLUMN sad_reactions SET DATA TYPE BIGINT USING sad_reactions::bigint;
UPDATE rawdata_stage SET angry_reactions = 0 WHERE angry_reactions = '';
ALTER TABLE rawdata_stage ALTER COLUMN angry_reactions SET DATA TYPE BIGINT USING angry_reactions::bigint;
UPDATE rawdata_stage SET comments = 0 WHERE comments = '';
ALTER TABLE rawdata_stage ALTER COLUMN comments SET DATA TYPE BIGINT USING comments::bigint;
UPDATE rawdata_stage SET shares = 0 WHERE shares = '';
ALTER TABLE rawdata_stage ALTER COLUMN shares SET DATA TYPE BIGINT USING shares::bigint;
UPDATE rawdata_stage SET saves = 0 WHERE saves = '';
ALTER TABLE rawdata_stage ALTER COLUMN saves SET DATA TYPE BIGINT USING saves::bigint;
UPDATE rawdata_stage SET post_link_clicks = 0 WHERE post_link_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN post_link_clicks SET DATA TYPE BIGINT USING post_link_clicks::bigint;
UPDATE rawdata_stage SET other_post_clicks = 0 WHERE other_post_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN other_post_clicks SET DATA TYPE BIGINT USING other_post_clicks::bigint;
UPDATE rawdata_stage SET post_clicks_all = 0 WHERE post_clicks_all = '';
ALTER TABLE rawdata_stage ALTER COLUMN post_clicks_all SET DATA TYPE BIGINT USING post_clicks_all::bigint;
UPDATE rawdata_stage SET post_media_clicks = 0 WHERE post_media_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN post_media_clicks SET DATA TYPE BIGINT USING post_media_clicks::bigint;
UPDATE rawdata_stage SET post_hashtag_clicks = 0 WHERE post_hashtag_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN post_hashtag_clicks SET DATA TYPE BIGINT USING post_hashtag_clicks::bigint;
UPDATE rawdata_stage SET post_detail_expand_clicks = 0 WHERE post_detail_expand_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN post_detail_expand_clicks SET DATA TYPE BIGINT USING post_detail_expand_clicks::bigint;
UPDATE rawdata_stage SET profile_clicks = 0 WHERE profile_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN profile_clicks SET DATA TYPE BIGINT USING profile_clicks::bigint;
UPDATE rawdata_stage SET post_photo_view_clicks = 0 WHERE post_photo_view_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN post_photo_view_clicks SET DATA TYPE BIGINT USING post_photo_view_clicks::bigint;
UPDATE rawdata_stage SET post_video_play_clicks = 0 WHERE post_video_play_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN post_video_play_clicks SET DATA TYPE BIGINT USING post_video_play_clicks::bigint;
UPDATE rawdata_stage SET other_engagements = 0 WHERE other_engagements = '';
ALTER TABLE rawdata_stage ALTER COLUMN other_engagements SET DATA TYPE BIGINT USING other_engagements::bigint;
UPDATE rawdata_stage SET answers = 0 WHERE answers = '';
ALTER TABLE rawdata_stage ALTER COLUMN answers SET DATA TYPE BIGINT USING answers::bigint;
UPDATE rawdata_stage SET app_engagements = 0 WHERE app_engagements = '';
ALTER TABLE rawdata_stage ALTER COLUMN app_engagements SET DATA TYPE BIGINT USING app_engagements::bigint;
UPDATE rawdata_stage SET app_install_attempts = 0 WHERE app_install_attempts = '';
ALTER TABLE rawdata_stage ALTER COLUMN app_install_attempts SET DATA TYPE BIGINT USING app_install_attempts::bigint;
UPDATE rawdata_stage SET app_opens = 0 WHERE app_opens = '';
ALTER TABLE rawdata_stage ALTER COLUMN app_opens SET DATA TYPE BIGINT USING app_opens::bigint;
UPDATE rawdata_stage SET follows_from_post = 0 WHERE follows_from_post = '';
ALTER TABLE rawdata_stage ALTER COLUMN follows_from_post SET DATA TYPE BIGINT USING follows_from_post::bigint;
UPDATE rawdata_stage SET unfollows_from_post = 0 WHERE unfollows_from_post = '';
ALTER TABLE rawdata_stage ALTER COLUMN unfollows_from_post SET DATA TYPE BIGINT USING unfollows_from_post::bigint;
UPDATE rawdata_stage SET negative_feedback = 0 WHERE negative_feedback = '';
ALTER TABLE rawdata_stage ALTER COLUMN negative_feedback SET DATA TYPE BIGINT USING negative_feedback::bigint;
UPDATE rawdata_stage SET bit_ly_link_clicks = 0 WHERE bit_ly_link_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN bit_ly_link_clicks SET DATA TYPE BIGINT USING bit_ly_link_clicks::bigint;
UPDATE rawdata_stage SET engaged_users = 0 WHERE engaged_users = '';
ALTER TABLE rawdata_stage ALTER COLUMN engaged_users SET DATA TYPE BIGINT USING engaged_users::bigint;
UPDATE rawdata_stage SET engaged_fans = 0 WHERE engaged_fans = '';
ALTER TABLE rawdata_stage ALTER COLUMN engaged_fans SET DATA TYPE BIGINT USING engaged_fans::bigint;
UPDATE rawdata_stage SET users_talking_about_this = 0 WHERE users_talking_about_this = '';
ALTER TABLE rawdata_stage ALTER COLUMN users_talking_about_this SET DATA TYPE BIGINT USING users_talking_about_this::bigint;
UPDATE rawdata_stage SET unique_reactions = 0 WHERE unique_reactions = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_reactions SET DATA TYPE BIGINT USING unique_reactions::bigint;
UPDATE rawdata_stage SET unique_comments = 0 WHERE unique_comments = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_comments SET DATA TYPE BIGINT USING unique_comments::bigint;
UPDATE rawdata_stage SET unique_comments = 0 WHERE unique_comments = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_comments SET DATA TYPE BIGINT USING unique_comments::bigint;
UPDATE rawdata_stage SET unique_shares = 0 WHERE unique_shares = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_shares SET DATA TYPE BIGINT USING unique_shares::bigint;
UPDATE rawdata_stage SET unique_answers = 0 WHERE unique_answers = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_answers SET DATA TYPE BIGINT USING unique_answers::bigint;
UPDATE rawdata_stage SET unique_post_clicks = 0 WHERE unique_post_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_post_clicks SET DATA TYPE BIGINT USING unique_post_clicks::bigint;
UPDATE rawdata_stage SET unique_post_link_clicks = 0 WHERE unique_post_link_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_post_link_clicks SET DATA TYPE BIGINT USING unique_post_link_clicks::bigint;
UPDATE rawdata_stage SET unique_post_photo_view_clicks = 0 WHERE unique_post_photo_view_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_post_photo_view_clicks SET DATA TYPE BIGINT USING unique_post_photo_view_clicks::bigint;
UPDATE rawdata_stage SET unique_post_video_play_clicks = 0 WHERE unique_post_video_play_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_post_video_play_clicks SET DATA TYPE BIGINT USING unique_post_video_play_clicks::bigint;
UPDATE rawdata_stage SET unique_other_post_clicks = 0 WHERE unique_other_post_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_other_post_clicks SET DATA TYPE BIGINT USING unique_other_post_clicks::bigint;
UPDATE rawdata_stage SET unique_negative_feedback = 0 WHERE unique_negative_feedback = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_negative_feedback SET DATA TYPE BIGINT USING unique_negative_feedback::bigint;
UPDATE rawdata_stage SET subscribers_gained_from_video = 0 WHERE subscribers_gained_from_video = '';
ALTER TABLE rawdata_stage ALTER COLUMN subscribers_gained_from_video SET DATA TYPE BIGINT USING subscribers_gained_from_video::bigint;
UPDATE rawdata_stage SET annotation_clicks = 0 WHERE annotation_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN annotation_clicks SET DATA TYPE BIGINT USING annotation_clicks::bigint;
UPDATE rawdata_stage SET card_clicks = 0 WHERE card_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN card_clicks SET DATA TYPE BIGINT USING card_clicks::bigint;
UPDATE rawdata_stage SET video_views = REPLACE(video_views, ',', '');
UPDATE rawdata_stage SET video_views = 0 WHERE video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN video_views SET DATA TYPE BIGINT USING video_views::bigint;
UPDATE rawdata_stage SET media_views = REPLACE(media_views, ',', '');
UPDATE rawdata_stage SET media_views = 0 WHERE media_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN media_views SET DATA TYPE BIGINT USING media_views::bigint;
UPDATE rawdata_stage SET organic_video_views = REPLACE(organic_video_views, ',', '');
UPDATE rawdata_stage SET organic_video_views = 0 WHERE organic_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN organic_video_views SET DATA TYPE BIGINT USING organic_video_views::bigint;
UPDATE rawdata_stage SET paid_video_views = 0 WHERE paid_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN paid_video_views SET DATA TYPE BIGINT USING paid_video_views::bigint;
UPDATE rawdata_stage SET partial_video_views = REPLACE(partial_video_views, ',', '');
UPDATE rawdata_stage SET partial_video_views = 0 WHERE partial_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN partial_video_views SET DATA TYPE BIGINT USING partial_video_views::bigint;
UPDATE rawdata_stage SET organic_partial_video_views = REPLACE(organic_partial_video_views, ',', '');
UPDATE rawdata_stage SET organic_partial_video_views = 0 WHERE organic_partial_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN organic_partial_video_views SET DATA TYPE BIGINT USING organic_partial_video_views::bigint;
UPDATE rawdata_stage SET paid_partial_video_views = REPLACE(paid_partial_video_views, ',', '');
UPDATE rawdata_stage SET paid_partial_video_views = 0 WHERE paid_partial_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN paid_partial_video_views SET DATA TYPE BIGINT USING paid_partial_video_views::bigint;
UPDATE rawdata_stage SET full_video_views = REPLACE(full_video_views, ',', '');
UPDATE rawdata_stage SET full_video_views = 0 WHERE full_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN full_video_views SET DATA TYPE BIGINT USING full_video_views::bigint;
UPDATE rawdata_stage SET full_video_view_rate = REPLACE(full_video_view_rate, ',', '');
UPDATE rawdata_stage SET full_video_view_rate = 0 WHERE full_video_view_rate = '';
ALTER TABLE rawdata_stage ALTER COLUMN full_video_view_rate SET DATA TYPE BIGINT USING full_video_view_rate::bigint;
UPDATE rawdata_stage SET follow_video_views = REPLACE(follow_video_views, ',', '');
UPDATE rawdata_stage SET follow_video_views = 0 WHERE follow_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN follow_video_views SET DATA TYPE BIGINT USING follow_video_views::bigint;
UPDATE rawdata_stage SET for_you_video_views = REPLACE(for_you_video_views, ',', '');
UPDATE rawdata_stage SET for_you_video_views = 0 WHERE for_you_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN for_you_video_views SET DATA TYPE BIGINT USING for_you_video_views::bigint;
UPDATE rawdata_stage SET hashtag_video_views = REPLACE(hashtag_video_views, ',', '');
UPDATE rawdata_stage SET hashtag_video_views = 0 WHERE hashtag_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN hashtag_video_views SET DATA TYPE BIGINT USING hashtag_video_views::bigint;
UPDATE rawdata_stage SET business_account_video_views = REPLACE(business_account_video_views, ',', '');
UPDATE rawdata_stage SET business_account_video_views = 0 WHERE business_account_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN business_account_video_views SET DATA TYPE BIGINT USING business_account_video_views::bigint;
UPDATE rawdata_stage SET sound_video_views = REPLACE(sound_video_views, ',', '');
UPDATE rawdata_stage SET sound_video_views = 0 WHERE sound_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN sound_video_views SET DATA TYPE BIGINT USING sound_video_views::bigint;
UPDATE rawdata_stage SET unspecified_video_views = REPLACE(unspecified_video_views, ',', '');
UPDATE rawdata_stage SET unspecified_video_views = 0 WHERE unspecified_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN unspecified_video_views SET DATA TYPE BIGINT USING unspecified_video_views::bigint;
UPDATE rawdata_stage SET organic_full_video_views = REPLACE(organic_full_video_views, ',', '');
UPDATE rawdata_stage SET organic_full_video_views = 0 WHERE organic_full_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN organic_full_video_views SET DATA TYPE BIGINT USING organic_full_video_views::bigint;
UPDATE rawdata_stage SET paid_full_video_views = REPLACE(paid_full_video_views, ',', '');
UPDATE rawdata_stage SET paid_full_video_views = 0 WHERE paid_full_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN paid_full_video_views SET DATA TYPE BIGINT USING paid_full_video_views::bigint;
UPDATE rawdata_stage SET autoplay_video_views = REPLACE(autoplay_video_views, ',', '');
UPDATE rawdata_stage SET autoplay_video_views = 0 WHERE autoplay_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN autoplay_video_views SET DATA TYPE BIGINT USING autoplay_video_views::bigint;
UPDATE rawdata_stage SET click_to_play_video_views = REPLACE(click_to_play_video_views, ',', '');
UPDATE rawdata_stage SET click_to_play_video_views = 0 WHERE click_to_play_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN click_to_play_video_views SET DATA TYPE BIGINT USING click_to_play_video_views::bigint;
UPDATE rawdata_stage SET sound_on_video_views = REPLACE(sound_on_video_views, ',', '');
UPDATE rawdata_stage SET sound_on_video_views = 0 WHERE sound_on_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN sound_on_video_views SET DATA TYPE BIGINT USING sound_on_video_views::bigint;
UPDATE rawdata_stage SET sound_off_video_views = REPLACE(sound_off_video_views, ',', '');
UPDATE rawdata_stage SET sound_off_video_views = 0 WHERE sound_off_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN sound_off_video_views SET DATA TYPE BIGINT USING sound_off_video_views::bigint;
UPDATE rawdata_stage SET ten_second_video_views = REPLACE(ten_second_video_views, ',', '');
UPDATE rawdata_stage SET ten_second_video_views = 0 WHERE ten_second_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN ten_second_video_views SET DATA TYPE BIGINT USING ten_second_video_views::bigint;
UPDATE rawdata_stage SET organic_10_second_video_views = REPLACE(organic_10_second_video_views, ',', '');
UPDATE rawdata_stage SET organic_10_second_video_views = 0 WHERE organic_10_second_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN organic_10_second_video_views SET DATA TYPE BIGINT USING organic_10_second_video_views::bigint;
UPDATE rawdata_stage SET paid_10_second_video_views = REPLACE(paid_10_second_video_views, ',', '');
UPDATE rawdata_stage SET paid_10_second_video_views = 0 WHERE paid_10_second_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN paid_10_second_video_views SET DATA TYPE BIGINT USING paid_10_second_video_views::bigint;
UPDATE rawdata_stage SET autoplay_10_second_video_views = REPLACE(autoplay_10_second_video_views, ',', '');
UPDATE rawdata_stage SET autoplay_10_second_video_views = 0 WHERE autoplay_10_second_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN autoplay_10_second_video_views SET DATA TYPE BIGINT USING autoplay_10_second_video_views::bigint;
UPDATE rawdata_stage SET click_to_play_10_second_video_views = 0 WHERE click_to_play_10_second_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN click_to_play_10_second_video_views SET DATA TYPE BIGINT USING click_to_play_10_second_video_views::bigint;
UPDATE rawdata_stage SET sound_on_10_second_video_views = REPLACE(sound_on_10_second_video_views, ',', '');
UPDATE rawdata_stage SET sound_on_10_second_video_views = 0 WHERE sound_on_10_second_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN sound_on_10_second_video_views SET DATA TYPE BIGINT USING sound_on_10_second_video_views::bigint;
UPDATE rawdata_stage SET sound_off_10_second_video_views = REPLACE(sound_off_10_second_video_views, ',', '');
UPDATE rawdata_stage SET sound_off_10_second_video_views = 0 WHERE sound_off_10_second_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN sound_off_10_second_video_views SET DATA TYPE BIGINT USING sound_off_10_second_video_views::bigint;
UPDATE rawdata_stage SET autoplay_partial_video_views = REPLACE(autoplay_partial_video_views, ',', '');
UPDATE rawdata_stage SET autoplay_partial_video_views = 0 WHERE autoplay_partial_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN autoplay_partial_video_views SET DATA TYPE BIGINT USING autoplay_partial_video_views::bigint;
UPDATE rawdata_stage SET click_to_play_partial_video_views = REPLACE(click_to_play_partial_video_views, ',', '');
UPDATE rawdata_stage SET click_to_play_partial_video_views = 0 WHERE click_to_play_partial_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN click_to_play_partial_video_views SET DATA TYPE BIGINT USING click_to_play_partial_video_views::bigint;
UPDATE rawdata_stage SET autoplay_full_video_views = REPLACE(autoplay_full_video_views, ',', '');
UPDATE rawdata_stage SET autoplay_full_video_views = 0 WHERE autoplay_full_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN autoplay_full_video_views SET DATA TYPE BIGINT USING autoplay_full_video_views::bigint;
UPDATE rawdata_stage SET click_to_play_full_video_views = REPLACE(click_to_play_full_video_views, ',', '');
UPDATE rawdata_stage SET click_to_play_full_video_views = 0 WHERE click_to_play_full_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN click_to_play_full_video_views SET DATA TYPE BIGINT USING click_to_play_full_video_views::bigint;
UPDATE rawdata_stage SET nine_five_percent_video_views = REPLACE(nine_five_percent_video_views, ',', '');
UPDATE rawdata_stage SET nine_five_percent_video_views = 0 WHERE nine_five_percent_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN nine_five_percent_video_views SET DATA TYPE BIGINT USING nine_five_percent_video_views::bigint;
UPDATE rawdata_stage SET organic_95_percent_video_views = REPLACE(organic_95_percent_video_views, ',', '');
UPDATE rawdata_stage SET organic_95_percent_video_views = 0 WHERE organic_95_percent_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN organic_95_percent_video_views SET DATA TYPE BIGINT USING organic_95_percent_video_views::bigint;
UPDATE rawdata_stage SET paid_95_percent_video_views = REPLACE(paid_95_percent_video_views, ',', '');
UPDATE rawdata_stage SET paid_95_percent_video_views = 0 WHERE paid_95_percent_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN paid_95_percent_video_views SET DATA TYPE BIGINT USING paid_95_percent_video_views::bigint;
UPDATE rawdata_stage SET video_length_seconds = REPLACE(video_length_seconds, ',', '');
UPDATE rawdata_stage SET video_length_seconds = 0 WHERE video_length_seconds = '';
ALTER TABLE rawdata_stage ALTER COLUMN video_length_seconds SET DATA TYPE DECIMAL (9,2) USING video_length_seconds::DECIMAL (9,2);
UPDATE rawdata_stage SET video_length_seconds = REPLACE(video_length_seconds, ',', '');
UPDATE rawdata_stage SET video_length_seconds = 0 WHERE video_length_seconds = '';
ALTER TABLE rawdata_stage ALTER COLUMN video_length_seconds SET DATA TYPE DECIMAL (9,2) USING video_length_seconds::DECIMAL (9,2);
UPDATE rawdata_stage SET video_view_time_seconds = REPLACE(video_view_time_seconds, ',', '');
UPDATE rawdata_stage SET video_view_time_seconds = 0 WHERE video_view_time_seconds = '';
ALTER TABLE rawdata_stage ALTER COLUMN video_view_time_seconds SET DATA TYPE DECIMAL (9,2) USING video_view_time_seconds::DECIMAL (9,2);
UPDATE rawdata_stage SET organic_video_view_time_seconds = REPLACE(organic_video_view_time_seconds, ',', '');
UPDATE rawdata_stage SET organic_video_view_time_seconds = 0 WHERE organic_video_view_time_seconds = '';
ALTER TABLE rawdata_stage ALTER COLUMN organic_video_view_time_seconds SET DATA TYPE DECIMAL (9,2) USING organic_video_view_time_seconds::DECIMAL (9,2);
UPDATE rawdata_stage SET paid_video_view_time_seconds = REPLACE(paid_video_view_time_seconds, ',', '');
UPDATE rawdata_stage SET paid_video_view_time_seconds = 0 WHERE paid_video_view_time_seconds = '';
ALTER TABLE rawdata_stage ALTER COLUMN paid_video_view_time_seconds SET DATA TYPE DECIMAL (9,2) USING paid_video_view_time_seconds::DECIMAL (9,2);
UPDATE rawdata_stage SET unique_video_views = REPLACE(unique_video_views, ',', '');
UPDATE rawdata_stage SET unique_video_views = 0 WHERE unique_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_video_views SET DATA TYPE BIGINT USING unique_video_views::BIGINT;
UPDATE rawdata_stage SET unique_organic_video_views = REPLACE(unique_organic_video_views, ',', '');
UPDATE rawdata_stage SET unique_organic_video_views = 0 WHERE unique_organic_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_organic_video_views SET DATA TYPE BIGINT USING unique_organic_video_views::BIGINT;
UPDATE rawdata_stage SET unique_paid_video_views = REPLACE(unique_paid_video_views, ',', '');
UPDATE rawdata_stage SET unique_paid_video_views = 0 WHERE unique_paid_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_paid_video_views SET DATA TYPE BIGINT USING unique_paid_video_views::BIGINT;
UPDATE rawdata_stage SET unique_10_second_video_views = REPLACE(unique_10_second_video_views, ',', '');
UPDATE rawdata_stage SET unique_10_second_video_views = 0 WHERE unique_10_second_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_10_second_video_views SET DATA TYPE BIGINT USING unique_10_second_video_views::BIGINT;
UPDATE rawdata_stage SET unique_full_video_views = REPLACE(unique_full_video_views, ',', '');
UPDATE rawdata_stage SET unique_full_video_views = 0 WHERE unique_full_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_full_video_views SET DATA TYPE BIGINT USING unique_full_video_views::BIGINT;
UPDATE rawdata_stage SET unique_organic_95_percent_video_views = REPLACE(unique_organic_95_percent_video_views, ',', '');
UPDATE rawdata_stage SET unique_organic_95_percent_video_views = 0 WHERE unique_organic_95_percent_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_organic_95_percent_video_views SET DATA TYPE BIGINT USING unique_organic_95_percent_video_views::BIGINT;
UPDATE rawdata_stage SET unique_paid_95_percent_video_views = REPLACE(unique_paid_95_percent_video_views, ',', '');
UPDATE rawdata_stage SET unique_paid_95_percent_video_views = 0 WHERE unique_paid_95_percent_video_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN unique_paid_95_percent_video_views SET DATA TYPE BIGINT USING unique_paid_95_percent_video_views::BIGINT;
UPDATE rawdata_stage SET video_ad_break_ad_impressions = REPLACE(video_ad_break_ad_impressions, ',', '');
UPDATE rawdata_stage SET video_ad_break_ad_impressions = 0 WHERE video_ad_break_ad_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN video_ad_break_ad_impressions SET DATA TYPE BIGINT USING video_ad_break_ad_impressions::BIGINT;
UPDATE rawdata_stage SET video_ad_break_ad_earnings = 0 WHERE video_ad_break_ad_earnings = '';
ALTER TABLE rawdata_stage ALTER COLUMN video_ad_break_ad_earnings SET DATA TYPE BIGINT USING video_ad_break_ad_earnings::BIGINT;
UPDATE rawdata_stage SET video_ad_break_ad_cost_per_impression_cpm = 0 WHERE video_ad_break_ad_cost_per_impression_cpm = '';
ALTER TABLE rawdata_stage ALTER COLUMN video_ad_break_ad_cost_per_impression_cpm SET DATA TYPE BIGINT USING video_ad_break_ad_cost_per_impression_cpm::BIGINT;
UPDATE rawdata_stage SET youtube_premium_views = 0 WHERE youtube_premium_views = '';
ALTER TABLE rawdata_stage ALTER COLUMN youtube_premium_views SET DATA TYPE BIGINT USING youtube_premium_views::BIGINT;
UPDATE rawdata_stage SET estimated_minutes_watched = 0 WHERE estimated_minutes_watched = '';
ALTER TABLE rawdata_stage ALTER COLUMN estimated_minutes_watched SET DATA TYPE BIGINT USING estimated_minutes_watched::BIGINT;
UPDATE rawdata_stage SET estimated_premium_minutes_watched = 0 WHERE estimated_premium_minutes_watched = '';
ALTER TABLE rawdata_stage ALTER COLUMN estimated_premium_minutes_watched SET DATA TYPE BIGINT USING estimated_premium_minutes_watched::BIGINT;
UPDATE rawdata_stage SET story_taps_back = 0 WHERE story_taps_back = '';
ALTER TABLE rawdata_stage ALTER COLUMN story_taps_back SET DATA TYPE BIGINT USING story_taps_back::BIGINT;
UPDATE rawdata_stage SET story_taps_forward = 0 WHERE story_taps_forward = '';
ALTER TABLE rawdata_stage ALTER COLUMN story_taps_forward SET DATA TYPE BIGINT USING story_taps_forward::BIGINT;
UPDATE rawdata_stage SET story_exits = 0 WHERE story_exits = '';
ALTER TABLE rawdata_stage ALTER COLUMN story_exits SET DATA TYPE BIGINT USING story_exits::BIGINT;
UPDATE rawdata_stage SET story_replies = 0 WHERE story_replies = '';
ALTER TABLE rawdata_stage ALTER COLUMN story_replies SET DATA TYPE BIGINT USING story_replies::BIGINT;
UPDATE rawdata_stage SET video_added_to_playlists = 0 WHERE video_added_to_playlists = '';
ALTER TABLE rawdata_stage ALTER COLUMN video_added_to_playlists SET DATA TYPE BIGINT USING video_added_to_playlists::BIGINT;
UPDATE rawdata_stage SET subscribers_lost_from_video = 0 WHERE subscribers_lost_from_video = '';
ALTER TABLE rawdata_stage ALTER COLUMN subscribers_lost_from_video SET DATA TYPE BIGINT USING subscribers_lost_from_video::BIGINT;
UPDATE rawdata_stage SET video_removed_from_playlists = 0 WHERE video_removed_from_playlists = '';
ALTER TABLE rawdata_stage ALTER COLUMN video_removed_from_playlists SET DATA TYPE BIGINT USING video_removed_from_playlists::BIGINT;
UPDATE rawdata_stage SET annotation_impressions = 0 WHERE annotation_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN annotation_impressions SET DATA TYPE BIGINT USING annotation_impressions::BIGINT;
UPDATE rawdata_stage SET annotation_clickable_impressions = 0 WHERE annotation_clickable_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN annotation_clickable_impressions SET DATA TYPE BIGINT USING annotation_clickable_impressions::BIGINT;
UPDATE rawdata_stage SET annotation_closable_impressions = 0 WHERE annotation_closable_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN annotation_closable_impressions SET DATA TYPE BIGINT USING annotation_closable_impressions::BIGINT;
UPDATE rawdata_stage SET annotation_closes = 0 WHERE annotation_closes = '';
ALTER TABLE rawdata_stage ALTER COLUMN annotation_closes SET DATA TYPE BIGINT USING annotation_closes::BIGINT;
UPDATE rawdata_stage SET card_impressions = 0 WHERE card_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN card_impressions SET DATA TYPE BIGINT USING card_impressions::BIGINT;
UPDATE rawdata_stage SET card_teaser_impressions = 0 WHERE card_teaser_impressions = '';
ALTER TABLE rawdata_stage ALTER COLUMN card_teaser_impressions SET DATA TYPE BIGINT USING card_teaser_impressions::BIGINT;
UPDATE rawdata_stage SET card_teaser_clicks = 0 WHERE card_teaser_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN card_teaser_clicks SET DATA TYPE BIGINT USING card_teaser_clicks::BIGINT;
UPDATE rawdata_stage SET poll_votes = 0 WHERE poll_votes = '';
ALTER TABLE rawdata_stage ALTER COLUMN poll_votes SET DATA TYPE BIGINT USING poll_votes::BIGINT;

UPDATE rawdata_stage SET post_media_clicks = REPLACE(post_media_clicks, ',', '');
UPDATE rawdata_stage SET post_media_clicks = 0 WHERE post_media_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN post_media_clicks SET DATA TYPE BIGINT USING post_media_clicks::BIGINT;

UPDATE rawdata_stage SET profile_clicks = REPLACE(profile_clicks, ',', '');
UPDATE rawdata_stage SET profile_clicks = 0 WHERE profile_clicks = '';
ALTER TABLE rawdata_stage ALTER COLUMN profile_clicks SET DATA TYPE BIGINT USING profile_clicks::BIGINT;
/* remove % from value */
UPDATE rawdata_stage SET click_through_rate = REPLACE(click_through_rate, ',', '');
UPDATE rawdata_stage SET click_through_rate = 0.00 WHERE click_through_rate = '';
ALTER TABLE rawdata_stage ALTER COLUMN click_through_rate SET DATA TYPE DECIMAL(9,2) USING click_through_rate::DECIMAL(9,2);
UPDATE rawdata_stage SET engagement_rate_per_impression = REPLACE(engagement_rate_per_impression, '%', '');
UPDATE rawdata_stage SET engagement_rate_per_impression = 0.00 WHERE engagement_rate_per_impression = '';
ALTER TABLE rawdata_stage ALTER COLUMN engagement_rate_per_impression SET DATA TYPE DECIMAL(9,2) USING engagement_rate_per_impression::DECIMAL(9,2);
UPDATE rawdata_stage SET engagement_rate_per_reach = REPLACE(engagement_rate_per_reach, '%', '');
UPDATE rawdata_stage SET engagement_rate_per_reach = 0.00 WHERE engagement_rate_per_reach = '';
ALTER TABLE rawdata_stage ALTER COLUMN engagement_rate_per_reach SET DATA TYPE DECIMAL(9,2) USING engagement_rate_per_reach::DECIMAL(9,2);
UPDATE rawdata_stage SET full_video_view_rate = REPLACE(full_video_view_rate, '%', '');
UPDATE rawdata_stage SET full_video_view_rate = 0.00 WHERE full_video_view_rate = '';
ALTER TABLE rawdata_stage ALTER COLUMN full_video_view_rate SET DATA TYPE DECIMAL(9,2) USING full_video_view_rate::DECIMAL(9,2);
UPDATE rawdata_stage SET comments = 0 WHERE comments = '';
ALTER TABLE rawdata_stage ALTER COLUMN comments SET DATA TYPE BIGINT USING comments::BIGINT;

UPDATE rawdata_stage SET shares = 0 WHERE shares = '';
ALTER TABLE rawdata_stage ALTER COLUMN shares SET DATA TYPE BIGINT USING shares::BIGINT;
UPDATE rawdata_stage SET wow_reactions = 0 WHERE wow_reactions = '';
ALTER TABLE rawdata_stage ALTER COLUMN wow_reactions SET DATA TYPE BIGINT USING wow_reactions::BIGINT;
UPDATE rawdata_stage SET saves = 0 WHERE saves = '';
ALTER TABLE rawdata_stage ALTER COLUMN saves SET DATA TYPE BIGINT USING saves::BIGINT;

UPDATE rawdata_stage SET sad_reactions = 0 WHERE sad_reactions = '';
ALTER TABLE rawdata_stage ALTER COLUMN sad_reactions SET DATA TYPE BIGINT USING sad_reactions::BIGINT;
UPDATE rawdata_stage SET angry_reactions = 0 WHERE angry_reactions = '';
ALTER TABLE rawdata_stage ALTER COLUMN angry_reactions SET DATA TYPE BIGINT USING angry_reactions::BIGINT;