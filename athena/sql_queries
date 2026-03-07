-- Create analytics database
CREATE DATABASE db_yt_analytics;

-- Validate cleaned dataset
SELECT * FROM "AwsDataCatalog"."yt-db"."yt_cleaned" WHERE id =1;

-- Join raw statistics with cleaned category data
SELECT * FROM "yt-db"."raw_statistics_6c345fb329aefb538d680cb4c8763d6a" a INNER JOIN "yt-db"."yt_cleaned" b ON a.category_id = b.id WHERE a.region = 'ca';

-- Query final analytics dataset
SELECT * FROM "db_yt_analytics"."final_analytics" LIMIT 200;

-- Region-wise distribution of trending videos
SELECT region, COUNT(*) AS total_videos FROM "db_yt_analytics"."final_analytics" GROUP BY region ORDER BY total_videos DESC;
