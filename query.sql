SELECT  * INTO #temp_data_iptv
FROM
(
	SELECT  tvchannels_id
	       ,devices_id
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 00:00:00' AND startview_datetime < '2022-02-19 05:00:00' THEN 1 END ) AS 'd_142296'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 05:00:00' AND startview_datetime < '2022-02-19 05:30:00' THEN 1 END ) AS 'd_142297'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 05:30:00' AND startview_datetime < '2022-02-19 05:55:00' THEN 1 END ) AS 'd_142298'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 05:55:00' AND startview_datetime < '2022-02-19 06:00:00' THEN 1 END ) AS 'd_142299'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 06:00:00' AND startview_datetime < '2022-02-19 06:05:00' THEN 1 END ) AS 'd_142300'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 06:05:00' AND startview_datetime < '2022-02-19 06:15:00' THEN 1 END ) AS 'd_142301'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 06:15:00' AND startview_datetime < '2022-02-19 06:25:00' THEN 1 END ) AS 'd_142302'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 06:25:00' AND startview_datetime < '2022-02-19 06:30:00' THEN 1 END ) AS 'd_142303'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 06:30:00' AND startview_datetime < '2022-02-19 06:50:00' THEN 1 END ) AS 'd_142304'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 06:50:00' AND startview_datetime < '2022-02-19 07:00:00' THEN 1 END ) AS 'd_142305'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 07:00:00' AND startview_datetime < '2022-02-19 07:05:00' THEN 1 END ) AS 'd_142306'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 07:15:00' AND startview_datetime < '2022-02-19 07:20:00' THEN 1 END ) AS 'd_142307'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 07:20:00' AND startview_datetime < '2022-02-19 07:40:00' THEN 1 END ) AS 'd_142308'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 07:40:00' AND startview_datetime < '2022-02-19 08:00:00' THEN 1 END ) AS 'd_142309'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 08:00:00' AND startview_datetime < '2022-02-19 08:05:00' THEN 1 END ) AS 'd_142310'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 08:05:00' AND startview_datetime < '2022-02-19 08:30:00' THEN 1 END ) AS 'd_142311'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 08:30:00' AND startview_datetime < '2022-02-19 10:00:00' THEN 1 END ) AS 'd_142312'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 10:00:00' AND startview_datetime < '2022-02-19 10:30:00' THEN 1 END ) AS 'd_142313'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 10:30:00' AND startview_datetime < '2022-02-19 10:55:00' THEN 1 END ) AS 'd_142314'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 10:55:00' AND startview_datetime < '2022-02-19 11:00:00' THEN 1 END ) AS 'd_142315'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 11:00:00' AND startview_datetime < '2022-02-19 11:30:00' THEN 1 END ) AS 'd_142316'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 11:30:00' AND startview_datetime < '2022-02-19 12:00:00' THEN 1 END ) AS 'd_142317'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 12:00:00' AND startview_datetime < '2022-02-19 13:00:00' THEN 1 END ) AS 'd_142318'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 13:00:00' AND startview_datetime < '2022-02-19 13:45:00' THEN 1 END ) AS 'd_142319'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 13:45:00' AND startview_datetime < '2022-02-19 14:00:00' THEN 1 END ) AS 'd_142320'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 14:00:00' AND startview_datetime < '2022-02-19 14:05:00' THEN 1 END ) AS 'd_142321'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 14:05:00' AND startview_datetime < '2022-02-19 14:50:00' THEN 1 END ) AS 'd_142322'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 14:50:00' AND startview_datetime < '2022-02-19 15:00:00' THEN 1 END ) AS 'd_142323'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 15:00:00' AND startview_datetime < '2022-02-19 16:00:00' THEN 1 END ) AS 'd_142324'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 16:00:00' AND startview_datetime < '2022-02-19 16:05:00' THEN 1 END ) AS 'd_142325'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 16:05:00' AND startview_datetime < '2022-02-19 16:30:00' THEN 1 END ) AS 'd_142326'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 16:30:00' AND startview_datetime < '2022-02-19 17:00:00' THEN 1 END ) AS 'd_142327'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 17:00:00' AND startview_datetime < '2022-02-19 17:05:00' THEN 1 END ) AS 'd_142328'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 17:05:00' AND startview_datetime < '2022-02-19 17:30:00' THEN 1 END ) AS 'd_142329'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 17:30:00' AND startview_datetime < '2022-02-19 18:00:00' THEN 1 END ) AS 'd_142330'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 18:00:00' AND startview_datetime < '2022-02-19 20:15:00' THEN 1 END ) AS 'd_142331'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 20:15:00' AND startview_datetime < '2022-02-19 21:10:00' THEN 1 END ) AS 'd_142332'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 21:10:00' AND startview_datetime < '2022-02-19 22:00:00' THEN 1 END ) AS 'd_142333'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 22:00:00' AND startview_datetime < '2022-02-19 22:05:00' THEN 1 END ) AS 'd_142334'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 22:05:00' AND startview_datetime < '2022-02-19 23:00:00' THEN 1 END ) AS 'd_142335'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 23:00:00' AND startview_datetime < '2022-02-19 23:30:00' THEN 1 END ) AS 'd_142336'
	       ,COUNT(CASE WHEN startview_datetime > '2022-02-19 23:30:00' AND startview_datetime < '2022-02-19 23:59:00' THEN 1 END ) AS 'd_142337'
	FROM S3Application.dbo.rating_data_2022_2
	WHERE startview_datetime >= '2022-02-19 00:00:01'
	AND startview_datetime <= '2022-02-19 23:59:59'
	AND tvchannels_id > 0
	AND ( tvchannels_id = 66 or tvchannels_id = 65 or tvchannels_id = 62 or tvchannels_id = 67 or tvchannels_id = 63 or tvchannels_id = 3 or tvchannels_id = 2 or tvchannels_id = 48 or tvchannels_id = 76 or tvchannels_id = 73 or tvchannels_id = 74 or tvchannels_id = 39 or tvchannels_id = 45 or tvchannels_id = 69 or tvchannels_id = 64 )
	GROUP BY  tvchannels_id
	         ,devices_id
) AS r1