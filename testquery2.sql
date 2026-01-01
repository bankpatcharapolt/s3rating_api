SELECT  merge_s3remote_id
       ,channels_id
       ,COUNT(case WHEN overview >= 1 THEN 1 end) AS 'overview'
FROM #temp_data_overview
INNER JOIN channels
ON #temp_data_overview.channels_id = channels.id
WHERE channels.active = 1
GROUP BY  channels_id
         ,merge_s3remote_id