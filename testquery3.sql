SELECT  merge_s3remote_id
       ,channels_id
       ,(CASE WHEN province_region IS NULL THEN 'none' WHEN province_region = '' THEN 'none1' WHEN province_region = '-' THEN 'none2' ELSE province_region END) AS province_region
       ,COUNT(case WHEN d_142338 >= 1 THEN 1 end) AS 'field_142338'
       ,COUNT(case WHEN d_142339 >= 1 THEN 1 end) AS 'field_142339'
       ,COUNT(case WHEN d_142340 >= 1 THEN 1 end) AS 'field_142340'
       ,COUNT(case WHEN d_142341 >= 1 THEN 1 end) AS 'field_142341'
       ,COUNT(case WHEN d_142342 >= 1 THEN 1 end) AS 'field_142342'
       ,COUNT(case WHEN d_142343 >= 1 THEN 1 end) AS 'field_142343'
       ,COUNT(case WHEN d_142344 >= 1 THEN 1 end) AS 'field_142344'
       ,COUNT(case WHEN d_142345 >= 1 THEN 1 end) AS 'field_142345'
       ,COUNT(case WHEN d_142346 >= 1 THEN 1 end) AS 'field_142346'
       ,COUNT(case WHEN d_142347 >= 1 THEN 1 end) AS 'field_142347'
       ,COUNT(case WHEN d_142348 >= 1 THEN 1 end) AS 'field_142348'
       ,COUNT(case WHEN d_142349 >= 1 THEN 1 end) AS 'field_142349'
       ,COUNT(case WHEN d_142350 >= 1 THEN 1 end) AS 'field_142350'
       ,COUNT(case WHEN d_142351 >= 1 THEN 1 end) AS 'field_142351'
       ,COUNT(case WHEN d_142352 >= 1 THEN 1 end) AS 'field_142352'
       ,COUNT(case WHEN d_142353 >= 1 THEN 1 end) AS 'field_142353'
       ,COUNT(case WHEN d_142354 >= 1 THEN 1 end) AS 'field_142354'
       ,COUNT(case WHEN d_142355 >= 1 THEN 1 end) AS 'field_142355'
       ,COUNT(case WHEN d_142356 >= 1 THEN 1 end) AS 'field_142356'
       ,COUNT(case WHEN d_142357 >= 1 THEN 1 end) AS 'field_142357'
       ,COUNT(case WHEN d_142358 >= 1 THEN 1 end) AS 'field_142358'
       ,COUNT(case WHEN d_142359 >= 1 THEN 1 end) AS 'field_142359'
       ,COUNT(case WHEN d_142360 >= 1 THEN 1 end) AS 'field_142360'
       ,COUNT(case WHEN d_142379 >= 1 THEN 1 end) AS 'field_142379'
       ,COUNT(case WHEN d_142362 >= 1 THEN 1 end) AS 'field_142362'
       ,COUNT(case WHEN d_142363 >= 1 THEN 1 end) AS 'field_142363'
       ,COUNT(case WHEN d_142364 >= 1 THEN 1 end) AS 'field_142364'
       ,COUNT(case WHEN d_142365 >= 1 THEN 1 end) AS 'field_142365'
       ,COUNT(case WHEN d_142366 >= 1 THEN 1 end) AS 'field_142366'
       ,COUNT(case WHEN d_142367 >= 1 THEN 1 end) AS 'field_142367'
       ,COUNT(case WHEN d_142368 >= 1 THEN 1 end) AS 'field_142368'
       ,COUNT(case WHEN d_142369 >= 1 THEN 1 end) AS 'field_142369'
       ,COUNT(case WHEN d_142370 >= 1 THEN 1 end) AS 'field_142370'
       ,COUNT(case WHEN d_142371 >= 1 THEN 1 end) AS 'field_142371'
       ,COUNT(case WHEN d_142372 >= 1 THEN 1 end) AS 'field_142372'
       ,COUNT(case WHEN d_142373 >= 1 THEN 1 end) AS 'field_142373'
       ,COUNT(case WHEN d_142374 >= 1 THEN 1 end) AS 'field_142374'
       ,COUNT(case WHEN d_142375 >= 1 THEN 1 end) AS 'field_142375'
       ,COUNT(case WHEN d_142376 >= 1 THEN 1 end) AS 'field_142376'
       ,COUNT(case WHEN d_142377 >= 1 THEN 1 end) AS 'field_142377'
       ,COUNT(case WHEN d_142378 >= 1 THEN 1 end) AS 'field_142378'
FROM #temp_data
LEFT JOIN device_addresses
ON #temp_data.devices_id = device_addresses.devices_id
INNER JOIN channels
ON #temp_data.channels_id = channels.id
WHERE #temp_data.channels_id = 252
GROUP BY  province_region
         ,#temp_data.channels_id
         ,merge_s3remote_id