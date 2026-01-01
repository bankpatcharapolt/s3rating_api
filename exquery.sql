CREATE VIEW temp_data_1683043544005219770 AS
SELECT
    channels_id,
    devices_id,
    COUNT(
        CASE
            WHEN startview_datetime > '2023-04-30 12:01:00'
            AND startview_datetime < '2023-04-30 12:02:00' THEN 1
        END
    ) AS 'd_12_1',
    COUNT(
        CASE
            WHEN startview_datetime > '2023-04-30 12:02:00'
            AND startview_datetime < '2023-04-30 12:03:00' THEN 1
        END
    ) AS 'd_12_2',
    COUNT(
        CASE
            WHEN startview_datetime > '2023-04-30 12:03:00'
            AND startview_datetime < '2023-04-30 12:04:00' THEN 1
        END
    ) AS 'd_12_3',
    COUNT(
        CASE
            WHEN startview_datetime > '2023-04-30 12:04:00'
            AND startview_datetime < '2023-04-30 12:05:00' THEN 1
        END
    ) AS 'd_12_4'
FROM
    view_satalite_1683043544005219770
WHERE
    startview_datetime >= '2023-04-30 12:01:00'
    AND startview_datetime <= '2023-04-30 12:05:00'
    AND channels_id > 0
GROUP BY
    channels_id,
    devices_id