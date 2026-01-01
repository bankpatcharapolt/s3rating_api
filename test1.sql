SELECT
    r1.tvchannels_id,
    r1.hour as hour,
    r1.MINUTE as minute,
    CONVERT(varchar(12), tvchannels_id) + '_' +(
        CONVERT(varchar(12), r1.hour) + '_' + CONVERT(varchar(12), r1.MINUTE)
    ) AS h_m,
    Count(
        case
            when r1.d_12_1 >= 1 then 1
        end
    ) as 'field_12_1',
    Count(
        case
            when r1.d_12_2 >= 1 then 1
        end
    ) as 'field_12_2',
    Count(
        case
            when r1.d_12_3 >= 1 then 1
        end
    ) as 'field_12_3',
    Count(
        case
            when r1.d_12_4 >= 1 then 1
        end
    ) as 'field_12_4'
FROM
    (
        SELECT
            DATEPART(HOUR, startview_datetime) as hour,
            DATEPART(MINUTE, startview_datetime) / 1 as MINUTE,
            devices_id,
            count(*) as count_allratingrecord,
            tvchannels_id,
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
            [S3Application].dbo.rating_data_2023_4
        WHERE
            startview_datetime > '2023-04-30 12:01:00'
            AND startview_datetime < '2023-04-30 12:05:00'
            AND tvchannels_id > 0
            AND tvchannels_id = 225
        GROUP BY
            tvchannels_id,
            DATEPART(YEAR, startview_datetime),
            DATEPART(MONTH, startview_datetime),
            DATEPART(DAY, startview_datetime),
            DATEPART(HOUR, startview_datetime),
            DATEPART(minute, startview_datetime) / 1,
            devices_id
    ) r1
group by
    r1.hour,
    r1.MINUTE,
    r1.tvchannels_id
order by
    r1.tvchannels_id,
    r1.hour,
    r1.MINUTE