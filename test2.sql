SELECT
    channels.id as channels_id,
    merge_s3remote_id,
    r1.hour as hour,
    r1.MINUTE as minute,
    CONVERT(varchar(12), merge_s3remote_id) + '_' +(
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
            CAST(
                DATEPART(MINUTE, startview_datetime) / 1 AS char(2)
            ) as MINUTE,
            devices_id,
            count(*) as count_allratingrecord,
            channels_id,
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
            rating_data_2023_4
        WHERE
            startview_datetime > '2023-04-30 12:01:00'
            AND startview_datetime < '2023-04-30 12:05:00'
            AND channels_id > 0
            AND channels_id = 525
        GROUP BY
            channels_id,
            DATEPART(YEAR, startview_datetime),
            DATEPART(MONTH, startview_datetime),
            DATEPART(DAY, startview_datetime),
            DATEPART(HOUR, startview_datetime),
            DATEPART(MINUTE, startview_datetime) / 1,
            devices_id
    ) r1
    inner join channels on r1.channels_id = channels.id
where
    channels.active = 1
group by
    r1.hour,
    r1.MINUTE,
    channels.id,
    channels.merge_s3remote_id
order by
    r1.hour,
    r1.MINUTE
    /*===============================================*/
    CREATE VIEW temp_data_1683041603192423089 AS
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
    view_satalite_1683042278192634778
WHERE
    startview_datetime >= '2023-04-30 12:01:00'
    AND startview_datetime <= '2023-04-30 12:05:00'
    AND channels_id > 0
GROUP BY
    channels_id,
    devices_id
    /*===================================================*/
    /* ============ salite gender ===================== */
select
    (
        CASE
            WHEN temp_gender_1683041603192423089.gender IS NULL THEN 'none'
            WHEN temp_gender_1683041603192423089.gender = '' THEN 'none1'
            WHEN temp_gender_1683041603192423089.gender = '-' THEN 'none2'
            ELSE temp_gender_1683041603192423089.gender
        END
    ) as gender,
    merge_s3remote_id,
    Count(
        case
            when d_12_1 >= 1 then 1
        end
    ) as 'field_12_1',
    Count(
        case
            when d_12_2 >= 1 then 1
        end
    ) as 'field_12_2',
    Count(
        case
            when d_12_3 >= 1 then 1
        end
    ) as 'field_12_3',
    Count(
        case
            when d_12_4 >= 1 then 1
        end
    ) as 'field_12_4',
    channels_id,
    count(*) as total_row
from
    temp_data_1683041603192423089
    INNER JOIN channels ON temp_data_1683041603192423089.channels_id = channels.id
    left join temp_gender_1683041603192423089 on temp_data_1683041603192423089.devices_id = temp_gender_1683041603192423089.dvid
where
    channels.active = 1
    and channels_id = 525
group by
    gender,
    channels_id,
    channels.merge_s3remote_id
order by
    channels_id asc,
    temp_gender_1683041603192423089.gender asc
    /*==============================================*/
    CREATE VIEW temp_gender_1683041603192423089 AS
select
    r2.devices_id as dvid,
    r2.age,
    r2.gender
from
    (
        select
            distinct devices_id,
            (
                SELECT
                    top 1 age
                FROM
                    device_users AS dx
                WHERE
                    device_users.devices_id = dx.devices_id
            ) AS age,
            (
                select
                    top 1 gender
                from
                    device_users as dx
                where
                    device_users.devices_id = dx.devices_id
            ) as gender
        from
            device_users
    ) as r2
group by
    r2.devices_id,
    r2.gender,
    r2.age
    /*================================================*/
select
    (
        CASE
            WHEN temp_gender_1683041603192423089.gender IS NULL THEN 'none'
            WHEN temp_gender_1683041603192423089.gender = '' THEN 'none1'
            WHEN temp_gender_1683041603192423089.gender = '-' THEN 'none2'
            ELSE temp_gender_1683041603192423089.gender
        END
    ) as gender,
    merge_s3remote_id,
    Count(
        case
            when d_12_1 >= 1 then 1
        end
    ) as 'field_12_1',
    Count(
        case
            when d_12_2 >= 1 then 1
        end
    ) as 'field_12_2',
    Count(
        case
            when d_12_3 >= 1 then 1
        end
    ) as 'field_12_3',
    Count(
        case
            when d_12_4 >= 1 then 1
        end
    ) as 'field_12_4',
    channels_id,
    count(*) as total_row
from
    temp_data_1683041603192423089
    INNER JOIN channels ON temp_data_1683041603192423089.channels_id = channels.id
    left join temp_gender_1683041603192423089 on temp_data_1683041603192423089.devices_id = temp_gender_1683041603192423089.dvid
where
    channels.active = 1
    and channels_id = 525
group by
    gender,
    channels_id,
    channels.merge_s3remote_id
order by
    channels_id asc,
    temp_gender_1683041603192423089.gender asc
    /*=====================================*/
select
    (
        CASE
            WHEN temp_gender_1683041603192423089.gender IS NULL THEN 'none'
            WHEN temp_gender_1683041603192423089.gender = '' THEN 'none1'
            WHEN temp_gender_1683041603192423089.gender = '-' THEN 'none2'
            ELSE temp_gender_1683041603192423089.gender
        END
    ) as gender,
    merge_s3remote_id,
    Count(
        case
            when d_12_1 >= 1 then 1
        end
    ) as 'field_12_1',
    Count(
        case
            when d_12_2 >= 1 then 1
        end
    ) as 'field_12_2',
    Count(
        case
            when d_12_3 >= 1 then 1
        end
    ) as 'field_12_3',
    Count(
        case
            when d_12_4 >= 1 then 1
        end
    ) as 'field_12_4',
    channels_id,
    count(*) as total_row
from
    temp_data_1683041603192423089
    INNER JOIN channels ON temp_data_1683041603192423089.channels_id = channels.id
    left join temp_gender_1683041603192423089 on temp_data_1683041603192423089.devices_id = temp_gender_1683041603192423089.dvid
where
    channels.active = 1
    and channels_id = 525
group by
    gender,
    channels_id,
    channels.merge_s3remote_id
order by
    channels_id asc,
    temp_gender_1683041603192423089.gender asc
    /*=====================================================*/
    CREATE VIEW view_iptv_1683041104634470966 AS
select
    rating_data_2023_4.devices_id,
    tvchannels_id,
    chip_code,
    view_seconds,
    ip_address,
    startview_datetime,
    created
from
    S3Application.dbo.rating_data_2023_4
WHERE
    startview_datetime >= '2023-04-30 12:01:00'
    AND startview_datetime <= '2023-04-30 12:05:00'
    AND tvchannels_id = 225
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
    r1.MINUTE CREATE VIEW temp_data_iptv_1683040779110861255 AS
SELECT
    tvchannels_id,
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
    view_iptv_1683040779110861255
WHERE
    startview_datetime >= '2023-04-30 12:01:00'
    AND startview_datetime <= '2023-04-30 12:05:00'
    AND tvchannels_id > 0
GROUP BY
    tvchannels_id,
    devices_id
select
    (
        CASE
            WHEN temp_gender_1683040712576869870.gender IS NULL THEN 'none'
            WHEN temp_gender_1683040712576869870.gender = '' THEN 'none1'
            WHEN temp_gender_1683040712576869870.gender = '-' THEN 'none2'
            ELSE temp_gender_1683040712576869870.gender
        END
    ) as gender,
    merge_s3remote_id,
    Count(
        case
            when d_12_1 >= 1 then 1
        end
    ) as 'field_12_1',
    Count(
        case
            when d_12_2 >= 1 then 1
        end
    ) as 'field_12_2',
    Count(
        case
            when d_12_3 >= 1 then 1
        end
    ) as 'field_12_3',
    Count(
        case
            when d_12_4 >= 1 then 1
        end
    ) as 'field_12_4',
    channels_id,
    count(*) as total_row
from
    temp_data_1683040712576869870
    INNER JOIN channels ON temp_data_1683040712576869870.channels_id = channels.id
    left join temp_gender_1683040712576869870 on temp_data_1683040712576869870.devices_id = temp_gender_1683040712576869870.dvid
where
    channels.active = 1
    and channels_id = 525
group by
    gender,
    channels_id,
    channels.merge_s3remote_id
order by
    channels_id asc,
    temp_gender_1683040712576869870.gender asc