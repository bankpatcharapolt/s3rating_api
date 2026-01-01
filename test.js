const dateString = "2024-03-31";
const dateParts = dateString.split("-");
const year = parseInt(dateParts[0]);
const month = parseInt(dateParts[1]);

let nextMonth = month + 1;
let nextYear = year;
if (nextMonth > 12) {
    nextMonth = 1;
    nextYear++;
}

// Format next month and year as two digits with leading zeros if necessary
const nextMonthStr = nextMonth < 10 ? `0${nextMonth}` : `${nextMonth}`;
const nextYearStr = `${nextYear}`;

const rating_data_table_current_month = `rating_data_${year}_${dateParts[1]}`;
const rating_data_table_next_month = `rating_data_${nextYearStr}_${nextMonthStr}`;

let query = `INSERT INTO ${temptablename}
             SELECT devices_id,
                    channels_id,
                    ship_code,
                    frq,
                    sym,
                    pol,
                    server_id,
                    vdo_pid,
                    ado_pid,
                    view_seconds,
                    ip_address,
                    startview_datetime,
                    channel_ascii_code,
                    created 
             FROM ${rating_data_table_current_month}
             WHERE startview_datetime >= '${dateString} 00:00:01'
               AND startview_datetime <= '${dateString} 23:59:59'`;

if (channels_id != null) {
    query += ` AND channels_id = ${channels_id}`;
}

query += ` UNION ALL
           SELECT devices_id,
                  channels_id,
                  ship_code,
                  frq,
                  sym,
                  pol,
                  server_id,
                  vdo_pid,
                  ado_pid,
                  view_seconds,
                  ip_address,
                  startview_datetime,
                  channel_ascii_code,
                  created 
           FROM ${rating_data_table_next_month}
           WHERE startview_datetime >= '${nextYearStr}-${nextMonthStr}-01 00:00:01'
             AND startview_datetime <= '${nextYearStr}-${nextMonthStr}-31 23:59:59'`;

if (channels_id != null) {
    query += ` AND channels_id = ${channels_id}`;
}