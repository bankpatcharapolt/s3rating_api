var config = require('./dbconfig');
var config_s3app = require('./dbconfig_s3app');
var config_pg = require('./dbconfig_pg');
const decache = require('decache');

//const moment = require('moment');
const moment = require('moment-timezone');
const util = require('util');
var http = require('http');
var url = require('url') ;
var fs = require('fs');
var lodash  = require('lodash');
var JSONStream = require('JSONStream');
var es = require('event-stream');
const { readFileSync } = require('fs');
const sql = require('mssql');
const {
    response
} = require('express');
const { redirect } = require('express/lib/response');
const { time } = require('console');
const { result } = require('lodash');
// const pgp = require('pg-promise')(); // load module : postgresql database 
const Pool = require('pg').Pool // load module : postgresql database 

/**
 * =====================
 * API : แสดงรายการคนดูตามจังหวัด
 * =====================
 */
async function get_provinceviewer(date, channel_id) {
    try {
        let pool = await sql.connect(config);

        var month = get_dateobject(date, "month");
        var year = get_dateobject(date, "year");
        var rating_data_daily_device_table = "rating_data_daily_devices_" + year + "_" + month;

        // case : เรียกข้อมูล rating ของวันนั้น
        let rating_data_daily = await pool.request()
            .input('input_parameter_date', sql.VarChar, date)
            .input('input_parameter_channel', sql.Int, channel_id)
            .query("SELECT * from rating_data_daily where  date = @input_parameter_date and channels_id = @input_parameter_channel");
        if (rating_data_daily != null) {
            let rating_data_daily_obj = rating_data_daily.recordsets[0];
            var rating_data_daily_id = JSON.parse(JSON.stringify(rating_data_daily_obj));

            if (rating_data_daily_id != null) {
                var rating_data_daily_id = rating_data_daily_id[0].id;

                let rating_data_dailydevice = await pool.request()
                    .input('input_parameter_id', sql.Int, rating_data_daily_id)
                    .query("select count(*) as all_devices , region_name,provinces.id as province_id  from " + rating_data_daily_device_table + " left join device_addresses on  " + rating_data_daily_device_table + ".devices_id  = device_addresses.devices_id inner join provinces on provinces.ipstack_province_name  = device_addresses.region_name  where rating_data_daily_id = @input_parameter_id  group by device_addresses.region_name , provinces.id  order by provinces.id asc");

                if (rating_data_dailydevice != null) {
                    return [{
                        "status": true,
                        data: rating_data_dailydevice.recordsets[0],
                        "result_code": "000",
                        "result_desc": "Success"
                    }];
                } else {
                    return [{
                        "status": false,
                        "result_code": "-005",
                        "result_desc": "not found data"
                    }];
                }
            }


        } else {
            return [{
                "status": false,
                "result_code": "-005",
                "result_desc": "not found data"
            }];
        }

    } catch (error) {
        return [{
            "status": false,
            "result_code": "-005",
            "result_desc": "not found data"
        }];
    }
}

async function get_deviceviewerbyprovince(date, channel_id, province_id) {
    try {
        let pool = await sql.connect(config);

        var month = get_dateobject(date, "month");
        var year = get_dateobject(date, "year");
        var rating_data_daily_device_table = "rating_data_daily_devices_" + year + "_" + month;

        // case : เรียกข้อมูล rating ของวันนั้น
        let rating_data_daily = await pool.request()
            .input('input_parameter_date', sql.VarChar, date)
            .input('input_parameter_channel', sql.Int, channel_id)
            .query("SELECT * from rating_data_daily where  date = @input_parameter_date and channels_id = @input_parameter_channel");
        if (rating_data_daily != null) {
            let rating_data_daily_obj = rating_data_daily.recordsets[0];
            var rating_data_daily_id = JSON.parse(JSON.stringify(rating_data_daily_obj));

            if (rating_data_daily_id != null) {
                var rating_data_daily_id = rating_data_daily_id[0].id;

                let rating_data_dailydevice = await pool.request()
                    .input('input_parameter_id', sql.Int, rating_data_daily_id)
                    .input('input_parameter_provinceid', sql.Int, province_id)
                    .query("select " + rating_data_daily_device_table + ".devices_id," + rating_data_daily_device_table + ".ship_code, region_name,device_addresses.latitude , device_addresses.longitude from " + rating_data_daily_device_table + " left join device_addresses on  " + rating_data_daily_device_table + ".devices_id  = device_addresses.devices_id inner join provinces on provinces.ipstack_province_name  = device_addresses.region_name  where rating_data_daily_id = @input_parameter_id and provinces.id = @input_parameter_provinceid ");

                if (rating_data_dailydevice != null) {
                    return [{
                        "status": true,
                        data: rating_data_dailydevice.recordsets[0],
                        "result_code": "000",
                        "result_desc": "Success"
                    }];
                } else {
                    return [{
                        "status": false,
                        "result_code": "-005",
                        "result_desc": "not found data"
                    }];
                }
            } else {
                return [{
                    "status": false,
                    "result_code": "-005",
                    "result_desc": "not found data"
                }];
            }


        }

    } catch (error) {
        return [{
            "status": false,
            "result_code": "-005",
            "result_desc": "not found data"
        }];
    }

}
async function get_deviceviewer_perminute(date, channels_id) {


    let pool = await sql.connect(config);

    var month = get_dateobject(date, "month");
    var year = get_dateobject(date, "year");
    var rating_data_table = "rating_data_" + year + "_" + month;

    let $query = build_query( "minute" , rating_data_table , channels_id , date);
    let rating_data_daily = await pool.request()
        .query($query);
    if (rating_data_daily != null) {
        let rating_data_daily_obj = rating_data_daily.recordsets;
        var rating_data_daily_id = JSON.parse(JSON.stringify(rating_data_daily_obj));
        if (rating_data_daily_id != null) {
            return [{
                "status": true,
                "data": rating_data_daily_obj,
                "result_code": "000",
                "result_desc": "Success"
            }];
        } else {
            return [{
                "status": false,
                "result_code": "-005",
                "result_desc": "not found data"
            }];
        }


    }
}

const get_tvprogram_recordset = async (channels_id  = null , date = null) => {
    let pool = await sql.connect(config);
  
    let $query_tvprogram= get_tvprogram( channels_id  , date);
    var obj;
    let tvprogram     = await pool.request()
    .query($query_tvprogram);
    let tvprogram_recordset = tvprogram.recordsets;
    
    if(tvprogram_recordset.length != 0){
        obj = JSON.parse(JSON.stringify(tvprogram_recordset));
        //channels   =  channels_obj;
        return [{
            "status": true,
            "data" : obj,
            "result_code": "000",
            "result_desc": "tvprogram item not found."
        }]
       
    }else{
        return [{
            "status": false,
            "result_code": "-005",
            "result_desc": "tvprogram item not found."
        }]
    }
    return obj;
  

}


function query_overviewtemptable($type , temptablename , date , channels_id = null , start_datetime_string = null , end_datetime_string = null , rating_data_table_ = null , view_table = null){
    let $query = "";
    // 
    if($type == "satalite"){
        $query+= `
        IF OBJECT_ID('tempdb..${temptablename}') IS NOT NULL DROP TABLE ${temptablename}
            create table ${temptablename} (id int IDENTITY(1,1) primary key,devices_id  int,channels_id int,ship_code varchar(100)
            ,frq varchar(50)
            ,sym varchar(50)
            ,pol varchar(50)
            ,server_id  varchar(50)
            ,vdo_pid varchar(50)
            ,ado_pid varchar(50)
            ,view_seconds int
            ,ip_address varchar(50)
            ,startview_datetime datetime
            ,channel_ascii_code int
            ,created datetime
            )`;
           
    }else if($type == "satalite_insert"){
        let rating_data_table = get_ratingdatatable( date);
        if(rating_data_table_ != null){
            rating_data_table = rating_data_table_;
        }
        let $start_datetimestring_condition =   `${date} 00:00:01`;
        if(start_datetime_string != null){
            $start_datetimestring_condition = start_datetime_string;
        } 
        let $end_datetimestring_condition   = `${date} 23:59:59`;
        if(end_datetime_string != null){
            $end_datetimestring_condition = end_datetime_string;
        } 
        if(view_table != null){
            $query +=`CREATE VIEW   ${view_table} AS
            select ${rating_data_table}.devices_id,channels_id,ship_code,
            frq,sym,pol,server_id,vdo_pid,ado_pid,view_seconds,ip_address
            ,startview_datetime,channel_ascii_code
            ,created from ${rating_data_table} 
            WHERE startview_datetime >= '${$start_datetimestring_condition}'
            AND startview_datetime <= '${$end_datetimestring_condition}'`;

            if(channels_id != null){
                $query += "   AND channels_id = " + channels_id;
            }
        }else{
            $query +=`INSERT INTO ${temptablename}
            select ${rating_data_table}.devices_id,channels_id,ship_code,
            frq,sym,pol,server_id,vdo_pid,ado_pid,view_seconds,ip_address
            ,startview_datetime,channel_ascii_code
            ,created from ${rating_data_table} 
            WHERE startview_datetime >= '${$start_datetimestring_condition}'
            AND startview_datetime <= '${$end_datetimestring_condition}'`;

            if(channels_id != null){
                $query += "   AND channels_id = " + channels_id;
            }
        }
        
    }else if($type == "iptv"){
        // 
        $query+= `
        IF OBJECT_ID('tempdb..${temptablename}') IS NOT NULL DROP TABLE ${temptablename}
            create table ${temptablename} (id int IDENTITY(1,1) primary key ,devices_id  int,tvchannels_id int,chip_code varchar(100)
            ,view_seconds int
            ,ip_address varchar(50)
            ,startview_datetime datetime
            ,created datetime
            )`;
    }else if($type == "iptv_insert"){
        let rating_data_table = get_iptvratingdatatable( date);
        if(rating_data_table_ != null){
            rating_data_table = rating_data_table_;
        }
        let $start_datetimestring_condition =   `${date} 00:00:01`;
        if(start_datetime_string != null){
            $start_datetimestring_condition = start_datetime_string;
        } 
        let $end_datetimestring_condition   = `${date} 23:59:59`;
        if(end_datetime_string != null){
            $end_datetimestring_condition = end_datetime_string;
        } 
        if(view_table != null){
            $query +=`CREATE VIEW   ${view_table} AS
            select ${rating_data_table}.devices_id,tvchannels_id,chip_code,view_seconds
            ,ip_address,startview_datetime,created from ${config_s3app.database+".dbo."}${rating_data_table} 
            WHERE startview_datetime >= '${$start_datetimestring_condition}'
            AND startview_datetime <= '${$end_datetimestring_condition}'`;

            if(channels_id != null){
                $query += "   AND tvchannels_id = " + channels_id;
            }
        }else{
            $query +=`INSERT INTO ${temptablename}
            select ${rating_data_table}.devices_id,tvchannels_id,chip_code,view_seconds
            ,ip_address,startview_datetime,created from ${config_s3app.database+".dbo."}${rating_data_table} 
            WHERE startview_datetime >= '${$start_datetimestring_condition}'
            AND startview_datetime <= '${$end_datetimestring_condition}'`;

            if(channels_id != null){
                $query += "   AND tvchannels_id = " + channels_id;
            }
        }
    }
    return $query;
}

function get_iptvratingdatatable(date){
    

    var month = get_dateobject(date, "month");
    var year = get_dateobject(date, "year");
    var rating_data_table =  "rating_data_" + year + "_" + month;

    return rating_data_table;

}

function check_islastdateofmonth(date){
    var month = get_dateobject(date, "month");
    var year = get_dateobject(date, "year");
    var day  = get_dateobject(date , "day");
   // console.log(month);
    if(month == 2 && (day == 28 || day == 29)){
        return true;

    }else if((month == 1  || month == 3
        || month == 5 || month == 7 
        || month == 8 || month == 10
        || month == 12 ) && day == 31){
            return true;
    }else if((month == 4  || month == 6
        || month == 9 || month == 11  ) && day == 30){
            return true;
    }

    return false;


}
function set_datatable(date , type = null){
    let rating_data_table_;
    var month = get_dateobject(date, "month");
    var year = get_dateobject(date, "year");
    var day  = get_dateobject(date , "day");
    let m = month;
    let y = year;
    if(month < 12){
        m = m +1;
    }
    else{
        m = 1;
        y  = y + 1;
    }
     rating_data_table_ = "rating_data_" + y + "_" + m;
    return rating_data_table_;
}
const create_temptblsatalite = async (pool , date  = null , config = null , channels_id =  null , start_datetime_string = null , end_datetime_string = null , view_table = null ) => {

    let islastdate = check_islastdateofmonth(date);
  
  
    if(view_table != null){
        let req = new sql.Request(pool);
        let $query_inserttablesatalite  = query_overviewtemptable("satalite_insert" , view_table , date , channels_id ,  start_datetime_string  , end_datetime_string  , null , view_table)
        let rows = await req.batch($query_inserttablesatalite);
        if(islastdate == true){
            // let rating_data_table_ =set_datatable(date )
            // await req.batch( "drop view " + view_table ); // case : drop temp table
            // let $query_inserttablesatalite  = query_overviewtemptable("satalite_insert" , view_table , date , channels_id ,  start_datetime_string  , end_datetime_string  , rating_data_table_ , view_table)
            // let rows = await req.batch($query_inserttablesatalite);
        }
      
        return [{
            "status": true,
            "req" : req,
            data: view_table,
            "result_code": "000",
            "result_desc": "Success"
        }];
    }else{
        let $query_temptablesatalite     =query_overviewtemptable( "satalite" , "#temp_table_satalite" ,null );
        let req = new sql.Request(pool);
        await req.batch($query_temptablesatalite);

        let $query_inserttablesatalite  = query_overviewtemptable("satalite_insert" , "#temp_table_satalite" , date , channels_id ,  start_datetime_string  , end_datetime_string )
        let rows = await req.batch($query_inserttablesatalite);
        if(islastdate == true){
            // let rating_data_table_ =set_datatable(date )
            // let $query_inserttablesatalite  = query_overviewtemptable("satalite_insert" , "#temp_table_satalite" , date , channels_id ,  start_datetime_string  , end_datetime_string  , rating_data_table_)
            // rows = await req.batch($query_inserttablesatalite);
        }

        if(rows.rowsAffected > 0){
            return [{
                "status": true,
                "req" : req,
                data: "#temp_table_satalite",
                "result_code": "000",
                "result_desc": "Success"
            }];
           
        }else{
            return [{
                "status": true,
                "req" : req,
                data: "#temp_table_satalite",
                "result_code": "-005",
                "result_desc": "not found channel"
            }];
        }
    }
  
    
    return channels;
  

}
const create_temptbliptv = async (pool , date  = null , config = null , channels_id  = null , start_datetime_string = null , end_datetime_string = null , view_table = null) => {

    let islastdate = check_islastdateofmonth(date);
    if(view_table != null){

        let req = new sql.Request(pool);
        let $query_inserttableiptv  = query_overviewtemptable("iptv_insert" ,view_table , date , channels_id ,  start_datetime_string  , end_datetime_string  , null ,view_table)
       // console.log($query_inserttableiptv); return;
        let rows = await req.batch($query_inserttableiptv);
        if(islastdate == true){
            let rating_data_table_ =set_datatable(date  , 'iptv');
            // let $query_inserttableiptv  = query_overviewtemptable("iptv_insert" ,view_table , date , channels_id ,  start_datetime_string  , end_datetime_string , rating_data_table_ ,view_table)
            // //console.log($query_inserttableiptv);    
            // await req.batch( "drop view " + view_table ); // case : drop temp table
            // let rows = await req.batch($query_inserttableiptv);
        }

        return [{
            "status": true,
            "req" : req,
            data: view_table,
            "result_code": "000",
            "result_desc": "Success"
        }];
        
    }else{
        let $query_temptableiptv     =query_overviewtemptable( "iptv" , "#temp_table_iptv");
        //console.log($query_temptableiptv);
        let req = new sql.Request(pool);
        await req.batch($query_temptableiptv);

        let $query_inserttableiptv  = query_overviewtemptable("iptv_insert" , "#temp_table_iptv" , date , channels_id ,  start_datetime_string  , end_datetime_string )
        //console.log($query_inserttableiptv);    
        let rows = await req.batch($query_inserttableiptv);

        if(islastdate == true){
            // let rating_data_table_ =set_datatable(date  , 'iptv');
            // let $query_inserttableiptv  = query_overviewtemptable("iptv_insert" , "#temp_table_iptv" , date , channels_id ,  start_datetime_string  , end_datetime_string , rating_data_table_ )
            // //console.log($query_inserttableiptv);    
            // rows = await req.batch($query_inserttableiptv);
        }

    
        if(rows.rowsAffected > 0){
            return [{
                "status": true,
                "req" : req,
                data: "#temp_table_iptv",
                "result_code": "000",
                "result_desc": "Success"
            }];
        
        }else{
            return [{
                "status": true,
                "req" : req,
                data: "#temp_table_iptv",
                "result_code": "-005",
                "result_desc": "not found channel"
            }];

        }
    }
    return channels;
  

}

function set_datetime(date){
    var date =set_timezone(date, "Asia/Jakarta");
   // dt.getDate() +  "/"  + (dt.getMonth() + 1) +  "/"  + dt.getFullYear()
    var fullyear  = date.getFullYear();
    var month     = date.getMonth() + 1;
    month =  month < 10 ? "0"+month : month;
    var d         = date.getDate();
    d =  d < 10 ? "0"+d : d;
    var st_h  = date.getUTCHours();
    st_h =  st_h < 10 ? "0"+st_h : st_h;
    var st_m  = date.getUTCMinutes();
    st_m =  st_m < 10 ? "0"+st_m : st_m;
    var st_c  = date.getUTCSeconds();
    st_c =  st_c < 10 ? "0"+st_c : st_c;

    return fullyear + "-" + month + "-"+ d+" "+ st_h + ":" + st_m + ":" + st_c ;
}

function getmm_datetime(mm){

    var month = mm.format('MM');
    var day   = mm.format('DD');
    var year  = mm.format('YYYY');
    var hour  = mm.format('HH');
    var minute= mm.format('mm');
    var sec   = mm.format('ss')

    return year +"-" + month + "-"+day + " " + hour + ":" + minute + ":" + sec;
}
function get_hourminute(timeperiods){
    // var timeperiod =set_timezone(timeperiods, "Asia/Jakarta");
    let timeperiod = new Date(timeperiods);
    var st_h  = timeperiod.getHours();
    st_h =  st_h < 10 ? "0"+st_h : st_h;
    var st_m  = timeperiod.getMinutes();
    st_m =  st_m < 10 ? "0"+st_m : st_m;


    return st_h + ":" + st_m;
}
function get_timeinterval(startString =null, endString =null,  time_interval = null , report_type = null , time_start = null, time_end = null) {
   
    var start = moment.tz(startString + ' 05:00', 'Asia/Jakarta');
    var end   = moment.tz(endString   +' 23:59', 'Asia/Jakarta');
   
    var date_arr = [];
    /* ================= case : report  รายวันไฟล์เดียวลูกค้าจ่าย requirment ให้เอาช่วงเวลา 05.00 - 24.00  ================= */
    if(time_interval == "all"){
       
        start = moment.tz(startString + ' 05:00', 'Asia/Jakarta');
        end   = moment.tz(endString   +' 23:59', 'Asia/Jakarta');
        var arr = [];
        let h_m  =  moment(start).hour() + "_" + moment(start).minute();
        let start_date  = getmm_datetime(start);
        arr.push(start_date);
        let end_date  = getmm_datetime(end);
        arr.push(end_date);
        arr.push(h_m);
        date_arr.push([arr]);
        return date_arr;
    }
   
    /** ================= (eof )case : report  รายวันไฟล์เดียวลูกค้าจ่าย requirment ให้เอาช่วงเวลา 05.00 - 24.00  =================  */
     /* ================= case : report ข้อมูลรายละเอียดต่อนาทีลูกค้า requirment ให้เอาช่วงเวลา 00.00- 01.00 ,  05.00 - 24.00  ================= */
    if(report_type == "avgview"){
        start = moment.tz(startString + ' 00:00', 'Asia/Jakarta');
        end   = moment.tz(endString   +' 23:59', 'Asia/Jakarta');
    }

    if(report_type == "statistic_api"){
        start = moment.tz(startString +' '+ time_start , 'Asia/Jakarta');
        end   = moment.tz(endString + ' ' + time_end, 'Asia/Jakarta');

        var arr = [];
        let h_m  =  moment(start).hour() + "_" + moment(start).minute();
        let start_date  = getmm_datetime(start);
        arr.push(start_date);
        let end_date  = getmm_datetime(end);
        arr.push(end_date);
        arr.push(h_m);
        date_arr.push([arr]);
        return date_arr;
       
    }
    if(report_type == "channel_rating_api"){
        start = moment.tz(startString +' '+ time_start , 'Asia/Jakarta');
        end   = moment.tz(endString + ' ' + time_end, 'Asia/Jakarta');
    }


    /* ================= case : report  รายวันไฟล์เดียวลูกค้าจ่าย requirment ให้เอาช่วงเวลา 00.00- 01.00  ,05.00 - 24.00  ================= */
    while(start < end) {
        var arr = [];
        let h_m  =  moment(start).hour() + "_" + moment(start).minute();
     
        if(report_type == "avgview" ){
            /** ================= case : ไม่เอาช่วงเวลา 01.00 - 05.00 =================   */
                let except_time = moment.tz(startString + ' 01:00:00', 'Asia/Jakarta'); // excpet time : 01.00 - 05.00
                let except_time2 = moment.tz(startString + ' 04:59:59', 'Asia/Jakarta'); 
            
                let start_date  = getmm_datetime(start);
            
                if(start.isBefore(except_time) || start.isAfter(except_time2)){
                
                    arr.push(start_date);
                    start = start.add(time_interval, 'minutes'); // case : set end 
                    let end = start.format();
                    let end_date  = getmm_datetime(start);
                    arr.push(end_date);
                    arr.push(h_m);
                    date_arr.push([arr]);
                }else{
                    start = start.add(time_interval, 'minutes'); // case : set end 
                    let end = start.format();
                    let end_date  = getmm_datetime(start);
                }
              /** ================= (eof) case :ไม่เอาช่วงเวลา  01.00 - 05.00 =================   */
        }else if(report_type == 'channel_rating_api')
        {
            let start_date  = getmm_datetime(start);
            arr.push(start_date);
            start = start.add( 1 , 'minutes'); // case : get every 1
            let end = start.format();
            let end_date  = getmm_datetime(start);
            arr.push(end_date);
            arr.push(h_m);
            date_arr.push([arr]);
        }
        else{
            let start_date  = getmm_datetime(start);
            arr.push(start_date);
            

            start = start.add(time_interval, 'minutes'); // case : set end 
            let end = start.format();
            let end_date  = getmm_datetime(start);
            arr.push(end_date);

            arr.push(h_m);

        

            date_arr.push([arr]);
        }
    }
    return date_arr;
}

function query_deviceusertable(report_type = null){

    if(report_type == null){
        $query = `( select    distinct  devices_id
            ,(select top 1 gender from device_users as dx  where device_users.devices_id = dx.devices_id
            )  as gender from device_users ) as r2 `
    }else if(report_type == 'statistic_api'){
        $query = `( select    distinct  devices_id,(
            SELECT  top 1 age
            FROM device_users AS dx
            WHERE device_users.devices_id = dx.devices_id ) AS age
            ,(select top 1 gender from device_users as dx  where device_users.devices_id = dx.devices_id
            )  as gender from device_users ) as r2 `
    }
    return $query;
}

function count_deviceregister(datetime){
    $query = ` select count(*)  as all_devices from devices where created <= '${datetime}' `;
    return $query;
}

function test(){
    $channel_top20array = tvdigitalchannel_ondemand("satalite" , null); // # find top20 channel from satalite channel id
    $channel_top20array.forEach(channel_id => {
        console.log(channel_id);
        var fixed_channelarray = [ channel_id ];
        let obj_gender_self     =  find_channeldelete_specificarray(  "satalite" , './json/overview/tvprogram_baseon_tpbs/2022_2_20/logmergegendertop20_1649670900.json' , fixed_channelarray); // get only this channel gender object
        console.log(obj_gender_self);
    });
    return;
}

function generation_randomnumber(){

    var random_number =  Math.floor(Math.random() * 1000000) + 1;
    return random_number;
}

function test_province(){
    let sum_array_provinceregion_all  = sum_array_provinceregion("./json/overview/tvprogram_baseon_tpbs/2022_4_11/logprovinceall_1649749843.json"); //  sum everychannel base on time slot
    let sum_array_province_all  =  sum_array_province("./json/overview/tvprogram_baseon_tpbs/2022_4_11/logprovincemergedata_1649749845.json");
   
    
    key_regionbangkok_found   = get_keybyvalue( sum_array_provinceregion_all, "Bangkok" , "element.province_region");
    key_provincebangkok_found = get_keybyvalue( sum_array_province_all , 10 , "find_provincekey");
  
    if(key_regionbangkok_found >= 0 && key_regionbangkok_found != undefined){
          // region_code= 10  , province_name = กรุงเทพมหานคร   
          if(key_provincebangkok_found >= 0 && key_provincebangkok_found != undefined){
              let province_bangkok   = sum_array_province_all[key_provincebangkok_found]; 
              let region_bangkok     = sum_array_provinceregion_all[key_regionbangkok_found];
              sum_array_province_all[key_provincebangkok_found]  = [];
              sum_array_province_all[key_provincebangkok_found]  =    sum_array_provinceregion_all[key_regionbangkok_found];
             
          }else{
      
          }
    }
}

function sort_province_top5peak(arr , value){

    var var_merge_b = `b.field_${value}`;
    var var_merge_a = `a.field_${value}`;
    let return_array = arr.sort((a, b) => eval(var_merge_b) - eval(var_merge_a));
    
    return return_array;
}

function sort_province_top5low(arr , value){

    var var_merge_b = `b.field_${value}`;
    var var_merge_a = `a.field_${value}`;
    let return_arrays = arr.sort((a, b) =>   eval(var_merge_a) - eval(var_merge_b));

    return return_arrays;
}
function set_provincebangkokbyregionvalue(path_provinceregion = null , path_provinceall = null  , channels_id = null){

    // for test province region all : ./json/overview/tvprogram_baseon_tpbs/2022_4_11/logprovinceall_1649749843.json
    // for test province all        : ./json/overview/tvprogram_baseon_tpbs/2022_4_11/logprovincemergedata_1649749845.json
    let sum_array_provinceregion_all  = sum_array_provinceregion(path_provinceregion , channels_id); //  sum everychannel base on time slot
    let sum_array_province_all  =  sum_array_province(path_provinceall , channels_id);
   
    
    key_regionbangkok_found   = get_keybyvalue( sum_array_provinceregion_all, "Bangkok" , "element.province_region");
    key_provincebangkok_found = get_keybyvalue( sum_array_province_all , 10 , "find_provincekey");
  
    if(key_regionbangkok_found >= 0 && key_regionbangkok_found != undefined){
          // region_code= 10  , province_name = กรุงเทพมหานคร   
          if(key_provincebangkok_found >= 0 && key_provincebangkok_found != undefined){
              sum_array_province_all[key_provincebangkok_found]  = sum_array_provinceregion_all[key_regionbangkok_found];
              delete sum_array_province_all[key_provincebangkok_found].province_region;
              sum_array_province_all[key_provincebangkok_found].region_code =  10;
              sum_array_province_all[key_provincebangkok_found].province_name =  "กรุงเทพมหานคร";
             
          }else{
      
          }
    }

    // case : delete region code empty or null 
    key_provincenull_found = get_keybyvalue( sum_array_province_all , null , "find_provincekey");
    if(key_provincenull_found >= 0 && key_provincenull_found != undefined){
        delete sum_array_province_all[key_provincenull_found];
    }

    // console.log("key NaN found: "  + key_provincenull_found);
    // console.log(sum_array_province_all);
    return sum_array_province_all;
}

// ./json/overview/tvprogram_baseon_tpbs/2022_4_11/logprovincemergedata_1649749845.json
async function get_overview_report(date , avg_minute){
    var date_arr = get_timeinterval(date , date  , 30); // get datetime every x minute
 
    if(date.length != 0 && avg_minute.length != 0){
        let channels = await get_channels();
        if(channels[0].status != undefined){
            return channels;
        }else{
            let pool = await sql.connect(config);
        
          
          
                // case : overview report
           
                let tvprogram_basetime_satalitereport_data;
                let tvprogram_basetime_iptvreport_data;
                let rating_data_daily_satalite;
                let rating_data_daily_iptv;
                let tvprogram_basetime_ytreport_data;
                let satalite_gender;
                let iptv_gender;
                let youtube_gender;
                let province_region_satalite;
                let province_region_iptv;
                var month = get_dateobject(date, "month");
                var year = get_dateobject(date, "year");
                let rating_data_table  = get_ratingdatatable( date );
                let province_region_youtube;
                let province_satalite;
                let province_iptv;
                let province_youtube;
                // let channel_daily_devices_summary_table = get_channeldailydevicetbl(date);
                let channel_daily_devices_summary_table = query_deviceusertable();
                    let temptableratingsatalite =  await create_temptblsatalite(pool ,  date  , config);
                    // console.log(temptableratingsatalite);
                    let temptableratingiptv     =  await create_temptbliptv(pool ,  date  , config);
                    
                    var temptableratingdata     = temptableratingsatalite[0].data;
                    var temptableiptvdata       = temptableratingiptv[0].data;
             
                    // var tvprogram_recordset_obj = JSON.parse(JSON.stringify(tvprogram_recordset));
                    let $query_satalite_minute = build_query( "overview_minute" , temptableratingdata , null , date  , null , null, date_arr ,null , null , 30 );
                    let $query_IPTV_s3pp     = build_query( "overview_minute_s3app" , temptableiptvdata  , null , date , null , null , date_arr  , null ,null , 30);
                 
                    let req_dropsatalite = new sql.Request(pool);
                    let req_dropiptv     = new sql.Request(pool);
                   
                 
                    rating_data_daily_satalite = await pool.request()
                    .query($query_satalite_minute.temptable);
                    rating_data_daily_iptv     = await pool.request()
                    .query($query_IPTV_s3pp.temptable);

                  
                    // case : drop old temp table and create new one
                    $var_temptable = `#temp_data`;
                    // case : write new query
                    // return;
                    let $query_satalite = build_query( "overview_tvprogram_basetime_satalitereport" , temptableratingdata ,  null , date , null , null , date_arr );
                    
                    // case : crete new temp table satalite
                    $query_createtemptable = "SELECT * INTO " + $var_temptable +" FROM ( "+ $query_satalite.temptable + " ) as r1";  // case : create new temporary table 
                    // console.log($query_createtemptable);
                    // return [{"status":false}];
                    let req = new sql.Request(pool);
                    await req.batch($query_createtemptable);
                   
                    $query_tvprogrambaseonsatalite = ` SELECT merge_s3remote_id,channels_id ${$query_satalite.select_condition} FROM  #temp_data  inner join channels on #temp_data.channels_id = channels.id where channels.active = 1 group by channels_id,merge_s3remote_id`; // case : select temporary table data
                   
                    // case :  create new temp gender
                    $var_temptable_gender ="#temp_gender"; 
                    $query_createtempgender = "select * into "+$var_temptable_gender+" from (select r2.devices_id as dvid ,r2.gender from "+channel_daily_devices_summary_table+" group by r2.devices_id,r2.gender) r1";  // case : create new temporary table 
                    // console.log($query_createtempgender);
                    let req_tempgender = new sql.Request(pool);
                 
                    await req_tempgender.batch($query_createtempgender);
                    // case  : (query) group by gender ( satalite )
                    let $query_satalite_gender = build_query( "overview_groupby_gender_satalite" ,null , null , null , null , null , date_arr, null , $var_temptable );
                    let query_tempgender_satalite = await req_tempgender.batch($query_satalite_gender.temptable);
                    satalite_gender = query_tempgender_satalite.recordsets[0];

                  
                   
                    // case : (query) group by province region satalite data
                    let $query_satalite_addr = build_query( "overview_groupby_addr_satalite" ,null , null , null , null , null , date_arr, null , $var_temptable );
                    let query_groupbyaddr_satalite = await req_tempgender.batch($query_satalite_addr.temptable);
                    province_region_satalite = query_groupbyaddr_satalite.recordsets[0];        

                    // case : (query) group by province data
                    let $query_province_addr = build_query( "overview_groupby_provinceaddr_satalite" ,null , null , null , null , null , date_arr, null , $var_temptable );
                    let query_groupbyprovinceaddr_satalite = await req_tempgender.batch($query_province_addr.temptable);
                    province_addrsatalite = query_groupbyprovinceaddr_satalite.recordsets[0];  
                  

                    // case :  (drop) temp satalite
                    let query_temptable = await req.batch($query_tvprogrambaseonsatalite);
                   
                    await req.batch( "drop table " + $var_temptable ); // case : drop temp table
                    tvprogram_basetime_satalitereport_data = query_temptable.recordset;

                    // case : crete new temp table iptv
                    $var_temptable_iptv = `#temp_data_iptv`;
                    let $query_iptv = build_query( "overview_tvprogram_basetime_iptvreport_s3app" , temptableiptvdata ,  null , date , null , null , date_arr );
                     
                    $query_createtemptable_iptv = "SELECT * INTO " + $var_temptable_iptv +" FROM ( "+ $query_iptv.temptable + " ) as r1";  // case : create new temporary table 
            
                     let reqs3app = new sql.Request(pool);
                     await reqs3app.batch($query_createtemptable_iptv);
                 
                     $query_tvprogrambaseiptv = ` SELECT tvchannels_id ${$query_iptv.select_condition} FROM  #temp_data_iptv group by tvchannels_id`; // case : select temporary table data
                
                     let query_temptable_iptv = await reqs3app.batch($query_tvprogrambaseiptv);

                    // case  : (query) group by gender ( iptv )
                    let $query_iptv_gender;
                    $query_iptv_gender = build_query( "overview_groupby_gender_iptv" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv );
                  
                    let query_tempgender_iptv = await req_tempgender.batch($query_iptv_gender.temptable);
                    iptv_gender = query_tempgender_iptv.recordsets[0];
                    
                    
                    // case : (query) group by province region iptv data
                    let $query_iptv_addr = build_query( "overview_groupby_addr_iptv" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv );                
                    let query_groupbyaddr_iptv = await req_tempgender.batch($query_iptv_addr.temptable);
                    province_region_iptv = query_groupbyaddr_iptv.recordsets[0];
                  
                    // case : (query) group by province  iptv data
                    let $query_provinceiptv_addr = build_query( "overview_groupby_provinceaddr_iptv" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv );                
                    let query_groupbyprovinceaddr_iptv = await req_tempgender.batch($query_provinceiptv_addr.temptable);
                    province_addriptv = query_groupbyprovinceaddr_iptv.recordsets[0];

                  
            
                     // case :  (drop) temp iptv
                     await reqs3app.batch( "drop table " + $var_temptable_iptv ); // case : drop temp table
                     tvprogram_basetime_iptvreport_data = query_temptable_iptv.recordset;
            
                     // case : create new temp youtube view
                     $var_temptable_youtube = `#temp_data_youtube`;
                     let youtube_data_table = "youtube_rating_log_" + year + "_" + month;
                     let $query_youtube = build_query( "overview_youtubeviewreport_satalite" , rating_data_table ,  null , date , null , null , date_arr  ,  youtube_data_table);
                     //tvprogram_basetime_iptvreport_data = get_iptvratingdata( $query_youtube , $var_temptable_youtube , pool);
                     
                     $query_createtemptable_youtube = "SELECT * INTO " + $var_temptable_youtube +" FROM ( "+ $query_youtube.temptable + " ) as r1";  // case : create new temporary table 
                    
                     let reqs3ratingyt = new sql.Request(pool);
                     await reqs3ratingyt.batch($query_createtemptable_youtube);
            
                     $query_tvprogrambaseyoutube = ` SELECT devices_id ${$query_youtube.select_condition} FROM  #temp_data_youtube group by devices_id`; // case : select temporary table data
                     let query_temptable_yt = await reqs3ratingyt.batch($query_tvprogrambaseyoutube);

                    // case : group by temp youtube gender
                      let $query_satalite_youtube = build_query( "overview_groupby_gender_youtube" ,null , null , null , null , null , date_arr, null , $var_temptable_youtube );
                      // console.log($query_satalite_youtube);return;
                      let query_tempgender_youtube = await req_tempgender.batch($query_satalite_youtube.temptable);
                      youtube_gender = query_tempgender_youtube.recordsets[0];

                    // case : (query) group by province region youtube data
                    let $query_addr_youtube = build_query( "overview_groupby_addr_youtube" ,null , null , null , null , null , date_arr, null , $var_temptable_youtube );
                    let query_groupbyaddr_youtube = await req_tempgender.batch($query_addr_youtube.temptable);
                    province_region_youtube = query_groupbyaddr_youtube.recordsets[0];    
                    
                      // case : (query) group by province region youtube data
                      let $query_provinceaddr_youtube = build_query( "overview_groupby_provinceaddr_youtube" ,null , null , null , null , null , date_arr, null , $var_temptable_youtube );
                      let query_groupbyprovinceaddr_youtube = await req_tempgender.batch($query_provinceaddr_youtube.temptable);
                   
                      province_addryoutube = query_groupbyprovinceaddr_youtube.recordsets[0];    

                      await reqs3ratingyt.batch( "drop table " + $var_temptable_youtube ); // case : drop temp table
                      tvprogram_basetime_ytreport_data = query_temptable_yt.recordset;
            
                    // casae : (drop) temp gender
                     await req_tempgender.batch( "drop table " + $var_temptable_gender ); // case : drop temp table
                     await req_dropsatalite.batch( "drop table " + temptableratingdata ); // case : drop temp table ratingdata
                     await req_dropiptv.batch( "drop table " + temptableiptvdata ); // case : drop temp table iptv
            
                
        
                var arr_data = set_arrdata( [] );
   
                if (rating_data_daily_satalite != null ) {
                    let rating_data_daily_satalite_obj = rating_data_daily_satalite.recordsets[0];
                    var rating_data_daily_satalite_ = JSON.parse(JSON.stringify(rating_data_daily_satalite_obj));
            
                    if (rating_data_daily_satalite_ != null) {

                        //  arr_data = set_satalitetotal_arr( arr_data , rating_data_daily_satalite_); // case : set satalite data
                        if(rating_data_daily_iptv != null){
                            let rating_data_daily_iptv_obj = rating_data_daily_iptv.recordsets[0];
                        
                            var rating_data_daily_iptv_ = JSON.parse(JSON.stringify(rating_data_daily_iptv_obj));
                            arr_data = merge_andsortallofthisfuck( rating_data_daily_satalite_ , rating_data_daily_iptv_);
                        }
                        else{
                            arr_data = rating_data_daily_satalite_;
                        }
                       
                      
                        let tvprogrambaseon_channelid =  252;
                    
                        let $filename =   await write_excelfile_overview( arr_data  , channels  , date, date_arr 
                            , tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data , tvprogrambaseon_channelid
                            , tvprogram_basetime_ytreport_data , satalite_gender , iptv_gender 
                            , province_region_satalite , province_region_iptv  , youtube_gender , province_region_youtube , avg_minute , province_addrsatalite , province_addriptv , province_addryoutube);
                        
                        const file = $filename;
                    
                        return   [{
                            "status": true,
                            "data": file,
                            "result_code": "000",
                            "result_desc": "Success"
                        }];
                        
                    
                    
                    } else {
                        return [{
                            "status": false,
                            "result_code": "-005",
                            "result_desc": "not found data"
                        }];
                    }


                }


            
            
           
        }

      
    }else{
        return [{
            "status": false,
            "result_code": "-005",
            "result_desc": "startdatetime or enddatetime should not be empty."
        }];
    }

}


function requireUncached(module) {
    delete require.cache[require.resolve(module)];
    return require(module);
}
function cacheing_multidimensionarray(arr , filename , dir_jsonlocate){
    var obj = {
        data :[],
    };
     arr.forEach(element => {
        obj.data.push(Object.assign({}, element));
     });
     var path_cachingprovinceall      = save_log( obj, filename  , dir_jsonlocate ); // case : save log satalite data
     return path_cachingprovinceall;
}


function onaction_sumprovinceaddrtoptwenty_groupbychannel($channel_top20array , path20 , path_proviceaddrtop20, date_arr , dir_jsonlocate_cache  ){

    var arr_result = {};
    
 

             $channel_top20array.forEach(channel_id => {
              
                let sum_array_province_specific_top20  = sum_array_province(path_proviceaddrtop20 , channel_id); //  sum everychannel base on time slot ( top 20)
                sum_array_province_specific_top20      = set_provincebangkokbyregionvalue(path20 , path_proviceaddrtop20 , channel_id);
                // arr_result[hour][channel_id] =  sum_array_province_specific_top20;
                date_arr.forEach(element => {
        
                    element.forEach(v => {
                         let hour = v[2];
                         if(arr_result[hour] == undefined){
                            arr_result[hour] = [];
                         }
                        arr_result[hour][channel_id] = []; // declare array
                        arr_result[hour][channel_id]['sort_asc']  = [];
                        arr_result[hour][channel_id]['sort_desc'] = [];
                        sum_array_province_specific_top20.forEach(obj => {
                            
                            var res = {}
                            $field    = `field_${hour}`
                            $field_val=`obj.field_${hour}`;
                            res[$field]          = eval($field_val);
                            res['region_code']   = obj.region_code;
                            res['province_name'] = obj.province_name;
                            arr_result[hour][channel_id]['sort_asc'].push(Object.assign({}, res));
                            arr_result[hour][channel_id]['sort_desc'].push(Object.assign({}, res));
            
                         });
                    });      
                });
            });

   return  arr_result;

}

const onaction_removeobject_key = (obj, prop) => {
    let {[prop]: omit, ...res} = obj
    return res
  }

async function get_overview_report_daily(date ){
    var date_arr = get_timeinterval(date , date  , "all"); // get datetime every x minute

    

    let avg_minute = null;
    if(date.length != 0){
        let channels = await get_channels();
        if(channels[0].status != undefined){
            return channels;
        }else{
            let pool = await sql.connect(config);
        
          
          
                // case : overview report
           
                let tvprogram_basetime_satalitereport_data;
                let tvprogram_basetime_iptvreport_data;
                let rating_data_daily_satalite;
                let rating_data_daily_iptv;
                let tvprogram_basetime_ytreport_data;
                let satalite_gender;
                let iptv_gender;
                let youtube_gender;
                let province_region_satalite;
                let province_region_iptv;
                var month = get_dateobject(date, "month");
                var year = get_dateobject(date, "year");
                let rating_data_table  = get_ratingdatatable( date );
                let province_region_youtube;
                let province_satalite;
                let province_iptv;
                let province_youtube;
                let channeldailyrating_data;
                // let channel_daily_devices_summary_table = get_channeldailydevicetbl(date);
                let channel_daily_devices_summary_table = query_deviceusertable();
                    let temptableratingsatalite =  await create_temptblsatalite(pool ,  date  , config);
                    // console.log(temptableratingsatalite);
                    let temptableratingiptv     =  await create_temptbliptv(pool ,  date  , config);
                    
                    var temptableratingdata     = temptableratingsatalite[0].data;
                    var temptableiptvdata       = temptableratingiptv[0].data;
             
                    // var tvprogram_recordset_obj = JSON.parse(JSON.stringify(tvprogram_recordset));
                    // query get overview daily
                    let $query_satalite_minute = build_query( "overview_daily_satalite" , temptableratingdata , null , date  , null , null, date_arr ,null , null , avg_minute);
                    let $query_IPTV_s3pp     = build_query( "overview_daily_s3app" , temptableiptvdata  , null , date , null , null , date_arr  , null ,null , avg_minute );
                 
                    let req_dropsatalite = new sql.Request(pool);
                    let req_dropiptv     = new sql.Request(pool);
                   
                 
                    rating_data_daily_satalite = await pool.request()
                    .query($query_satalite_minute.temptable);
                    rating_data_daily_iptv     = await pool.request()
                    .query($query_IPTV_s3pp.temptable);

                  
                    // case : drop old temp table and create new one
                    $var_temptable = `#temp_data`;
                    // case : write new query
                    // return;
                    let $query_satalite = build_query( "overview_tvprogram_basetime_satalitereport" , temptableratingdata ,  null , date , null , null , date_arr );
                    
                    // case : crete new temp table satalite
                    $query_createtemptable = "SELECT * INTO " + $var_temptable +" FROM ( "+ $query_satalite.temptable + " ) as r1";  // case : create new temporary table 
                    // console.log($query_createtemptable);
                    // return [{"status":false}];
                    let req = new sql.Request(pool);
                    await req.batch($query_createtemptable);
                   
                    $query_tvprogrambaseonsatalite = ` SELECT merge_s3remote_id,channels_id ${$query_satalite.select_condition} FROM  #temp_data  inner join channels on #temp_data.channels_id = channels.id where channels.active = 1 group by channels_id,merge_s3remote_id`; // case : select temporary table data
                   
                    // case :  create new temp gender
                    $var_temptable_gender ="#temp_gender"; 
                    $query_createtempgender = "select * into "+$var_temptable_gender+" from (select r2.devices_id as dvid ,r2.gender from "+channel_daily_devices_summary_table+" group by r2.devices_id,r2.gender) r1";  // case : create new temporary table 
                    // console.log($query_createtempgender);
                    let req_tempgender = new sql.Request(pool);
                 
                    await req_tempgender.batch($query_createtempgender);
                    // case  : (query) group by gender ( satalite )
                    let $query_satalite_gender = build_query( "overview_groupby_gender_satalite" ,null , null , null , null , null , date_arr, null , $var_temptable );
                    let query_tempgender_satalite = await req_tempgender.batch($query_satalite_gender.temptable);
                    satalite_gender = query_tempgender_satalite.recordsets[0];

                  
                   
                    // case : (query) group by province region satalite data
                    let $query_satalite_addr = build_query( "overview_groupby_addr_satalite" ,null , null , null , null , null , date_arr, null , $var_temptable );
                    let query_groupbyaddr_satalite = await req_tempgender.batch($query_satalite_addr.temptable);
                    province_region_satalite = query_groupbyaddr_satalite.recordsets[0];        

                    let $query_province_addr = build_query( "overview_groupby_provinceaddr_satalite" ,null , null , null , null , null , date_arr, null , $var_temptable );
                    let query_groupbyprovinceaddr_satalite = await req_tempgender.batch($query_province_addr.temptable);
                    province_addrsatalite = query_groupbyprovinceaddr_satalite.recordsets[0];  

                  
                    // case :  (drop) temp satalite
                    let query_temptable = await req.batch($query_tvprogrambaseonsatalite);
                   
                    await req.batch( "drop table " + $var_temptable ); // case : drop temp table
                    tvprogram_basetime_satalitereport_data = query_temptable.recordset;

                    // case : crete new temp table iptv
                    $var_temptable_iptv = `#temp_data_iptv`;
                    let $query_iptv = build_query( "overview_tvprogram_basetime_iptvreport_s3app" , temptableiptvdata ,  null , date , null , null , date_arr );
                     
                    $query_createtemptable_iptv = "SELECT * INTO " + $var_temptable_iptv +" FROM ( "+ $query_iptv.temptable + " ) as r1";  // case : create new temporary table 
            
                     let reqs3app = new sql.Request(pool);
                     await reqs3app.batch($query_createtemptable_iptv);
                 
                     $query_tvprogrambaseiptv = ` SELECT tvchannels_id ${$query_iptv.select_condition} FROM  #temp_data_iptv group by tvchannels_id`; // case : select temporary table data
                
                     let query_temptable_iptv = await reqs3app.batch($query_tvprogrambaseiptv);

                    // case  : (query) group by gender ( iptv )
                    let $query_iptv_gender;
                    $query_iptv_gender = build_query( "overview_groupby_gender_iptv" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv );
                  
                    let query_tempgender_iptv = await req_tempgender.batch($query_iptv_gender.temptable);
                    iptv_gender = query_tempgender_iptv.recordsets[0];
                    
                    
                    // case : (query) group by province region iptv data
                    let $query_iptv_addr = build_query( "overview_groupby_addr_iptv" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv );                
                    let query_groupbyaddr_iptv = await req_tempgender.batch($query_iptv_addr.temptable);
                    province_region_iptv = query_groupbyaddr_iptv.recordsets[0];
                  

                    // case : (query) group by province  iptv data
                    let $query_provinceiptv_addr = build_query( "overview_groupby_provinceaddr_iptv" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv );                
                    let query_groupbyprovinceaddr_iptv = await req_tempgender.batch($query_provinceiptv_addr.temptable);
                    province_addriptv = query_groupbyprovinceaddr_iptv.recordsets[0];
                  
            
                     // case :  (drop) temp iptv
                     await reqs3app.batch( "drop table " + $var_temptable_iptv ); // case : drop temp table
                     tvprogram_basetime_iptvreport_data = query_temptable_iptv.recordset;
            
                     // case : create new temp youtube view
                     $var_temptable_youtube = `#temp_data_youtube`;
                     let youtube_data_table = "youtube_rating_log_" + year + "_" + month;
                     let $query_youtube = build_query( "overview_youtubeviewreport_satalite" , rating_data_table ,  null , date , null , null , date_arr  ,  youtube_data_table);
                     //tvprogram_basetime_iptvreport_data = get_iptvratingdata( $query_youtube , $var_temptable_youtube , pool);
                     
                     $query_createtemptable_youtube = "SELECT * INTO " + $var_temptable_youtube +" FROM ( "+ $query_youtube.temptable + " ) as r1";  // case : create new temporary table 
                    
                     let reqs3ratingyt = new sql.Request(pool);
                     await reqs3ratingyt.batch($query_createtemptable_youtube);
            
                     $query_tvprogrambaseyoutube = ` SELECT devices_id ${$query_youtube.select_condition} FROM  #temp_data_youtube group by devices_id`; // case : select temporary table data
                     let query_temptable_yt = await reqs3ratingyt.batch($query_tvprogrambaseyoutube);

                    // case : group by temp youtube gender
                      let $query_satalite_youtube = build_query( "overview_groupby_gender_youtube" ,null , null , null , null , null , date_arr, null , $var_temptable_youtube );
                      // console.log($query_satalite_youtube);return;
                      let query_tempgender_youtube = await req_tempgender.batch($query_satalite_youtube.temptable);
                      youtube_gender = query_tempgender_youtube.recordsets[0];

                    // case : (query) group by province region youtube data
                    let $query_addr_youtube = build_query( "overview_groupby_addr_youtube" ,null , null , null , null , null , date_arr, null , $var_temptable_youtube );
                    let query_groupbyaddr_youtube = await req_tempgender.batch($query_addr_youtube.temptable);
                    province_region_youtube = query_groupbyaddr_youtube.recordsets[0];    

                    // case : (query) group by province region youtube data
                    let $query_provinceaddr_youtube = build_query( "overview_groupby_provinceaddr_youtube" ,null , null , null , null , null , date_arr, null , $var_temptable_youtube );
                    let query_groupbyprovinceaddr_youtube = await req_tempgender.batch($query_provinceaddr_youtube.temptable);

                    province_addryoutube = query_groupbyprovinceaddr_youtube.recordsets[0];    

                    
                      /** ================ case : get channel daily rating top 20 channel ============================  */
                    
                      let $query_getchanneldailyrating = build_query( "get_channeldailyrating_overview" , null ,  null , date , null , null , date_arr );       
                      let execute_queryraw = await reqs3ratingyt.batch($query_getchanneldailyrating.rawquery);
                      channeldailyrating_data= execute_queryraw.recordset;
                     /** ============================================================================================ */
                     

                      await reqs3ratingyt.batch( "drop table " + $var_temptable_youtube ); // case : drop temp table
                      tvprogram_basetime_ytreport_data = query_temptable_yt.recordset;
            
                    // casae : (drop) temp gender
                     await req_tempgender.batch( "drop table " + $var_temptable_gender ); // case : drop temp table
                     await req_dropsatalite.batch( "drop table " + temptableratingdata ); // case : drop temp table ratingdata
                     await req_dropiptv.batch( "drop table " + temptableiptvdata ); // case : drop temp table iptv
            
                
        
                var arr_data = set_arrdata( [] );
   
                if (rating_data_daily_satalite != null ) {
                    let rating_data_daily_satalite_obj = rating_data_daily_satalite.recordsets[0];
                    var rating_data_daily_satalite_ = JSON.parse(JSON.stringify(rating_data_daily_satalite_obj));
            
                    if (rating_data_daily_satalite_ != null) {

                        //  arr_data = set_satalitetotal_arr( arr_data , rating_data_daily_satalite_); // case : set satalite data
                        if(rating_data_daily_iptv != null){
                            let rating_data_daily_iptv_obj = rating_data_daily_iptv.recordsets[0];
                        
                            var rating_data_daily_iptv_ = JSON.parse(JSON.stringify(rating_data_daily_iptv_obj));
                            arr_data = merge_andsortallofthisfuck( rating_data_daily_satalite_ , rating_data_daily_iptv_);
                        }
                        else{
                            arr_data = rating_data_daily_satalite_;
                        }
                       
                      
                        let tvprogrambaseon_channelid =  252;
                    
                        let $filename =   await write_excelfile_overview_daily( arr_data  , channels  , date, date_arr 
                            , tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data , tvprogrambaseon_channelid
                            , tvprogram_basetime_ytreport_data , satalite_gender , iptv_gender 
                            , province_region_satalite , province_region_iptv  , youtube_gender , province_region_youtube , avg_minute 
                            ,province_addrsatalite , province_addriptv , province_addryoutube , channeldailyrating_data
                            );
                        
                        const file = $filename;
                    
                        return   [{
                            "status": true,
                            "data": file,
                            "result_code": "000",
                            "result_desc": "Success"
                        }];
                        
                    
                    
                    } else {
                        return [{
                            "status": false,
                            "result_code": "-005",
                            "result_desc": "not found data"
                        }];
                    }


                }


            
            
           
        }

      
    }else{
        return [{
            "status": false,
            "result_code": "-005",
            "result_desc": "startdatetime or enddatetime should not be empty."
        }];
    }

}

async function get_average_views_perminute(date , avg_minute){
    var date_arr = get_timeinterval(date , date  , avg_minute , 'avgview'); // get datetime every x minute
  
    if(date.length != 0 && avg_minute.length != 0){
        let channels = await get_channels();
        if(channels[0].status != undefined){
            return channels;
        }else{
            let pool = await sql.connect(config);
        
          
          
                // case : overview report
           
                let tvprogram_basetime_satalitereport_data;
                let tvprogram_basetime_iptvreport_data;
                let channeldailyrating_data;
                let rating_data_daily_satalite;
                let rating_data_daily_iptv;
                let tvprogram_basetime_ytreport_data;
                let satalite_gender;
                let iptv_gender;
                let youtube_gender;
                let province_region_satalite;
                let province_region_iptv;
                var month = get_dateobject(date, "month");
                var year = get_dateobject(date, "year");
                let rating_data_table  = get_ratingdatatable( date );
                let province_region_youtube;
                let province_satalite;
                let province_iptv;
                let province_youtube;
                // let channel_daily_devices_summary_table = get_channeldailydevicetbl(date);
                let channel_daily_devices_summary_table = query_deviceusertable();
                    let temptableratingsatalite =  await create_temptblsatalite(pool ,  date  , config);
                    // console.log(temptableratingsatalite);
                    let temptableratingiptv     =  await create_temptbliptv(pool ,  date  , config);
                    
                    var temptableratingdata     = temptableratingsatalite[0].data;
                    var temptableiptvdata       = temptableratingiptv[0].data;
             
                    // var tvprogram_recordset_obj = JSON.parse(JSON.stringify(tvprogram_recordset));
                    let $query_satalite_minute = build_query( "overview_minute_perminute" , temptableratingdata , null , date  , null , null, date_arr ,null , null , avg_minute);
                    let $query_IPTV_s3pp     = build_query( "overview_minute_s3app_perminute" , temptableiptvdata  , null , date , null , null , date_arr  , null ,null , avg_minute );
                 
                    let req_dropsatalite = new sql.Request(pool);
                    let req_dropiptv     = new sql.Request(pool);
                   
                 
                    rating_data_daily_satalite = await pool.request()
                    .query($query_satalite_minute.temptable);
                    rating_data_daily_iptv     = await pool.request()
                    .query($query_IPTV_s3pp.temptable);

                    // case : drop old temp table and create new one
                    $var_temptable = `#temp_data`;
                    // case : write new query
                    // return;
                    let $query_satalite = build_query( "overview_tvprogram_basetime_satalitereport" , temptableratingdata ,  null , date , null , null , date_arr );
                    
                    // case : crete new temp table satalite
                    $query_createtemptable = "SELECT * INTO " + $var_temptable +" FROM ( "+ $query_satalite.temptable + " ) as r1";  // case : create new temporary table 
                    // console.log($query_createtemptable);
                    // return [{"status":false}];
                    let req = new sql.Request(pool);
                    await req.batch($query_createtemptable);
                   
                    $query_tvprogrambaseonsatalite = ` SELECT merge_s3remote_id,channels_id ${$query_satalite.select_condition} FROM  #temp_data  inner join channels on #temp_data.channels_id = channels.id where channels.active = 1 group by channels_id,merge_s3remote_id`; // case : select temporary table data
                   
                    // case :  create new temp gender
                    $var_temptable_gender ="#temp_gender"; 
                    $query_createtempgender = "select * into "+$var_temptable_gender+" from (select r2.devices_id as dvid ,r2.gender from "+channel_daily_devices_summary_table+" group by r2.devices_id,r2.gender) r1";  // case : create new temporary table 
                    // console.log($query_createtempgender);
                    let req_tempgender = new sql.Request(pool);
                    await req_tempgender.batch($query_createtempgender);
                 
                    // case :  (drop) temp satalite
                    let query_temptable = await req.batch($query_tvprogrambaseonsatalite);
                   
                    await req.batch( "drop table " + $var_temptable ); // case : drop temp table
                    tvprogram_basetime_satalitereport_data = query_temptable.recordset;

                    // case : crete new temp table iptv
                    $var_temptable_iptv = `#temp_data_iptv`;
                    let $query_iptv = build_query( "overview_tvprogram_basetime_iptvreport_s3app" , temptableiptvdata ,  null , date , null , null , date_arr );
                     
                    $query_createtemptable_iptv = "SELECT * INTO " + $var_temptable_iptv +" FROM ( "+ $query_iptv.temptable + " ) as r1";  // case : create new temporary table 
            
                     let reqs3app = new sql.Request(pool);
                     await reqs3app.batch($query_createtemptable_iptv);
                 
                     $query_tvprogrambaseiptv = ` SELECT tvchannels_id ${$query_iptv.select_condition} FROM  #temp_data_iptv group by tvchannels_id`; // case : select temporary table data
                     let query_temptable_iptv = await reqs3app.batch($query_tvprogrambaseiptv);

            
                     // case :  (drop) temp iptv
                     await reqs3app.batch( "drop table " + $var_temptable_iptv ); // case : drop temp table
                     tvprogram_basetime_iptvreport_data = query_temptable_iptv.recordset;
            
                     // case : create new temp youtube view
                     $var_temptable_youtube = `#temp_data_youtube`;
                     let youtube_data_table = "youtube_rating_log_" + year + "_" + month;
                     let $query_youtube = build_query( "overview_youtubeviewreport_satalite" , rating_data_table ,  null , date , null , null , date_arr  ,  youtube_data_table);
                     //tvprogram_basetime_iptvreport_data = get_iptvratingdata( $query_youtube , $var_temptable_youtube , pool);
                     
                     $query_createtemptable_youtube = "SELECT * INTO " + $var_temptable_youtube +" FROM ( "+ $query_youtube.temptable + " ) as r1";  // case : create new temporary table 
                    
                     let reqs3ratingyt = new sql.Request(pool);
                     await reqs3ratingyt.batch($query_createtemptable_youtube);
            
                     $query_tvprogrambaseyoutube = ` SELECT devices_id ${$query_youtube.select_condition} FROM  #temp_data_youtube group by devices_id`; // case : select temporary table data
                     let query_temptable_yt = await reqs3ratingyt.batch($query_tvprogrambaseyoutube);

                    /** ================ case : get channel daily rating top 20 channel ============================  */
                    
                     let $query_getchanneldailyrating = build_query( "get_channeldailyrating" , null ,  null , date , null , null , date_arr );
                     let execute_queryraw = await reqs3ratingyt.batch($query_getchanneldailyrating.rawquery);
                     channeldailyrating_data= execute_queryraw.recordset;
                    /** ============================================================================================ */
          

                      await reqs3ratingyt.batch( "drop table " + $var_temptable_youtube ); // case : drop temp table
                      tvprogram_basetime_ytreport_data = query_temptable_yt.recordset;
            
                    // casae : (drop) temp gender
                     await req_tempgender.batch( "drop table " + $var_temptable_gender ); // case : drop temp table
                     await req_dropsatalite.batch( "drop table " + temptableratingdata ); // case : drop temp table ratingdata
                     await req_dropiptv.batch( "drop table " + temptableiptvdata ); // case : drop temp table iptv
            
                
        
                var arr_data = set_arrdata( [] );
   
                if (rating_data_daily_satalite != null ) {
                    let rating_data_daily_satalite_obj = rating_data_daily_satalite.recordsets[0];
                    var rating_data_daily_satalite_ = JSON.parse(JSON.stringify(rating_data_daily_satalite_obj));
            
                    if (rating_data_daily_satalite_ != null) {

                  
                        let $filename =   await write_excelfile_avgviews_perminute(   channels  , date, date_arr 
                            , tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data , null
                            , tvprogram_basetime_ytreport_data , avg_minute  , channeldailyrating_data);
                        
                        const file = $filename;
                    
                        return   [{
                            "status": true,
                            "data": file,
                            "result_code": "000",
                            "result_desc": "Success"
                        }];
                        
                    
                    
                    } else {
                        return [{
                            "status": false,
                            "result_code": "-005",
                            "result_desc": "not found data"
                        }];
                    }


                }


            
            
           
        }

      
    }else{
        return [{
            "status": false,
            "result_code": "-005",
            "result_desc": "startdatetime or enddatetime should not be empty."
        }];
    }

}
async function write_excelfile_avgviews_perminute( channels  , date, date_arr 
    , tvprogram_basetime_satalitereport_data_s , tvprogram_basetime_iptvreport_data_s , channels_id
    , tvprogram_basetime_ytreport_data 
    , avg_minute  , channeldailyrating_data ){
    
        var excel = require('excel4node');
       

        let old_datearr = date_arr;
        $foldername = get_dateobject(date , "year") + "_"  +  get_dateobject(date , "month")+"_" + get_dateobject(date , "day");
        let dir     = "./excel/avgview_perminute/tvprogram_baseon_tpbs/" + $foldername;
        if (!fs.existsSync(dir)){
            fs.mkdirSync(dir);
        }

    
        /** ============  case : create folder ========== */
        $foldername = get_dateobject(date , "year") + "_"  +  get_dateobject(date , "month")+"_" + get_dateobject(date , "day");
        
        let dir_jsonlocate     = "./json/avgview_perminute/tvprogram_baseon_tpbs/" + $foldername;
        if (!fs.existsSync(dir_jsonlocate)){
            fs.mkdirSync(dir_jsonlocate);
        }
        /** ============  case : eof create folder ========== */

        let original_tvprogram_basetime_satalitereport_data = tvprogram_basetime_satalitereport_data_s;
        let original_tvprogram_basetime_iptv_data = tvprogram_basetime_iptvreport_data_s;
        // case : save log file
        let logsatalite    = JSON.stringify(original_tvprogram_basetime_satalitereport_data);
        var logsatalite_fn = 'logsatalite_' + Math.round(+new Date()/1000)+ ".json";
        fs.writeFileSync(dir_jsonlocate+'/'+logsatalite_fn, logsatalite);

        let logiptv    = JSON.stringify(original_tvprogram_basetime_iptv_data);
        var logiptv_fn = 'logiptv_' + Math.round(+new Date()/1000) + ".json";
        fs.writeFileSync(dir_jsonlocate+'/'+logiptv_fn, logiptv);
      
        
        let obj_satalite = require(dir_jsonlocate+'/'+logsatalite_fn);
        let obj_iptv     = require(dir_jsonlocate+'/'+logiptv_fn);
        
        var tvprogram_basetime_satalitereport_data = await find_channeldelete( "satalite" ,obj_satalite ); // list of top twenty digital tv
        var tvprogram_basetime_iptvreport_data     = await find_channeldelete( "iptv" ,obj_iptv );// list of top twenty digital tv
        
      // let tvprogram_obj          = get_thistvprogram( tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data , channels_id); // case : get only tvprogrm of this channel 
       let merge_data_obj         =  merge_data(tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data ); // case : merge top20channel satalite and iptv
       
       let logtop20    = JSON.stringify(merge_data_obj);
       var logtop20_fn = 'logtop20' + Math.round(+new Date()/1000) + ".json";
       fs.writeFileSync(dir_jsonlocate+'/'+logtop20_fn, logtop20);
       
       let merge_data_obj_everychannel  =   merge_data(original_tvprogram_basetime_satalitereport_data , original_tvprogram_basetime_iptv_data ); 
       
        let sum_allbaseontime = sum_everychannel_baseontime(merge_data_obj_everychannel); //  sum everychannel base on time slot
        let sum_top20         = sum_everychannel_baseontime(merge_data_obj); // sum  top20  base on timeslot
        
        let pool = await sql.connect(config);
        let req = new sql.Request(pool);
        let filename_ = 'avgviewreportperminute_'+seconds_since_epoch(new Date()) +'.xlsx';
      
        $channel_top20array = tvdigitalchannel_ondemand("satalite" , null); // # find top20 channel from satalite channel id
        $channel_top20array_header = tvdigitalchannel_ondemand("satalite" , "header");
        var shortmonthname = "";

       
        let tmpid = generation_randomnumber()  +  "_" + Date.now();
      
       
        var workbook = new excel.Workbook();  // Create a new instance of a Workbook class
        var worksheet = workbook.addWorksheet('ค่าเข้าถึงไม่นับซ้ำ');   // Add Worksheets to the workbook
        var worksheet_cumulative = workbook.addWorksheet('ค่าสะสม'  + avg_minute + 'นาที');   //  เพิ่มค่าสะสมทุก 10 นาที
        var worksheet_rating = workbook.addWorksheet('ค่าrating');   //  เพิ่มชีท rating
        let field_datestart = 6;
 
        let avg_field_number;
        var lastarr_key = date_arr[date_arr.length - 1];
        let lastvalue   = lastarr_key[0][2]; // ex. 23_50
        let avg_value_psitotal = 0;
        let avg_value_iptv = 0;
        let avg_value_yt = 0;
        let avg_value_20channel = {};
        let avg_value_20channel_rating = {};
        let init_setfiledatestring_ = init_setfiledatestring(date);
        await date_arr.forEach(element => {
           
            element.forEach(v => {
                    $field =`field_${v[2]}`;
                    var hour    = v[2]; // get hour
                    var starttime = get_starttime_notconvert( v );
                    var endtime   = get_endtime_notconvert( v );

         
                    $row = 5;
                    $row_cumulative = 5;
                    $row_rating   =5;
                    
                    /* =========== Create a reusable style =========== */
                    var style = get_wbstyle( workbook  , "numberformat");
                    var text_style_header = get_wbstyle(workbook , "text_style_header"); // create header style 
                    var text_style = get_wbstyle(workbook , "text_style"); // create table cell style
                    /* =========== eof : Create a reusable style =========== */

                    
             

                    // Set value of cell A1 to 100 as a number type styled with paramaters of style ( แถว , หลัก )
                    worksheet  = create_excelheader(worksheet  , channels , text_style_header , null , date , "avg_viewer" , avg_minute);
                    worksheet.cell(4, field_datestart).string( starttime + "-" + endtime ).style(text_style); // create date text
                    
                    /** ============ report ค่าสะสมไม่นับซ้ำ ============================  */
                    worksheet_cumulative  = create_excelheader(worksheet_cumulative  , channels , text_style_header , null , date , "avg_viewer" , avg_minute);
           
                    worksheet_cumulative.cell(4, field_datestart).string( starttime + "-" + endtime ).style(text_style); // create date text
                   /** ============  eof report ค่าสะสมไม่นับซ้ำ ============================  */
                    /** ============ report ค่าสะสมไม่นับซ้ำ ============================  */
                    worksheet_rating  = create_excelheader(worksheet_rating  , channels , text_style_header , null , date , "avg_viewer" , avg_minute);
                    worksheet_rating.cell(4, field_datestart).string( starttime + "-" + endtime ).style(text_style); // create date text
                   /** ============  eof report ค่าสะสมไม่นับซ้ำ ============================  */

                    worksheet.cell($row, 1).string( "PSI TOTAL" ).style(text_style);
                    // worksheet_cumulative.cell($row, 1).string( "PSI TOTAL" ).style(text_style);
                    let $psitotal          = `sum_allbaseontime['${$field}']`;
                    $psitotal = eval($psitotal);
                    worksheet.cell($row, field_datestart ).number( $psitotal  ).style(text_style);
                    // worksheet_cumulative.cell($row, field_datestart ).number( $psitotal  ).style(text_style);
                    avg_value_psitotal += $psitotal;
                    set_cell(worksheet , $row , date , text_style , v , null, null , null ,null , 'avg_viewer');
                    // set_cell(worksheet_cumulative , $row , date , text_style , v , null, null , null ,null , 'avg_viewer');

                    /** =================== set avg cell ======================  */
                    if(hour == lastvalue){
                        let lst_field_datestart  = field_datestart  +1;
                        worksheet.cell(4, lst_field_datestart ).string(  "AVG"  ).style(text_style);
                        worksheet_cumulative.cell(4, lst_field_datestart ).string(  "AVG"  ).style(text_style);
                        worksheet_rating.cell(4, lst_field_datestart ).string(  "AVG"  ).style(text_style);
                        avg_value_psitotal = avg_value_psitotal  / date_arr.length;
                        set_avgfieldvalue(worksheet , $row,text_style, field_datestart , round_xdecimalplace(avg_value_psitotal));
                        // set_avgfieldvalue(worksheet_cumulative , $row,text_style, field_datestart , round_xdecimalplace(avg_value_psitotal));
                    }
                    /** =================== (eof) set avg cell ======================  */

                    ++$row;
                    worksheet.cell($row, 1).string( "OTHER TV" ).style(text_style);
                    // worksheet_cumulative.cell($row, 1).string( "OTHER TV" ).style(text_style);
               
                    var $sumtop_20 = `sum_top20['${$field}']`;
                    $sumtop_20     =  eval($sumtop_20);
                    let $other_tv  = parseInt($psitotal) - parseInt($sumtop_20);
                    worksheet.cell($row, field_datestart ).number( $other_tv  ).style(text_style);
                    worksheet_cumulative.cell($row, field_datestart ).number( $other_tv  ).style(text_style);
                    worksheet_rating.cell($row, field_datestart ).number( $other_tv  ).style(text_style);
                    avg_value_iptv += $other_tv;
                    set_cell(worksheet , $row , date , text_style , v , null, null , null ,null , 'avg_viewer');
                    // set_cell(worksheet_cumulative , $row , date , text_style , v , null, null , null ,null , 'avg_viewer');
                     /** =================== set avg cell ======================  */
                     if(hour == lastvalue){
                        avg_value_iptv = avg_value_iptv  / date_arr.length;
                        set_avgfieldvalue(worksheet , $row,text_style, field_datestart , round_xdecimalplace(avg_value_iptv));
                        // set_avgfieldvalue(worksheet_cumulative , $row,text_style, field_datestart , round_xdecimalplace(avg_value_iptv));
                    }
                    /** =================== (eof) set avg cell ======================  */
                    ++$row;
                    worksheet.cell($row, 1).string( "ONLINE (YOUTUBE)" ).style(text_style);
                    // worksheet_cumulative.cell($row, 1).string( "ONLINE (YOUTUBE)" ).style(text_style);
                    let yt_total = 0;
                    if(tvprogram_basetime_ytreport_data != null){
                        yt_total = set_sum( tvprogram_basetime_ytreport_data ,$field);
                        yt_total = parseInt(yt_total) > 0  ? parseInt(yt_total) : 0;
                        
                    }
                    worksheet.cell($row, field_datestart ).number( yt_total  ).style(text_style);
                    // worksheet_cumulative.cell($row, field_datestart ).number( yt_total  ).style(text_style);
                    avg_value_yt += yt_total;
                    set_cell(worksheet , $row , date , text_style , v , null, null , null ,null , 'avg_viewer');
                    // set_cell(worksheet_cumulative , $row , date , text_style , v , null, null , null ,null , 'avg_viewer');
                     /** =================== set avg cell ======================  */
                     if(hour == lastvalue){
                        avg_value_yt = avg_value_yt  / date_arr.length;
                        set_avgfieldvalue(worksheet , $row,text_style, field_datestart , round_xdecimalplace(avg_value_yt));
                        // set_avgfieldvalue(worksheet_cumulative , $row,text_style, field_datestart , round_xdecimalplace(avg_value_yt));
                    }
                    /** =================== (eof) set avg cell ======================  */

                    $loop_count=0;
                    $channel_top20array.forEach(channel_id => {
                        ++$row;
                       
                        let keyfound      =  get_keybyvalue( merge_data_obj , channel_id);
                        worksheet.cell($row, 1).string( $channel_top20array_header[$loop_count] ).style(text_style); // case : set channel name
                        worksheet_cumulative.cell($row_cumulative, 1).string( $channel_top20array_header[$loop_count] ).style(text_style); // case : set channel name
                        worksheet_rating.cell($row_rating, 1).string( $channel_top20array_header[$loop_count] ).style(text_style); // case : set channel name
                        set_cell(worksheet , $row , date , text_style , v , null, null , null ,null , 'avg_viewer');
                        set_cell(worksheet_cumulative , $row_cumulative , date , text_style , v , null, null , null ,null , 'avg_viewer');
                        set_cell(worksheet_rating , $row_cumulative , date , text_style , v , null, null , null ,null , 'avg_viewer');
                        if(avg_value_20channel[channel_id] == undefined){
                            avg_value_20channel[channel_id] = 0;
                        }
                        if(avg_value_20channel_rating[channel_id] == undefined){
                            avg_value_20channel_rating[channel_id] = {};
                            avg_value_20channel_rating[channel_id]['reach_devices'] = 0;
                            avg_value_20channel_rating[channel_id]['rating']        = 0; 
                        }
                        if(keyfound >= 0 && keyfound != undefined){
                            
                            $value =  `merge_data_obj[keyfound].${$field}`;
                            $value = eval($value);
                            if($value > 0){
                           
                                worksheet.cell($row, field_datestart ).number( $value  ).style(text_style);
                                worksheet_cumulative.cell($row_cumulative, field_datestart ).number( $value  ).style(text_style);
                                
                                avg_value_20channel[channel_id] += $value;
                                 /** =================== set avg cell ======================  */
                                if(hour == lastvalue){
                                    let avg_val = avg_value_20channel[channel_id]  / date_arr.length;
                                    set_avgfieldvalue(worksheet , $row,text_style, field_datestart , round_xdecimalplace(avg_val));
                                   //    set_avgfieldvalue(worksheet_cumulative , $row_cumulative,text_style, field_datestart , round_xdecimalplace(avg_val));
                                }
                                /** =================== (eof) set avg cell ======================  */
                                
                            }

                        }else{
                         
                            worksheet.cell($row, field_datestart ).number(  0  ).style(text_style);
                            // worksheet_cumulative.cell($row_cumulative, field_datestart ).number(  0  ).style(text_style);
                            /** =================== set avg cell ======================  */
                              if(hour == lastvalue){
                                let avg_val = 0;
                                if(avg_value_20channel[channel_id]  > 0){
                                     avg_val = avg_value_20channel[channel_id]  / date_arr.length;
                                     avg_val = round_xdecimalplace(avg_val);
                                }
                                set_avgfieldvalue(worksheet , $row,text_style, field_datestart , avg_val);
                                // set_avgfieldvalue(worksheet_cumulative , $row_cumulative,text_style, field_datestart , avg_val);
                            }
                            /** =================== (eof) set avg cell ======================  */
                        }

                        /** ================== case  : sheet ค่าสะสม 10 นาทีที่ต้องเอา rating และ reach devices มาแสดง =====================   */
                        let key_find         =   channel_id+"_"+ hour;
                        let key_channelfound = get_keybyvalue_reportavgviewevery10minute( channeldailyrating_data ,key_find );
                        

                        if(key_channelfound >= 0 && key_channelfound != undefined){
                            $reach_devices =  `channeldailyrating_data[key_channelfound].reach_devices`;
                            $rating =  `channeldailyrating_data[key_channelfound].rating`;
                            $reach_devices = eval($reach_devices);
                            $reach_devices = $reach_devices > 0 ? $reach_devices : 0;
                            $rating = eval($rating);
                            $rating = $rating > 0 ? $rating : 0;
                            if($reach_devices > 0){
                                 worksheet_cumulative.cell($row_cumulative, field_datestart ).number( $reach_devices  ).style(text_style);
                                 worksheet_rating.cell($row_cumulative, field_datestart ).number( $rating ).style(text_style);
                                 avg_value_20channel_rating[channel_id]['reach_devices'] += parseInt($reach_devices);
                                 avg_value_20channel_rating[channel_id]['rating'] += parseFloat($rating);
                                //  /** =================== set avg cell ======================  */
                                if(hour == lastvalue){
                                    let avg_val_reachdevices =  parseInt(avg_value_20channel_rating[channel_id]['reach_devices'])  / date_arr.length;
                                    avg_val_reachdevices     = round_xdecimalplace(avg_val_reachdevices);
                                    let avg_val_rating       =  parseFloat(avg_value_20channel_rating[channel_id]['rating'])  / date_arr.length;
                                    avg_val_rating           = round_xdecimalplace(avg_val_rating , 2);
                                  
                                   
                                    set_avgfieldvalue(worksheet_cumulative , $row_cumulative,text_style, field_datestart ,  avg_val_reachdevices );
                                    set_avgfieldvalue(worksheet_rating , $row_cumulative,text_style, field_datestart , avg_val_rating  );
                                }
                                /** =================== (eof) set avg cell ======================  */
                                
                            }
                        }else{
                            worksheet_cumulative.cell($row_cumulative, field_datestart ).number(  0  ).style(text_style);
                            worksheet_rating.cell($row_cumulative, field_datestart ).number(  0  ).style(text_style);
                          
                            /** =================== set avg cell ======================  */
                              if(hour == lastvalue){
                                let avg_val_reachdevices = 0;
                                let avg_val_rating   = 0;
                                if(avg_value_20channel_rating[channel_id]['reach_devices']  > 0){
                                    avg_val_reachdevices =  parseInt(avg_value_20channel_rating[channel_id]['reach_devices'])  / date_arr.length;
                                    avg_val_reachdevices     = round_xdecimalplace(avg_val_reachdevices);
                                    avg_val_rating       =  parseFloat(avg_value_20channel_rating[channel_id]['rating'])  / date_arr.length;
                                    avg_val_rating           = round_xdecimalplace(avg_val_rating , 2);
                                }
                               // console.log("ไม่มีค่า  :  " + avg_val_rating + ";" + avg_val_reachdevices);
                                set_avgfieldvalue(worksheet_cumulative , $row_cumulative,text_style, field_datestart ,avg_val_reachdevices );
                                set_avgfieldvalue(worksheet_rating , $row_cumulative,text_style, field_datestart , avg_val_rating );
                                // set_avgfieldvalue(worksheet_cumulative , $row_cumulative,text_style, field_datestart , avg_val);
                            }
                            /** =================== (eof) set avg cell ======================  */
                        }
                        /** ================== (eof ) case  : sheet ค่าสะสม 10 นาทีที่ต้องเอา rating และ reach devices มาแสดง =====================   */
               
                        ++$row_cumulative;
                        ++$row_rating;
                        ++ $loop_count; // count every channel
                      

                       
                    })             
                    ++ field_datestart;
                });

               
               
        });

    
      

        /* ========= case : create excel file ( overview report ) */
        // var lastarr_key = date_arr[date_arr.length - 1];
        // var start_datetime = add_hour(date_arr[0][0][0] , 7);
        // var end_datetime   = add_hour(lastarr_key[0][1] , 7);
        // if(avg_minute == 30){
        //     avg_minute = avg_minute * 2 * 24;  // 60 minute * 24 hour
        // }
        let $24hour         = avg_minute * 24;
        let $filename_24hr  = 'overviewreportperminute_'+ init_setfiledatestring_ +'_'+seconds_since_epoch(new Date()) +'.xlsx';
        workbook.write(dir+ '/' + $filename_24hr);

        let result_    = await create_newreport(tmpid , $foldername , date , pool , "avgview_perminute" , $filename_24hr ); // case : create new temp id
        // let result_    =   create_newreportoverview( null  , insert_id[0].id , date , pool , start_datetime , end_datetime  , avg_minute , "all" , $filename_24hr); // case : create new temp id
        
        /* ========= (eof) : create excel file ( overview report ) */
       
        
      
}
function set_avgfieldvalue(worksheet , $row,text_style, field_datestart , value , type = null){
    let avg_field_number = field_datestart + 1;
    if(type == "every10minutereport"){
        worksheet.cell($row, avg_field_number ).string( value  ).style(text_style);
    }else{
        worksheet.cell($row, avg_field_number ).number( value  ).style(text_style);
    }
}

async function get_overview_report_perminute(date , avg_minute){
    //var date_arr = get_timeinterval(date , date  , avg_minute); // get datetime every x minute
    
    var date_arr = get_timeinterval(date , date  , avg_minute , "avgview"); // get datetime every x minute
   
    if(date.length != 0 && avg_minute.length != 0){
        let channels = await get_channels();
        if(channels[0].status != undefined){
            return channels;
        }else{
            let pool = await sql.connect(config);
        
          
          
                // case : overview report
           
                let tvprogram_basetime_satalitereport_data;
                let tvprogram_basetime_iptvreport_data;
                let rating_data_daily_satalite;
                let rating_data_daily_iptv;
                let tvprogram_basetime_ytreport_data;
                let satalite_gender;
                let iptv_gender;
                let youtube_gender;
                let province_region_satalite;
                let province_region_iptv;
                var month = get_dateobject(date, "month");
                var year = get_dateobject(date, "year");
                let rating_data_table  = get_ratingdatatable( date );
                let province_region_youtube;
                let province_satalite;
                let province_iptv;
                let province_youtube;
                let channeldailyrating_data;
                // let channel_daily_devices_summary_table = get_channeldailydevicetbl(date);
                let channel_daily_devices_summary_table = query_deviceusertable();
                    let temptableratingsatalite =  await create_temptblsatalite(pool ,  date  , config);
                    // console.log(temptableratingsatalite);
                    let temptableratingiptv     =  await create_temptbliptv(pool ,  date  , config);
                    
                    var temptableratingdata     = temptableratingsatalite[0].data;
                    var temptableiptvdata       = temptableratingiptv[0].data;
             
                    // var tvprogram_recordset_obj = JSON.parse(JSON.stringify(tvprogram_recordset));
                    let $query_satalite_minute = build_query( "overview_minute_perminute" , temptableratingdata , null , date  , null , null, date_arr ,null , null , avg_minute);
                    let $query_IPTV_s3pp     = build_query( "overview_minute_s3app_perminute" , temptableiptvdata  , null , date , null , null , date_arr  , null ,null , avg_minute );
                 
                    let req_dropsatalite = new sql.Request(pool);
                    let req_dropiptv     = new sql.Request(pool);
                   
                 
                    rating_data_daily_satalite = await pool.request()
                    .query($query_satalite_minute.temptable);
                    rating_data_daily_iptv     = await pool.request()
                    .query($query_IPTV_s3pp.temptable);

                  
                    // case : drop old temp table and create new one
                    $var_temptable = `#temp_data`;
                    // case : write new query
                    // return;
                    let $query_satalite = build_query( "overview_tvprogram_basetime_satalitereport" , temptableratingdata ,  null , date , null , null , date_arr );
                    
                    // case : crete new temp table satalite
                    $query_createtemptable = "SELECT * INTO " + $var_temptable +" FROM ( "+ $query_satalite.temptable + " ) as r1";  // case : create new temporary table 
                    // console.log($query_createtemptable);
                    // return [{"status":false}];
                    let req = new sql.Request(pool);
                    await req.batch($query_createtemptable);
                   
                    $query_tvprogrambaseonsatalite = ` SELECT merge_s3remote_id,channels_id ${$query_satalite.select_condition} FROM  #temp_data  inner join channels on #temp_data.channels_id = channels.id where channels.active = 1 group by channels_id,merge_s3remote_id`; // case : select temporary table data
                   
                    // case :  create new temp gender
                    $var_temptable_gender ="#temp_gender"; 
                    $query_createtempgender = "select * into "+$var_temptable_gender+" from (select r2.devices_id as dvid ,r2.gender from "+channel_daily_devices_summary_table+" group by r2.devices_id,r2.gender) r1";  // case : create new temporary table 
                    // console.log($query_createtempgender);
                    let req_tempgender = new sql.Request(pool);
                 
                    await req_tempgender.batch($query_createtempgender);
                    // case  : (query) group by gender ( satalite )
                    let $query_satalite_gender = build_query( "overview_groupby_gender_satalite" ,null , null , null , null , null , date_arr, null , $var_temptable );
                    let query_tempgender_satalite = await req_tempgender.batch($query_satalite_gender.temptable);
                    satalite_gender = query_tempgender_satalite.recordsets[0];

                  
                   
                    // case : (query) group by province region satalite data
                    let $query_satalite_addr = build_query( "overview_groupby_addr_satalite" ,null , null , null , null , null , date_arr, null , $var_temptable );
                    let query_groupbyaddr_satalite = await req_tempgender.batch($query_satalite_addr.temptable);
                    province_region_satalite = query_groupbyaddr_satalite.recordsets[0];        

                    // case : (query) group by province data
                    // let $query_province_addr = build_query( "overview_groupby_provinceaddr_satalite" ,null , null , null , null , null , date_arr, null , $var_temptable );
                    // let query_groupbyprovinceaddr_satalite = await req_tempgender.batch($query_province_addr.temptable);
                    // province_addrsatalite = query_groupbyprovinceaddr_satalite.recordsets[0];  
                  

                    // case :  (drop) temp satalite
                    let query_temptable = await req.batch($query_tvprogrambaseonsatalite);
                   
                    await req.batch( "drop table " + $var_temptable ); // case : drop temp table
                    tvprogram_basetime_satalitereport_data = query_temptable.recordset;

                    // case : crete new temp table iptv
                    $var_temptable_iptv = `#temp_data_iptv`;
                    let $query_iptv = build_query( "overview_tvprogram_basetime_iptvreport_s3app" , temptableiptvdata ,  null , date , null , null , date_arr );
                     
                    $query_createtemptable_iptv = "SELECT * INTO " + $var_temptable_iptv +" FROM ( "+ $query_iptv.temptable + " ) as r1";  // case : create new temporary table 
            
                     let reqs3app = new sql.Request(pool);
                     await reqs3app.batch($query_createtemptable_iptv);
                 
                     $query_tvprogrambaseiptv = ` SELECT tvchannels_id ${$query_iptv.select_condition} FROM  #temp_data_iptv group by tvchannels_id`; // case : select temporary table data
                
                     let query_temptable_iptv = await reqs3app.batch($query_tvprogrambaseiptv);

                    // case  : (query) group by gender ( iptv )
                    let $query_iptv_gender;
                    $query_iptv_gender = build_query( "overview_groupby_gender_iptv" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv );
                  
                    let query_tempgender_iptv = await req_tempgender.batch($query_iptv_gender.temptable);
                    iptv_gender = query_tempgender_iptv.recordsets[0];
                    
                    
                    // case : (query) group by province region iptv data
                    let $query_iptv_addr = build_query( "overview_groupby_addr_iptv" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv );                
                    let query_groupbyaddr_iptv = await req_tempgender.batch($query_iptv_addr.temptable);
                    province_region_iptv = query_groupbyaddr_iptv.recordsets[0];
                  
                    // case : (query) group by province  iptv data
                    // let $query_provinceiptv_addr = build_query( "overview_groupby_provinceaddr_iptv" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv );                
                    // let query_groupbyprovinceaddr_iptv = await req_tempgender.batch($query_provinceiptv_addr.temptable);
                    // province_addriptv = query_groupbyprovinceaddr_iptv.recordsets[0];

                  
            
                     // case :  (drop) temp iptv
                     await reqs3app.batch( "drop table " + $var_temptable_iptv ); // case : drop temp table
                     tvprogram_basetime_iptvreport_data = query_temptable_iptv.recordset;
            
                     // case : create new temp youtube view
                     $var_temptable_youtube = `#temp_data_youtube`;
                     let youtube_data_table = "youtube_rating_log_" + year + "_" + month;
                     let $query_youtube = build_query( "overview_youtubeviewreport_satalite" , rating_data_table ,  null , date , null , null , date_arr  ,  youtube_data_table);
                     //tvprogram_basetime_iptvreport_data = get_iptvratingdata( $query_youtube , $var_temptable_youtube , pool);
                     
                     $query_createtemptable_youtube = "SELECT * INTO " + $var_temptable_youtube +" FROM ( "+ $query_youtube.temptable + " ) as r1";  // case : create new temporary table 
                    
                     let reqs3ratingyt = new sql.Request(pool);
                     await reqs3ratingyt.batch($query_createtemptable_youtube);
            
                     $query_tvprogrambaseyoutube = ` SELECT devices_id ${$query_youtube.select_condition} FROM  #temp_data_youtube group by devices_id`; // case : select temporary table data
                     let query_temptable_yt = await reqs3ratingyt.batch($query_tvprogrambaseyoutube);

                    // case : group by temp youtube gender
                      let $query_satalite_youtube = build_query( "overview_groupby_gender_youtube" ,null , null , null , null , null , date_arr, null , $var_temptable_youtube );
                      // console.log($query_satalite_youtube);return;
                      let query_tempgender_youtube = await req_tempgender.batch($query_satalite_youtube.temptable);
                      youtube_gender = query_tempgender_youtube.recordsets[0];

                    // case : (query) group by province region youtube data
                    let $query_addr_youtube = build_query( "overview_groupby_addr_youtube" ,null , null , null , null , null , date_arr, null , $var_temptable_youtube );
                    let query_groupbyaddr_youtube = await req_tempgender.batch($query_addr_youtube.temptable);
                    province_region_youtube = query_groupbyaddr_youtube.recordsets[0];    
                    
                       // case : (query) group by province region youtube data
                    //   let $query_provinceaddr_youtube = build_query( "overview_groupby_provinceaddr_youtube" ,null , null , null , null , null , date_arr, null , $var_temptable_youtube );
                    //   let query_groupbyprovinceaddr_youtube = await req_tempgender.batch($query_provinceaddr_youtube.temptable);
                   
                    //   province_addryoutube = query_groupbyprovinceaddr_youtube.recordsets[0];    

                     /** ================ case : get channel daily rating top 20 channel ============================  */
                    
                     let $query_getchanneldailyrating = build_query( "get_channeldailyrating" , null ,  null , date , null , null , date_arr );
                 
                     let execute_queryraw = await reqs3ratingyt.batch($query_getchanneldailyrating.rawquery);
                     channeldailyrating_data= execute_queryraw.recordset;
                    /** ============================================================================================ */


                      await reqs3ratingyt.batch( "drop table " + $var_temptable_youtube ); // case : drop temp table
                      tvprogram_basetime_ytreport_data = query_temptable_yt.recordset;
            
                    // casae : (drop) temp gender
                     await req_tempgender.batch( "drop table " + $var_temptable_gender ); // case : drop temp table
                     await req_dropsatalite.batch( "drop table " + temptableratingdata ); // case : drop temp table ratingdata
                     await req_dropiptv.batch( "drop table " + temptableiptvdata ); // case : drop temp table iptv
            
                
        
                var arr_data = set_arrdata( [] );
   
                if (rating_data_daily_satalite != null ) {
                    let rating_data_daily_satalite_obj = rating_data_daily_satalite.recordsets[0];
                    var rating_data_daily_satalite_ = JSON.parse(JSON.stringify(rating_data_daily_satalite_obj));
            
                    if (rating_data_daily_satalite_ != null) {

                        //  arr_data = set_satalitetotal_arr( arr_data , rating_data_daily_satalite_); // case : set satalite data
                        if(rating_data_daily_iptv != null){
                            let rating_data_daily_iptv_obj = rating_data_daily_iptv.recordsets[0];
                        
                            var rating_data_daily_iptv_ = JSON.parse(JSON.stringify(rating_data_daily_iptv_obj));
                            arr_data = merge_andsortallofthisfuck( rating_data_daily_satalite_ , rating_data_daily_iptv_);
                        }
                        else{
                            arr_data = rating_data_daily_satalite_;
                        }
                       
                      
                        let tvprogrambaseon_channelid =  252;
                    
                        let $filename =   await write_excelfile_overview_perminute( arr_data  , channels  , date, date_arr 
                            , tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data , tvprogrambaseon_channelid
                            , tvprogram_basetime_ytreport_data , satalite_gender , iptv_gender 
                            , province_region_satalite , province_region_iptv  , youtube_gender , province_region_youtube , avg_minute , null , null , null , channeldailyrating_data);
                            //3ตัวสุดท้ายแต่เดิมคือ =  province_addrsatalite , province_addriptv , province_addryoutube
                        
                        const file = $filename;
                    
                        return   [{
                            "status": true,
                            "data": file,
                            "result_code": "000",
                            "result_desc": "Success"
                        }];
                        
                    
                    
                    } else {
                        return [{
                            "status": false,
                            "result_code": "-005",
                            "result_desc": "not found data"
                        }];
                    }


                }


            
            
           
        }

      
    }else{
        return [{
            "status": false,
            "result_code": "-005",
            "result_desc": "startdatetime or enddatetime should not be empty."
        }];
    }

}
async function write_excelfile_overview_daily(arr_data  , channels  , date, date_arr 
    , tvprogram_basetime_satalitereport_data_s , tvprogram_basetime_iptvreport_data_s , channels_id
    , tvprogram_basetime_ytreport_data , satalite_gender , iptv_gender 
    , province_region_satalite , province_region_iptv , youtube_gender , province_region_youtube 
    , avg_minute ,province_satalite , province_iptv , province_youtube , channeldailyrating_data  ){
    
        var excel = require('excel4node');
        let old_datearr = date_arr;
        $foldername = get_dateobject(date , "year") + "_"  +  get_dateobject(date , "month")+"_" + get_dateobject(date , "day");
        let dir     = "./excel/overview_daily/tvprogram_baseon_tpbs/" + $foldername;
        if (!fs.existsSync(dir)){
            fs.mkdirSync(dir);
        }

    
        /** ============  case : create folder ========== */
        $foldername = get_dateobject(date , "year") + "_"  +  get_dateobject(date , "month")+"_" + get_dateobject(date , "day");
        
        let dir_jsonlocate     = "./json/overview_daily/tvprogram_baseon_tpbs/" + $foldername;
        if (!fs.existsSync(dir_jsonlocate)){
            fs.mkdirSync(dir_jsonlocate);
        }
        /** ============  case : eof create folder ========== */

        let original_tvprogram_basetime_satalitereport_data = tvprogram_basetime_satalitereport_data_s;
        let original_tvprogram_basetime_iptv_data = tvprogram_basetime_iptvreport_data_s;
        // case : save log file
        let logsatalite    = JSON.stringify(original_tvprogram_basetime_satalitereport_data);
        var logsatalite_fn = 'logsatalite_' + Math.round(+new Date()/1000)+ ".json";
        fs.writeFileSync(dir_jsonlocate+'/'+logsatalite_fn, logsatalite);

        let logiptv    = JSON.stringify(original_tvprogram_basetime_iptv_data);
        var logiptv_fn = 'logiptv_' + Math.round(+new Date()/1000) + ".json";
        fs.writeFileSync(dir_jsonlocate+'/'+logiptv_fn, logiptv);
      
        
        let obj_satalite = require(dir_jsonlocate+'/'+logsatalite_fn);
        let obj_iptv     = require(dir_jsonlocate+'/'+logiptv_fn);
        
        var tvprogram_basetime_satalitereport_data = await find_channeldelete( "satalite" ,obj_satalite ); // list of top twenty digital tv
        var tvprogram_basetime_iptvreport_data     = await find_channeldelete( "iptv" ,obj_iptv );// list of top twenty digital tv
        
       
        
      

        let tvprogram_obj          = get_thistvprogram( tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data , channels_id); // case : get only tvprogrm of this channel 
       let merge_data_obj         =  merge_data(tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data ); // case : merge top20channel satalite and iptv
       
       let logtop20    = JSON.stringify(merge_data_obj);
       var logtop20_fn = 'logtop20' + Math.round(+new Date()/1000) + ".json";
       fs.writeFileSync(dir_jsonlocate+'/'+logtop20_fn, logtop20);
       
       let merge_data_obj_everychannel  =   merge_data(original_tvprogram_basetime_satalitereport_data , original_tvprogram_basetime_iptv_data ); 
       
        let sum_allbaseontime = sum_everychannel_baseontime(merge_data_obj_everychannel); //  sum everychannel base on time slot
        let sum_top20         = sum_everychannel_baseontime(merge_data_obj); // sum  top20  base on timeslot
        
        let logsatalitegender    = JSON.stringify(satalite_gender);
        var logsatalitegender_fn = 'logsatalitegender_' + Math.round(+new Date()/1000) + ".json";
        fs.writeFileSync(dir_jsonlocate+'/'+logsatalitegender_fn, logsatalitegender);
       
        let merge_data_gender_obj  = merge_data_gender_overview(satalite_gender , iptv_gender);
        var path_gendertop20                    = save_log( merge_data_gender_obj, "logmergegendertop20_"  , dir_jsonlocate );

    
      
        let sum_gender_all              = sum_array_gender(merge_data_gender_obj); //  sum everychannel base on time slot
      
        //let obj_satalitegender          =  Object.assign({}, satalite_gender); // case : convert array sum gender all to object
        let gender_satalitetop20        = await find_channeldelete(  "satalite" , satalite_gender); // get satalite gender top 20
        let gender_iptvtop20            = await find_channeldelete(  "iptv"     , iptv_gender); // get satalite gender top 20
        let merge_data_gender_obj_top20 = merge_data_gender_overview(gender_satalitetop20 , gender_iptvtop20); // merge top20 channel
        let sum_gender_alltop20         = sum_array_gender(merge_data_gender_obj_top20); //  sum everychannel base on time slot ( top 20)

        var logprovincesatalite_fn      = save_log( province_region_satalite, "logprovincesatalite_"  , dir_jsonlocate ); // case : save log satalite data
        var logprovinceiptv_fn          = save_log( province_region_iptv, "logprovinceiptv_"  , dir_jsonlocate ); // case : save log iptv data
        
        let obj_provincesatalite = require(logprovincesatalite_fn);
        let obj_provinceiptv     = require(logprovinceiptv_fn);
    
        /** =============== case :   region   ===================  */
        /** sum array province region :  all */
        let merge_data_province_obj   = merge_data_regionoverview(obj_provincesatalite , obj_provinceiptv, "element.province_region"); // get all province region
        let path_regionall  = save_log( merge_data_province_obj, "logprovinceall_"  , dir_jsonlocate ); // save log : province all  
        let sum_array_provinceregion_all  = sum_array_provinceregion(path_regionall); //  sum everychannel base on time slot
        /** sum array province region :  top 20 */
        let addr_satalitetop20        = await find_channeldelete(  "satalite" , obj_provincesatalite); // get satalite gender top 20
        let addr_iptvtop20            = await find_channeldelete(  "iptv"     , obj_provinceiptv); // get satalite gender top 20    
        let merge_data_provincetop20_obj   = merge_data_regionoverview(addr_satalitetop20 , addr_iptvtop20, "element.province_region"); // get all province region
        var path20                    = save_log( merge_data_provincetop20_obj, "logprovincetop20_"  , dir_jsonlocate );
        let sum_array_provinceregion_top20         = sum_array_provinceregion(path20); //  sum everychannel base on time slot ( top 20)

        var path_province_region_youtube                    = save_log( province_region_youtube, "logprovince_region_youtube_"  , dir_jsonlocate );

     /** =============== case :   eof region   ===================  */
           /** ==========  case : province   ============= */
      
      /** case  province  step (1) : save log  */
      var path_province_satalite                    = save_log( province_satalite, "logprovincesatalitegroup_"  , dir_jsonlocate );
      var path_province_iptv                        = save_log( province_iptv, "logprovinceiptvgroup_"  , dir_jsonlocate );
      var path_province_youtube                     = save_log( province_youtube, "logprovinceyoutubegroup_"  , dir_jsonlocate );
      /** case  province  step (2) : read json log file  */
      let obj_province_satalite = require(path_province_satalite); 
      let obj_province_iptv     = require(path_province_iptv);
      let obj_province_youtube  = require(path_province_youtube);
      /** case  province  step (3) : combine json data satalite and iptv  */
      let merge_data_provincegroup_obj  = merge_data_province_overview(obj_province_satalite , obj_province_iptv);
      /** case  province  step (4) : save log file from step (3) */
      var path_merge_province           = save_log( merge_data_provincegroup_obj, "logprovincemergedata_"  , dir_jsonlocate );


   
  
      /** case  province  step (5) : find othertv except from 20 tvdigital then delete it (satalite ) */
      let addr_provincesatalitetop20        = await find_channeldelete(  "satalite" , obj_province_satalite); // get satalite gender top 20
      /** case  province  step (6) : find othertv except from 20 tvdigital then delete it ( IPTV  ) */
      let addr_provinceiptvtop20            = await find_channeldelete(  "iptv"     , obj_province_iptv); // get satalite gender top 20    
      /** case  province  step (7) : combine them(from step 5 , 6 ) to one object */
      let merge_data_provinceaddrtop20_obj   = merge_data_province_overview(addr_provincesatalitetop20 , addr_provinceiptvtop20 ); // get all province
      /** case  province  step (8) : save log for using in future */
      var path_proviceaddrtop20                    = save_log( merge_data_provinceaddrtop20_obj, "logprovinceaddrtop20_"  , dir_jsonlocate );
  
 
      /** case : province all satalite + iptv */
      let sum_array_province_all  =  sum_array_province(path_merge_province);
      sum_array_province_all = set_provincebangkokbyregionvalue(path_regionall , path_merge_province ); 
      let path_cachingsumprovincearray = cacheing_multidimensionarray(sum_array_province_all , "caching_sumarryprovinceall" , dir_jsonlocate);


      let sum_array_province_top20  = sum_array_province(path_proviceaddrtop20); //  sum everychannel base on time slot ( top 20)
      sum_array_province_top20      = set_provincebangkokbyregionvalue(path20 , path_proviceaddrtop20 );
      sum_array_province_top20      = onaction_minusothertv_province(sum_array_province_all ,  sum_array_province_top20 , old_datearr); // psitotal - 20tvdigital
      let path_sum_array_province_top20 = cacheing_multidimensionarray(sum_array_province_top20 , "caching_sum_array_province_top20" , dir_jsonlocate);
      

      let youtube_summary_a      = set_provincebangkokbyregionvalue(path_province_region_youtube , path_province_youtube );
      let youtube_summary_b      = set_provincebangkokbyregionvalue(path_province_region_youtube , path_province_youtube );
      /* ========== eof : province  ===================*/




        let pool = await sql.connect(config);
        let req = new sql.Request(pool);
        let filename_ = 'overviewreportperminute_'+seconds_since_epoch(new Date()) +'.xlsx';
      
        $channel_top20array = tvdigitalchannel_ondemand("satalite" , null); // # find top20 channel from satalite channel id
        $channel_top20array_header = tvdigitalchannel_ondemand("satalite" , "header");
        var shortmonthname = "";

       
        let tmpid = generation_randomnumber()  +  "_" + Date.now();
      
       

        /** ================== case : รวมคนดูตามแต่ละช่วงเวลาของทุกช่องมาเก็บไว้ใน array เพื่อไปเรียกใช้ภายหลัง ==================== */
        
        let dir_jsonlocate_cache     = "./json/overview_daily/tvprogram_baseon_tpbs/" + $foldername + "/" + "cache_" + tmpid;
        if (!fs.existsSync(dir_jsonlocate_cache)){
                fs.mkdirSync(dir_jsonlocate_cache);
        }
        let sumprovinceaddrtoptwenty_groupbychannel =  onaction_sumprovinceaddrtoptwenty_groupbychannel( $channel_top20array , path20 , path_proviceaddrtop20 , date_arr  , dir_jsonlocate_cache);
        
        /** ================== eof : รวมคนดูตามแต่ละช่วงเวลาของทุกช่องมาเก็บไว้ใน array เพื่อไปเรียกใช้ภายหลัง ==================== */


        var workbook_all = new excel.Workbook();  // Create a new instance of a Workbook class
        await date_arr.forEach(element => {
         
            element.forEach(v => {
                    /** ตัดปัญหาเรื่องจำ cache บน nodejs เลย require 2 ตัว เจอวิธีที่ดีกว่าเขียนแก้ได้เลยครับ */
                    // case : psi total 
                    let sum_array_province_all_a  = requireUncached(path_cachingsumprovincearray);
                    let sum_array_province_all_b  =  requireUncached(path_cachingsumprovincearray);
                    // case : other tv
                    let sum_array_province_top20_a  = requireUncached(path_sum_array_province_top20);
                    let sum_array_province_top20_b  = requireUncached(path_sum_array_province_top20);

                    /** eof : ตัดปัญหาเรื่องจำ cache บน nodejs เลย require 2 ตัว */


                    $field =`field_${v[2]}`;
                    var hour    = v[2]; // get hour
                    let top5_province    =   sort_province_top5peak(sum_array_province_all_a.data , hour); // case : get top 5 province on period
                    let top5low_province =   sort_province_top5low(sum_array_province_all_b.data , hour); // case : get top 5 province on period

                    let top5_province_top20    =   sort_province_top5peak(sum_array_province_top20_a.data , hour); // case : get top 5 province on period
                    let top5low_province_top20 =   sort_province_top5low(sum_array_province_top20_b.data , hour); // case : get top 5 province on period

                    let top5_province_topyt    =   sort_province_top5peak(youtube_summary_a , hour); // case : get top 5 province on period
                    let top5low_province_topyt =   sort_province_top5low(youtube_summary_b , hour); // case : get top 5 province on period
                    var workbook = new excel.Workbook();  // Create a new instance of a Workbook class
                    var worksheet = workbook.addWorksheet('รายงานภาพรวม');   // Add Worksheets to the workbook
                    var worksheet_all = workbook_all.addWorksheet("รายงานภาพรวม");   // Add Worksheets to the workbook
                    /* =========== Create a reusable style =========== */
                    var style = get_wbstyle( workbook  , "numberformat");
                    var text_style_header = get_wbstyle(workbook , "text_style_header"); // create header style 
                    var text_style = get_wbstyle(workbook , "text_style"); // create table cell style
                    /* =========== eof : Create a reusable style =========== */

                    // Set value of cell A1 to 100 as a number type styled with paramaters of style ( แถว , หลัก )
                    worksheet_all  = create_excelheader(worksheet_all  , channels , text_style_header , null , date  , "overview_daily");
                   
                    $row = 5;
                    worksheet_all.cell($row, 1).string( "PSI TOTAL" ).style(text_style);
                    let $psitotal          = `sum_allbaseontime['${$field}']`;
                    $psitotal = eval($psitotal);
                    worksheet_all.cell($row, 8 ).number( $psitotal  ).style(text_style);
                    set_cell(worksheet_all , $row , date , text_style , v , merge_data_gender_obj, sum_gender_all , sum_array_provinceregion_all , null , "overview_daily");
                    // case :  เพศ
                    var $gender_male;
                    var $gender_female;
                    var $gender_none;
                    var $othertv_gender_male;
                    var $othertv_gender_female;
                    var $othertv_gender_none;
                    var $youtube_gender;
                    var $youtube_gender_male;
                    var $youtube_gender_female;
                    var $youtube_gender_none;
                    if(sum_gender_all != null){
                         $field =`field_${v[2]}`;
                         $gender        = get_totalgender(sum_gender_all , $field , 'overview');
                         $othertv_gender= get_totalgender(sum_gender_alltop20 , $field , 'overview');
                         $youtube_gender= get_totalgender(youtube_gender ,$field , 'overview' );
                         $gender_male   = $gender.male > 0 ? $gender.male : 0;
                         $gender_female = $gender.female > 0 ? $gender.female : 0;
                         $gender_none   = $gender.none > 0 ? $gender.none : 0;
                         $othertv_gender_male   = $othertv_gender.male > 0 ? $othertv_gender.male : 0;
                         $othertv_gender_male   = $gender_male - $othertv_gender_male;                
                         $othertv_gender_female = $othertv_gender.female > 0 ? $othertv_gender.female : 0;
                         $othertv_gender_female = $gender_female -  $othertv_gender_female;
                         $othertv_gender_none   = $othertv_gender.none > 0 ? $othertv_gender.none : 0;
                         $othertv_gender_none   = $gender_none  - $othertv_gender_none;

                         $youtube_gender_male   = $youtube_gender.male > 0 ? $youtube_gender.male : 0;
                         $youtube_gender_female = $youtube_gender.female > 0 ? $youtube_gender.female : 0;
                         $youtube_gender_none   = $youtube_gender.none > 0 ? $youtube_gender.none : 0;

                         set_cellgender( worksheet_all , $row , 9 , $gender_male , text_style);
                         set_cellgender( worksheet_all , $row , 10 , $gender_female , text_style);
                         set_cellgender( worksheet_all , $row , 11 , $gender_none , text_style);
                     } 
                 
                    set_provincetop5(worksheet_all , $row , top5_province , top5low_province , text_style , null , $field , 21);
                    ++$row;
                    worksheet_all.cell($row, 1).string( "OTHER TV" ).style(text_style);
               
                    var $sumtop_20 = `sum_top20['${$field}']`;
                    $sumtop_20     =  eval($sumtop_20);
                    let $other_tv  = parseInt($psitotal) - parseInt($sumtop_20);
                    worksheet_all.cell($row, 8 ).number( $other_tv  ).style(text_style);


                    /** work sheet all */
                    set_cell(worksheet_all , $row , date , text_style , v , null,null , sum_array_provinceregion_all , sum_array_provinceregion_top20 , "overview_daily");
                    set_cellgender( worksheet_all , $row , 9  , $othertv_gender_male , text_style);
                    set_cellgender( worksheet_all , $row , 10  , $othertv_gender_female , text_style);
                    set_cellgender( worksheet_all , $row , 11 , $othertv_gender_none , text_style);

                    set_provincetop5(worksheet_all , $row , top5_province_top20 , top5low_province_top20 , text_style , null , $field , 21);
                    ++$row;

                    worksheet_all.cell($row, 1).string( "ONLINE (YOUTUBE)" ).style(text_style);
                    let yt_total = 0;
                    if(tvprogram_basetime_ytreport_data != null){
                        yt_total = set_sum( tvprogram_basetime_ytreport_data ,$field);
                        yt_total = parseInt(yt_total) > 0  ? parseInt(yt_total) : 0;
                        
                    }

                   

                    worksheet_all.cell($row, 8 ).number( yt_total  ).style(text_style);
                    
                    set_cell(worksheet_all , $row , date , text_style , v , null,null , province_region_youtube , null ,  "overview_daily");
                    set_cellgender( worksheet_all , $row , 9  , $youtube_gender_male , text_style);
                    set_cellgender( worksheet_all , $row , 10  , $youtube_gender_female , text_style);
                    set_cellgender( worksheet_all , $row , 11 , $youtube_gender_none , text_style);
                    set_provincetop5(worksheet_all , $row , top5_province_topyt , top5low_province_topyt , text_style , null , $field , 21);
                    // console.log(merge_data_obj_everychannel); 
                    $loop_count=0;
                    $channel_top20array.forEach(channel_id => {
                        
                    
                        ++$row;
                        /** ==== case : get province data by specific channel ======  */
                        let sum_array_province_specific_a  = sum_array_province(path_proviceaddrtop20 , channel_id); //  sum everychannel base on time slot ( top 20)
                        sum_array_province_specific_a      = set_provincebangkokbyregionvalue(path20 , path_proviceaddrtop20 , channel_id);

                        let sum_array_province_specific_b  = sum_array_province(path_proviceaddrtop20 , channel_id); //  sum everychannel base on time slot ( top 20)
                        sum_array_province_specific_b      = set_provincebangkokbyregionvalue(path20 , path_proviceaddrtop20 , channel_id);

                        let top5_province_specific    =   sort_province_top5peak(sum_array_province_specific_a , hour); // case : get top 5 province on period
                        let top5low_province_specific =   sort_province_top5low(sum_array_province_specific_b , hour); // case : get top 5 province on period
                        
                        let keyfound      =  get_keybyvalue( merge_data_obj , channel_id);
                        worksheet_all.cell($row, 1).string( $channel_top20array_header[$loop_count] ).style(text_style); // case : set channel name
                        if(keyfound >= 0 && keyfound != undefined){
                            
                            $value =  `merge_data_obj[keyfound].${$field}`;
                            $value = eval($value);
                            if($value > 0){
                                worksheet_all.cell($row, 8 ).number( $value  ).style(text_style);
                                
                            }

                        }else{
                            worksheet_all.cell($row, 8 ).number(  0  ).style(text_style);
                        }
                        // case : set gender value region value
                        let fixed_channelarray  = [];
                        fixed_channelarray.push( channel_id );

                        $region_satalite_self =   find_channeldelete_specificarray(  "satalite" , path20 , fixed_channelarray); // get only this channel gender object
                        // console.log($region_satalite_self);

                        set_cell(worksheet_all , $row , date , text_style , v , null,null , $region_satalite_self , null , "overview_daily");
                        
                        
                        let obj_gender_self     =  find_channeldelete_specificarray(  "satalite" , path_gendertop20 , fixed_channelarray); // get only this channel gender object
                        // console.log(obj_gender_self);
                        $gender      = get_totalgender(obj_gender_self , $field);
                        $gender_male   = $gender.male > 0 ? $gender.male : 0;
                        $gender_female = $gender.female > 0 ? $gender.female : 0;
                        $gender_none   = $gender.none > 0 ? $gender.none : 0;

                        /** workbook all */
                        set_cellgender( worksheet_all , $row , 9  , $gender_male , text_style);
                        set_cellgender( worksheet_all , $row , 10  , $gender_female , text_style);
                        set_cellgender( worksheet_all , $row , 11 , $gender_none , text_style);

                
                        set_provincetop5(worksheet_all , $row , top5_province_specific , top5low_province_specific , text_style , null , $field , 21);



                        /** ================== case  : sheet ค่าสะสม 10 นาทีที่ต้องเอา rating และ reach devices มาแสดง =====================   */
                        let key_find         =   channel_id+"_"+ hour;
                        let key_channelfound = get_keybyvalue_reportavgviewevery10minute( channeldailyrating_data ,key_find );
            
                        if(key_channelfound >= 0 && key_channelfound != undefined){
                            $reach_devices =  `channeldailyrating_data[key_channelfound].reach_devices`;
                 
                            $reach_devices = eval($reach_devices);
                            $reach_devices = $reach_devices > 0 ? $reach_devices : 0;
                
                            if($reach_devices > 0){
                                worksheet_all.cell($row, 7 ).number( $reach_devices ).style(text_style);
                                /** =================== (eof) set avg cell ======================  */
                                
                            }
                        }else{
                        
                            worksheet_all.cell($row, 7 ).number( 0  ).style(text_style);
                       
                        
                        }
                        /** ================== (eof ) case  : sheet ค่าสะสม 10 นาทีที่ต้องเอา rating และ reach devices มาแสดง =====================   */


                        ++ $loop_count; // count every channel
                    })

                
   
                    
                });
        });
        
      

        /* ========= case : create excel file ( overview report ) */
        let init_setfiledatestring_ = init_setfiledatestring(date);
        let $filename_24hr  = 'overviewreport_'+ 24 +'_'+ init_setfiledatestring_+'.xlsx';
        workbook_all.write(dir+ '/' + $filename_24hr);

        let result    = await create_newreport(tmpid , $foldername , date , pool , "overview_daily" , $filename_24hr ); // case : create new temp id

        /* ========= (eof) : create excel file ( overview report ) */
       
        
      
}

function init_setfiledatestring(date){
    // let datestring = get_dateobject(date , "day") + "" + get_dateobject(date , "month") + "" + get_dateobject(date , "year");

    var dt = new Date( date );
    let month = dt.getMonth() + 1;
    month =  month < 10 ? "0"+month : month;
    var d         = dt.getDate();
    d =  d < 10 ? "0"+d : d;

    var date_text = d +  ""  + month +  ""  + dt.getFullYear();

    return date_text;

}
async function write_excelfile_overview_perminute(arr_data  , channels  , date, date_arr 
    , tvprogram_basetime_satalitereport_data_s , tvprogram_basetime_iptvreport_data_s , channels_id
    , tvprogram_basetime_ytreport_data , satalite_gender , iptv_gender 
    , province_region_satalite , province_region_iptv , youtube_gender , province_region_youtube 
    , avg_minute , province_satalite , province_iptv , province_youtube , channeldailyrating_data ){
    
        var excel = require('excel4node');
        let old_datearr = date_arr;
        $foldername = get_dateobject(date , "year") + "_"  +  get_dateobject(date , "month")+"_" + get_dateobject(date , "day");
        let dir     = "./excel/overview_perminute/tvprogram_baseon_tpbs/" + $foldername;
        if (!fs.existsSync(dir)){
            fs.mkdirSync(dir);
        }

    
        /** ============  case : create folder ========== */
        $foldername = get_dateobject(date , "year") + "_"  +  get_dateobject(date , "month")+"_" + get_dateobject(date , "day");
        
        let dir_jsonlocate     = "./json/overview_perminute/tvprogram_baseon_tpbs/" + $foldername;
        if (!fs.existsSync(dir_jsonlocate)){
            fs.mkdirSync(dir_jsonlocate);
        }
        /** ============  case : eof create folder ========== */

        let original_tvprogram_basetime_satalitereport_data = tvprogram_basetime_satalitereport_data_s;
        let original_tvprogram_basetime_iptv_data = tvprogram_basetime_iptvreport_data_s;
        // case : save log file
        let logsatalite    = JSON.stringify(original_tvprogram_basetime_satalitereport_data);
        var logsatalite_fn = 'logsatalite_' + Math.round(+new Date()/1000)+ ".json";
        fs.writeFileSync(dir_jsonlocate+'/'+logsatalite_fn, logsatalite);

        let logiptv    = JSON.stringify(original_tvprogram_basetime_iptv_data);
        var logiptv_fn = 'logiptv_' + Math.round(+new Date()/1000) + ".json";
        fs.writeFileSync(dir_jsonlocate+'/'+logiptv_fn, logiptv);
      
        
        let obj_satalite = require(dir_jsonlocate+'/'+logsatalite_fn);
        let obj_iptv     = require(dir_jsonlocate+'/'+logiptv_fn);
        
        var tvprogram_basetime_satalitereport_data = await find_channeldelete( "satalite" ,obj_satalite ); // list of top twenty digital tv
        var tvprogram_basetime_iptvreport_data     = await find_channeldelete( "iptv" ,obj_iptv );// list of top twenty digital tv
        
       
        
      

        let tvprogram_obj          = get_thistvprogram( tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data , channels_id); // case : get only tvprogrm of this channel 
       let merge_data_obj         =  merge_data(tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data ); // case : merge top20channel satalite and iptv
       
       let logtop20    = JSON.stringify(merge_data_obj);
       var logtop20_fn = 'logtop20' + Math.round(+new Date()/1000) + ".json";
       fs.writeFileSync(dir_jsonlocate+'/'+logtop20_fn, logtop20);
       
       let merge_data_obj_everychannel  =   merge_data(original_tvprogram_basetime_satalitereport_data , original_tvprogram_basetime_iptv_data ); 
       
        let sum_allbaseontime = sum_everychannel_baseontime(merge_data_obj_everychannel); //  sum everychannel base on time slot
        let sum_top20         = sum_everychannel_baseontime(merge_data_obj); // sum  top20  base on timeslot
        
        let logsatalitegender    = JSON.stringify(satalite_gender);
        var logsatalitegender_fn = 'logsatalitegender_' + Math.round(+new Date()/1000) + ".json";
        fs.writeFileSync(dir_jsonlocate+'/'+logsatalitegender_fn, logsatalitegender);
       
        let merge_data_gender_obj  = merge_data_gender_overview(satalite_gender , iptv_gender);
        var path_gendertop20                    = save_log( merge_data_gender_obj, "logmergegendertop20_"  , dir_jsonlocate );

    
      
        let sum_gender_all              = sum_array_gender(merge_data_gender_obj); //  sum everychannel base on time slot
      
        //let obj_satalitegender          =  Object.assign({}, satalite_gender); // case : convert array sum gender all to object
        let gender_satalitetop20        = await find_channeldelete(  "satalite" , satalite_gender); // get satalite gender top 20
        let gender_iptvtop20            = await find_channeldelete(  "iptv"     , iptv_gender); // get satalite gender top 20
        let merge_data_gender_obj_top20 = merge_data_gender_overview(gender_satalitetop20 , gender_iptvtop20); // merge top20 channel
        let sum_gender_alltop20         = sum_array_gender(merge_data_gender_obj_top20); //  sum everychannel base on time slot ( top 20)

        var logprovincesatalite_fn      = save_log( province_region_satalite, "logprovincesatalite_"  , dir_jsonlocate ); // case : save log satalite data
        var logprovinceiptv_fn          = save_log( province_region_iptv, "logprovinceiptv_"  , dir_jsonlocate ); // case : save log iptv data
        
        let obj_provincesatalite = require(logprovincesatalite_fn);
        let obj_provinceiptv     = require(logprovinceiptv_fn);
    
        /** =============== case :   region   ===================  */
        /** sum array province region :  all */
        let merge_data_province_obj   = merge_data_regionoverview(obj_provincesatalite , obj_provinceiptv, "element.province_region"); // get all province region
        let path_regionall  = save_log( merge_data_province_obj, "logprovinceall_"  , dir_jsonlocate ); // save log : province all  
        let sum_array_provinceregion_all  = sum_array_provinceregion(path_regionall); //  sum everychannel base on time slot
        /** sum array province region :  top 20 */
        let addr_satalitetop20        = await find_channeldelete(  "satalite" , obj_provincesatalite); // get satalite gender top 20
        let addr_iptvtop20            = await find_channeldelete(  "iptv"     , obj_provinceiptv); // get satalite gender top 20    
        let merge_data_provincetop20_obj   = merge_data_regionoverview(addr_satalitetop20 , addr_iptvtop20, "element.province_region"); // get all province region
        var path20                    = save_log( merge_data_provincetop20_obj, "logprovincetop20_"  , dir_jsonlocate );
        let sum_array_provinceregion_top20         = sum_array_provinceregion(path20); //  sum everychannel base on time slot ( top 20)



 
        var path_province_region_youtube                    = save_log( province_region_youtube, "logprovince_region_youtube_"  , dir_jsonlocate );

     /** =============== case :   eof region   ===================  */

      /** ==========  case : province   ============= */
      
    //   /** case  province  step (1) : save log  */
    //   var path_province_satalite                    = save_log( province_satalite, "logprovincesatalitegroup_"  , dir_jsonlocate );
    //   var path_province_iptv                        = save_log( province_iptv, "logprovinceiptvgroup_"  , dir_jsonlocate );
    //   var path_province_youtube                     = save_log( province_youtube, "logprovinceyoutubegroup_"  , dir_jsonlocate );
    //   /** case  province  step (2) : read json log file  */
    //   let obj_province_satalite = require(path_province_satalite); 
    //   let obj_province_iptv     = require(path_province_iptv);
    //   let obj_province_youtube  = require(path_province_youtube);
    //   /** case  province  step (3) : combine json data satalite and iptv  */
    //   let merge_data_provincegroup_obj  = merge_data_province_overview(obj_province_satalite , obj_province_iptv);
    //   /** case  province  step (4) : save log file from step (3) */
    //   var path_merge_province           = save_log( merge_data_provincegroup_obj, "logprovincemergedata_"  , dir_jsonlocate );


   
  
    //   /** case  province  step (5) : find othertv except from 20 tvdigital then delete it (satalite ) */
    //   let addr_provincesatalitetop20        = await find_channeldelete(  "satalite" , obj_province_satalite); // get satalite gender top 20
    //   /** case  province  step (6) : find othertv except from 20 tvdigital then delete it ( IPTV  ) */
    //   let addr_provinceiptvtop20            = await find_channeldelete(  "iptv"     , obj_province_iptv); // get satalite gender top 20    
    //   /** case  province  step (7) : combine them(from step 5 , 6 ) to one object */
    //   let merge_data_provinceaddrtop20_obj   = merge_data_province_overview(addr_provincesatalitetop20 , addr_provinceiptvtop20 ); // get all province
    //   /** case  province  step (8) : save log for using in future */
    //   var path_proviceaddrtop20                    = save_log( merge_data_provinceaddrtop20_obj, "logprovinceaddrtop20_"  , dir_jsonlocate );
  
 
    //   /** case : province all satalite + iptv */
    //   let sum_array_province_all  =  sum_array_province(path_merge_province);
    //   sum_array_province_all = set_provincebangkokbyregionvalue(path_regionall , path_merge_province ); 
    //   let path_cachingsumprovincearray = cacheing_multidimensionarray(sum_array_province_all , "caching_sumarryprovinceall" , dir_jsonlocate);


    //   let sum_array_province_top20  = sum_array_province(path_proviceaddrtop20); //  sum everychannel base on time slot ( top 20)
    //   sum_array_province_top20      = set_provincebangkokbyregionvalue(path20 , path_proviceaddrtop20 );
    //   sum_array_province_top20      = onaction_minusothertv_province(sum_array_province_all ,  sum_array_province_top20 , old_datearr); // psitotal - 20tvdigital
    //   let path_sum_array_province_top20 = cacheing_multidimensionarray(sum_array_province_top20 , "caching_sum_array_province_top20" , dir_jsonlocate);
      

    //   let youtube_summary_a      = set_provincebangkokbyregionvalue(path_province_region_youtube , path_province_youtube );
    //   let youtube_summary_b      = set_provincebangkokbyregionvalue(path_province_region_youtube , path_province_youtube );
      /* ========== eof : province  ===================*/


        let pool = await sql.connect(config);
        let req = new sql.Request(pool);
        let filename_ = 'overviewreportperminute_'+seconds_since_epoch(new Date()) +'.xlsx';
      
        $channel_top20array = tvdigitalchannel_ondemand("satalite" , null); // # find top20 channel from satalite channel id
        $channel_top20array_header = tvdigitalchannel_ondemand("satalite" , "header");
        var shortmonthname = "";

       
        let tmpid = generation_randomnumber()  +  "_" + Date.now();
        let result    = await create_newreport(tmpid , $foldername , date , pool); // case : create new temp id
        let insert_id = result.recordsets[0];
       

        /** ================== case : รวมคนดูตามแต่ละช่วงเวลาของทุกช่องมาเก็บไว้ใน array เพื่อไปเรียกใช้ภายหลัง ==================== */
        
        let dir_jsonlocate_cache     = "./json/overview_perminute/tvprogram_baseon_tpbs/" + $foldername + "/" + "cache_" + tmpid;
        // if (!fs.existsSync(dir_jsonlocate_cache)){
        //     fs.mkdirSync(dir_jsonlocate_cache);
        // }
       // let sumprovinceaddrtoptwenty_groupbychannel =  onaction_sumprovinceaddrtoptwenty_groupbychannel( $channel_top20array , path20 , path_proviceaddrtop20 , date_arr  , dir_jsonlocate_cache);
     
        
        /** ================== eof : รวมคนดูตามแต่ละช่วงเวลาของทุกช่องมาเก็บไว้ใน array เพื่อไปเรียกใช้ภายหลัง ==================== */


        var workbook_all = new excel.Workbook();  // Create a new instance of a Workbook class
        $row_wball = 5;
        var worksheet_all = workbook_all.addWorksheet("รายงานภาพรวมแสดงลักษณะ");   // Add Worksheets to the workbook
        let init_setfiledatestring_ = init_setfiledatestring(date);
        await date_arr.forEach(element => {
         
            element.forEach(v => {

                   /** ตัดปัญหาเรื่องจำ cache บน nodejs เลย require 2 ตัว เจอวิธีที่ดีกว่าเขียนแก้ได้เลยครับ */
                    // // case : psi total 
                    // let sum_array_province_all_a  = requireUncached(path_cachingsumprovincearray);
                    // let sum_array_province_all_b  =  requireUncached(path_cachingsumprovincearray);
                
                    // // case : other tv
                   
                    // let sum_array_province_top20_a  = requireUncached(path_sum_array_province_top20);
                    // let sum_array_province_top20_b  = requireUncached(path_sum_array_province_top20);
                   

                
                    /** eof : ตัดปัญหาเรื่องจำ cache บน nodejs เลย require 2 ตัว */

                     $field =`field_${v[2]}`;
                    var hour    = v[2]; // get hour
                  
                    // let top5_province    =   sort_province_top5peak(sum_array_province_all_a.data , hour); // case : get top 5 province on period
                    // let top5low_province =   sort_province_top5low(sum_array_province_all_b.data , hour); // case : get top 5 province on period

                    // let top5_province_top20    =   sort_province_top5peak(sum_array_province_top20_a.data , hour); // case : get top 5 province on period
                    // let top5low_province_top20 =   sort_province_top5low(sum_array_province_top20_b.data , hour); // case : get top 5 province on period

                    // let top5_province_topyt    =   sort_province_top5peak(youtube_summary_a , hour); // case : get top 5 province on period
                    // let top5low_province_topyt =   sort_province_top5low(youtube_summary_b , hour); // case : get top 5 province on period
                   


                    $row = 5;

                    

                    var workbook = new excel.Workbook();  // Create a new instance of a Workbook class
                   
                    var worksheet = workbook.addWorksheet('รายงานภาพรวมแสดงลักษณะ');   // Add Worksheets to the workbook

                
                    
                    /* =========== Create a reusable style =========== */
                    var style = get_wbstyle( workbook  , "numberformat");
                    var text_style_header = get_wbstyle(workbook , "text_style_header"); // create header style 
                    var text_style = get_wbstyle(workbook , "text_style"); // create table cell style
                    /* =========== eof : Create a reusable style =========== */

                    // Set value of cell A1 to 100 as a number type styled with paramaters of style ( แถว , หลัก )
                    worksheet  = create_excelheader(worksheet  , channels , text_style_header , null , date , "overview_perminute");
                    worksheet_all  = create_excelheader(worksheet_all  , channels , text_style_header , null , date , "overview_perminute");

                
                   
                    /** ============================  case : เพิ่มจำนวนรับชมสะสม 10 นาที  ==================================  */
                    // worksheet.cell($row, 7 ).number( 0  ).style(text_style);
                    // worksheet_all.cell($row_wball, 7 ).number( 0  ).style(text_style);
                    /** ============================  (eof) case : เพิ่มจำนวนรับชมสะสม 10 นาที  ==================================  */
                   
                    worksheet.cell($row, 1).string( "PSI TOTAL" ).style(text_style);
                    worksheet_all.cell($row_wball, 1).string( "PSI TOTAL" ).style(text_style);
                    let $psitotal          = `sum_allbaseontime['${$field}']`;
                    $psitotal = eval($psitotal);
                    worksheet.cell($row, 8 ).number( $psitotal  ).style(text_style);
                    worksheet_all.cell($row_wball, 8 ).number( $psitotal  ).style(text_style);
                    set_cell(worksheet , $row , date , text_style , v , merge_data_gender_obj, sum_gender_all , sum_array_provinceregion_all , null , 'overview_perminute');
                    set_cell(worksheet_all , $row_wball , date , text_style , v , merge_data_gender_obj, sum_gender_all , sum_array_provinceregion_all , null , 'overview_perminute');
                    // case :  เพศ
                    var $gender_male;
                    var $gender_female;
                    var $gender_none;
                    var $othertv_gender_male;
                    var $othertv_gender_female;
                    var $othertv_gender_none;
                    var $youtube_gender;
                    var $youtube_gender_male;
                    var $youtube_gender_female;
                    var $youtube_gender_none;
                    if(sum_gender_all != null){
                         $field =`field_${v[2]}`;
                         $gender        = get_totalgender(sum_gender_all , $field , 'overview');
                         $othertv_gender= get_totalgender(sum_gender_alltop20 , $field , 'overview');
                         $youtube_gender= get_totalgender(youtube_gender ,$field , 'overview' );
                         $gender_male   = $gender.male > 0 ? $gender.male : 0;
                         $gender_female = $gender.female > 0 ? $gender.female : 0;
                         $gender_none   = $gender.none > 0 ? $gender.none : 0;
                         $othertv_gender_male   = $othertv_gender.male > 0 ? $othertv_gender.male : 0;
                         $othertv_gender_male   = $gender_male - $othertv_gender_male;                
                         $othertv_gender_female = $othertv_gender.female > 0 ? $othertv_gender.female : 0;
                         $othertv_gender_female = $gender_female -  $othertv_gender_female;
                         $othertv_gender_none   = $othertv_gender.none > 0 ? $othertv_gender.none : 0;
                         $othertv_gender_none   = $gender_none  - $othertv_gender_none;

                         $youtube_gender_male   = $youtube_gender.male > 0 ? $youtube_gender.male : 0;
                         $youtube_gender_female = $youtube_gender.female > 0 ? $youtube_gender.female : 0;
                         $youtube_gender_none   = $youtube_gender.none > 0 ? $youtube_gender.none : 0;
                         set_cellgender( worksheet , $row , 9 , $gender_male , text_style);
                         set_cellgender( worksheet , $row , 10 , $gender_female , text_style);
                         set_cellgender( worksheet , $row , 11 , $gender_none , text_style);

                         set_cellgender( worksheet_all , $row_wball , 9 , $gender_male , text_style);
                         set_cellgender( worksheet_all , $row_wball , 10 , $gender_female , text_style);
                         set_cellgender( worksheet_all , $row_wball , 11 , $gender_none , text_style);
                     } 
                    // set_provincetop5(worksheet , $row , top5_province , top5low_province , text_style , null , $field);
                    // set_provincetop5(worksheet_all , $row , top5_province , top5low_province , text_style , null , $field);
                    ++$row;
                    ++$row_wball;
                    worksheet.cell($row, 1).string( "OTHER TV" ).style(text_style);
                    worksheet_all.cell($row_wball, 1).string( "OTHER TV" ).style(text_style);
               
                    var $sumtop_20 = `sum_top20['${$field}']`;
                    $sumtop_20     =  eval($sumtop_20);
                    let $other_tv  = parseInt($psitotal) - parseInt($sumtop_20);
                    worksheet.cell($row, 8 ).number( $other_tv  ).style(text_style);
                    worksheet_all.cell($row_wball, 8 ).number( $other_tv  ).style(text_style);

                    set_cell(worksheet , $row , date , text_style , v , null,null , sum_array_provinceregion_all , sum_array_provinceregion_top20 , 'overview_perminute');
                    set_cellgender( worksheet , $row , 9  , $othertv_gender_male , text_style);
                    set_cellgender( worksheet , $row , 10  , $othertv_gender_female , text_style);
                    set_cellgender( worksheet , $row , 11 , $othertv_gender_none , text_style);

                    /** work sheet all */
                    set_cell(worksheet_all , $row_wball , date , text_style , v , null,null , sum_array_provinceregion_all , sum_array_provinceregion_top20 , 'overview_perminute');
                    set_cellgender( worksheet_all , $row_wball , 9  , $othertv_gender_male , text_style);
                    set_cellgender( worksheet_all , $row_wball , 10  , $othertv_gender_female , text_style);
                    set_cellgender( worksheet_all , $row_wball , 11 , $othertv_gender_none , text_style);
                    // set_provincetop5(worksheet , $row , top5_province_top20 , top5low_province_top20 , text_style , null , $field);
                    // set_provincetop5(worksheet_all , $row , top5_province_top20 , top5low_province_top20 , text_style , null , $field);
                   // set_provincetop5(worksheet , $row , top5_province , top5low_province , text_style , "othertv" , field);
                    ++$row;
                    ++$row_wball;
                    worksheet.cell($row, 1).string( "ONLINE (YOUTUBE)" ).style(text_style);
                    worksheet_all.cell($row_wball, 1).string( "ONLINE (YOUTUBE)" ).style(text_style);
                    let yt_total = 0;
                    if(tvprogram_basetime_ytreport_data != null){
                        yt_total = set_sum( tvprogram_basetime_ytreport_data ,$field);
                        yt_total = parseInt(yt_total) > 0  ? parseInt(yt_total) : 0;
                        
                    }
                    worksheet.cell($row, 8 ).number( yt_total  ).style(text_style);
                    
                    set_cell(worksheet , $row , date , text_style , v , null,null , province_region_youtube , null , 'overview_perminute');
                    set_cellgender( worksheet , $row , 9  , $youtube_gender_male , text_style);
                    set_cellgender( worksheet , $row , 10  , $youtube_gender_female , text_style);
                    set_cellgender( worksheet , $row , 11 , $youtube_gender_none , text_style);
                  //  set_provincetop5(worksheet , $row , top5_province_topyt , top5low_province_topyt , text_style , null , $field);
                   

                    worksheet_all.cell($row_wball, 8 ).number( yt_total  ).style(text_style);
                    
                    set_cell(worksheet_all , $row_wball , date , text_style , v , null,null , province_region_youtube , null , 'overview_perminute');
                    set_cellgender( worksheet_all , $row_wball , 9  , $youtube_gender_male , text_style);
                    set_cellgender( worksheet_all , $row_wball , 10  , $youtube_gender_female , text_style);
                    set_cellgender( worksheet_all , $row_wball , 11 , $youtube_gender_none , text_style);
                    // set_provincetop5(worksheet_all , $row , top5_province_topyt , top5low_province_topyt , text_style , null , $field);
               
                    $loop_count=0;
                    $channel_top20array.forEach(channel_id => {
                        
                    
                        ++$row;
                        ++$row_wball;
                         /** ==== case : get province data by specific channel ======  */
                    
                        // let sum_array_province_specific_a  = sumprovinceaddrtoptwenty_groupbychannel[hour][channel_id]['sort_desc'];
                        // let top5_province_specific    =   sort_province_top5peak(sum_array_province_specific_a , hour); // case : get top 5 province on period
                        
           
                        // let sum_array_province_specific_b  = sumprovinceaddrtoptwenty_groupbychannel[hour][channel_id]['sort_asc'];
                        // let top5low_province_specific =   sort_province_top5low(sum_array_province_specific_b , hour); // case : get top 5 province on period
                        /** ==== eof : case get province data by specific chananel ======= */
                        
                        let keyfound      =  get_keybyvalue( merge_data_obj , channel_id);
                        worksheet.cell($row, 1).string( $channel_top20array_header[$loop_count] ).style(text_style); // case : set channel name
                        worksheet_all.cell($row_wball, 1).string( $channel_top20array_header[$loop_count] ).style(text_style); // case : set channel name
                        if(keyfound >= 0 && keyfound != undefined){
                            
                            $value =  `merge_data_obj[keyfound].${$field}`;
                            $value = eval($value);
                            if($value > 0){
                                worksheet.cell($row, 8 ).number( $value  ).style(text_style);
                                worksheet_all.cell($row_wball, 8 ).number( $value  ).style(text_style);
                                
                            }

                        }else{
                            worksheet.cell($row, 8 ).number(  0  ).style(text_style);
                            worksheet_all.cell($row_wball, 8 ).number(  0  ).style(text_style);
                        }
                        // case : set gender value region value
                        let fixed_channelarray  = [];
                        fixed_channelarray.push( channel_id );

                        $region_satalite_self =   find_channeldelete_specificarray(  "satalite" , path20 , fixed_channelarray); // get only this channel gender object
                        // console.log($region_satalite_self);
                        set_cell(worksheet , $row , date , text_style , v , null,null , $region_satalite_self , null , 'overview_perminute');
                        set_cell(worksheet_all , $row_wball , date , text_style , v , null,null , $region_satalite_self , null , 'overview_perminute');
                        
                        
                        let obj_gender_self     =  find_channeldelete_specificarray(  "satalite" , path_gendertop20 , fixed_channelarray); // get only this channel gender object
                        // console.log(obj_gender_self);
                        $gender      = get_totalgender(obj_gender_self , $field);
                        $gender_male   = $gender.male > 0 ? $gender.male : 0;
                        $gender_female = $gender.female > 0 ? $gender.female : 0;
                        $gender_none   = $gender.none > 0 ? $gender.none : 0;
                        set_cellgender( worksheet , $row , 9  , $gender_male , text_style);
                        set_cellgender( worksheet , $row , 10  , $gender_female , text_style);
                        set_cellgender( worksheet , $row , 11 , $gender_none , text_style);

                        /** workbook all */
                        set_cellgender( worksheet_all , $row_wball , 9  , $gender_male , text_style);
                        set_cellgender( worksheet_all , $row_wball , 10  , $gender_female , text_style);
                        set_cellgender( worksheet_all , $row_wball , 11 , $gender_none , text_style);


                        // set_provincetop5(worksheet , $row , top5_province_specific , top5low_province_specific , text_style , null , $field);
                        // set_provincetop5(worksheet_all , $row , top5_province_specific , top5low_province_specific , text_style , null , $field);


                        
                        /** ================== case  : sheet ค่าสะสม 10 นาทีที่ต้องเอา rating และ reach devices มาแสดง =====================   */
                        let key_find         =   channel_id+"_"+ hour;
                        let key_channelfound = get_keybyvalue_reportavgviewevery10minute( channeldailyrating_data ,key_find );
            
                        if(key_channelfound >= 0 && key_channelfound != undefined){
                            $reach_devices =  `channeldailyrating_data[key_channelfound].reach_devices`;
                            $rating =  `channeldailyrating_data[key_channelfound].rating`;
                            $reach_devices = eval($reach_devices);
                            $reach_devices = $reach_devices > 0 ? $reach_devices : 0;
                   
                            if($reach_devices > 0){
                            
                                 worksheet.cell($row, 7 ).number( $reach_devices  ).style(text_style);
                                 worksheet_all.cell($row_wball, 7 ).number( $reach_devices ).style(text_style);
                            
                                /** =================== (eof) set avg cell ======================  */
                                
                            }
                        }else{
                        
                            worksheet.cell($row, 7 ).number( 0  ).style(text_style);
                            worksheet_all.cell($row_wball, 7 ).number( 0  ).style(text_style);
                         
                        }
                        /** ================== (eof ) case  : sheet ค่าสะสม 10 นาทีที่ต้องเอา rating และ reach devices มาแสดง =====================   */

                        
                        ++ $loop_count; // count every channel
                    })


                    worksheet_all.cell(3, 2).string( "05:00:00 - 24:00:00" ).style(text_style);

                    let $filename  = 'overviewreportbehaviorpopulation_'+ hour + '_' + init_setfiledatestring_ +'_'+seconds_since_epoch(new Date()) +'.xlsx';
                    workbook.write(dir+ '/' + $filename);

                      /* ========= case : create excel file ( overview report ) */
                      var start_datetime = add_hour(v[0] , 7);
                      var end_datetime   = add_hour(v[1] , 7);
                      let result    =   create_newreportoverview( null  , insert_id[0].id , date , pool , start_datetime , end_datetime  , avg_minute , "specific_time" , $filename); // case : create new temp id
                      /* ========= (eof) : create excel file ( overview report ) */
                      ++$row;
                      ++$row_wball;
   
                    
                });
        });
        
      

        /* ========= case : create excel file ( overview report ) */
        var lastarr_key = date_arr[date_arr.length - 1];
        var start_datetime = add_hour(date_arr[0][0][0] , 7);
        var end_datetime   = add_hour(lastarr_key[0][1] , 7);
        if(avg_minute == 30){
            avg_minute = avg_minute * 2 * 24;  // 60 minute * 24 hour
        }
        let $24hour         = avg_minute * 24;
      
        
        let $filename_24hr  = 'overviewreportbehaviorpopulation_all_'+ init_setfiledatestring_+'_'+seconds_since_epoch(new Date()) +'.xlsx';
        workbook_all.write(dir+ '/' + $filename_24hr);

        let result_    =   create_newreportoverview( null  , insert_id[0].id , date , pool , start_datetime , end_datetime  , avg_minute , "all" , $filename_24hr); // case : create new temp id
       
       
        /* ========= (eof) : create excel file ( overview report ) */
       
        
      
}

function get_ratingdatatable(date , type = null){
    
    var month = get_dateobject(date, "month");
    var year = get_dateobject(date, "year");
    if(type == null){
       var rating_data_table = "rating_data_" + year + "_" + month;
    }else{
        var rating_data_table = "rating_data_" + year + "_" + month;
    }
    return rating_data_table;
}


function get_channeldailydevicetbl(date){
    var month = get_dateobject(date, "month");
    var year = get_dateobject(date, "year");
    var channel_daily_table =  "channel_daily_devices_summary_"+year+"_"+month;
    return channel_daily_table;
}
function get_wbstyle(workbook , type = null){
    var style;
    if(type == "numberformat"){
         style = workbook.createStyle({
            font: {
                color: '#FF0800',
                size: 12
            },
            numberFormat: '$#,##0.00; ($#,##0.00); -'
        });
    }else if(type == "text_style_header"){
         style = workbook.createStyle({
            font: {
                color: '#000000',
                size: 10
            },alignment: { 
                shrinkToFit: true, 
                wrapText: true
            },
            border: {
                left: {
                    style: 'thin',
                    color: 'black',
                },
                right: {
                    style: 'thin',
                    color: 'black',
                },
                top: {
                    style: 'thin',
                    color: 'black',
                },
                bottom: {
                    style: 'thin',
                    color: 'black',
                },
                outline: false,
            },
        });
    }else if(type == "text_style"){
         style = workbook.createStyle({
            font: {
                color: '#000000',
                size: 10
            },alignment: { 
                shrinkToFit: true, 
                wrapText: true
            },
            border: {
                left: {
                    style: 'thin',
                    color: 'black',
                },
                right: {
                    style: 'thin',
                    color: 'black',
                },
                top: {
                    style: 'thin',
                    color: 'black',
                },
                bottom: {
                    style: 'thin',
                    color: 'black',
                },
                outline: false,
            },
        });
    }
    return style;
}

const create_newreport = async (tmpid  = null , $foldername = null , date , pool , report_type = null , filename = null)  => {
    let report_type_name = report_type == null ? 'overview' : report_type;
    let $filename  =  filename == null ? "" : filename;
    const result = await pool.request()
    .input('date', sql.VarChar(50), date)
    .input('report_type', sql.VarChar(50), report_type_name)
    .input('foldername', sql.VarChar(50), $foldername)
    .input('tmpid' ,sql.VarChar(100) , tmpid)
    .input('filename' ,sql.VarChar(255)  ,$filename )
    .input('created_utc', sql.VarChar(50), Date.now())
    .query('insert into tpbs_report_store(date, report_type,foldername,tmpid,filename,created_utc) values(@date, @report_type, @foldername , @tmpid,@filename, @created_utc); SELECT SCOPE_IDENTITY() AS id;');
  
    return result;
}

function add_hour(date, hours) {
    const newDate = new Date(date);
    newDate.setHours(newDate.getHours() + hours);
    return newDate;
}

const create_newreportoverview = async (tmpid  = null , insert_id = null , date , pool , start_datetime  = null, end_datetime = null  , duration = null , type = null , filename = null ) => {
    const result = await pool.request()
    .input('type', sql.VarChar(50), type)
    .input('tpbs_report_store_id', sql.Int, insert_id )
    .input('start_datetime', sql.DateTime, start_datetime)
    .input('end_datetime' ,sql.DateTime , end_datetime)
    .input('filename' ,sql.VarChar(100) , filename)
    .input('duration', sql.Int, duration )
    .query('insert into tpbs_report_store_overview(type, tpbs_report_store_id,start_datetime,end_datetime , filename ,duration) values(@type, @tpbs_report_store_id, @start_datetime , @end_datetime, @filename,  @duration); SELECT SCOPE_IDENTITY() AS id;');
  
    return result;
}

function set_provincetop5(worksheet , $row , top5_province , top5low_province , text_style , type , field , col = null){

    if(type == "othertv"){

    }else{
        $col = 20;
        $length_limite = 24;
        if(col != null){
            $col = col;
            $length_limite = $col + 4;
        }
        for(i =0; i<5 ;i++){
            if($col <= $length_limite){
                let _field =  "top5_province[i]." + field;
                let value  = eval(_field);
                if(value== undefined){
                    value = 0;
                }

                 worksheet.cell($row, $col).string( top5_province[i].province_name + ";" + value).style(text_style);
                // worksheet.cell($row, $col).string( top5_province[i].province_name).style(text_style);
            }
            ++ $col;
        }

        for(i =0; i<5 ;i++){
            if($col > 24){
                let _field =  "top5low_province[i]." + field;
                let value  = eval(_field);
                if(value== undefined){
                    value = 0;
                }


                worksheet.cell($row, $col).string( top5low_province[i].province_name + ";" +value ).style(text_style);
                // worksheet.cell($row, $col).string( top5low_province[i].province_name).style(text_style);
            }
            ++ $col;
        }
    }



}
async function write_excelfile_overview(arr_data  , channels  , date, date_arr 
    , tvprogram_basetime_satalitereport_data_s , tvprogram_basetime_iptvreport_data_s , channels_id
    , tvprogram_basetime_ytreport_data , satalite_gender , iptv_gender 
    , province_region_satalite , province_region_iptv , youtube_gender , province_region_youtube 
    , avg_minute , province_satalite , province_iptv , province_youtube ){
    
        var excel = require('excel4node');
        
       

        $foldername = get_dateobject(date , "year") + "_"  +  get_dateobject(date , "month")+"_" + get_dateobject(date , "day");
        let dir     = "./excel/overview/tvprogram_baseon_tpbs/" + $foldername;
        if (!fs.existsSync(dir)){
            fs.mkdirSync(dir);
        }

    
        /** ============  case : create folder ========== */
        $foldername = get_dateobject(date , "year") + "_"  +  get_dateobject(date , "month")+"_" + get_dateobject(date , "day");
        
        let dir_jsonlocate     = "./json/overview/tvprogram_baseon_tpbs/" + $foldername;
        if (!fs.existsSync(dir_jsonlocate)){
            fs.mkdirSync(dir_jsonlocate);
        }
        /** ============  case : eof create folder ========== */

        let original_tvprogram_basetime_satalitereport_data = tvprogram_basetime_satalitereport_data_s;
        let original_tvprogram_basetime_iptv_data = tvprogram_basetime_iptvreport_data_s;
        // case : save log file
        let logsatalite    = JSON.stringify(original_tvprogram_basetime_satalitereport_data);
        var logsatalite_fn = 'logsatalite_' + Math.round(+new Date()/1000)+ ".json";
        fs.writeFileSync(dir_jsonlocate+'/'+logsatalite_fn, logsatalite);

        let logiptv    = JSON.stringify(original_tvprogram_basetime_iptv_data);
        var logiptv_fn = 'logiptv_' + Math.round(+new Date()/1000) + ".json";
        fs.writeFileSync(dir_jsonlocate+'/'+logiptv_fn, logiptv);
      
        
        let obj_satalite = require(dir_jsonlocate+'/'+logsatalite_fn);
        let obj_iptv     = require(dir_jsonlocate+'/'+logiptv_fn);
        
        var tvprogram_basetime_satalitereport_data = await find_channeldelete( "satalite" ,obj_satalite ); // list of top twenty digital tv
        var tvprogram_basetime_iptvreport_data     = await find_channeldelete( "iptv" ,obj_iptv );// list of top twenty digital tv
        
       
        
      

        let tvprogram_obj          = get_thistvprogram( tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data , channels_id); // case : get only tvprogrm of this channel 
       let merge_data_obj         =  merge_data(tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data ); // case : merge top20channel satalite and iptv
       
       let logtop20    = JSON.stringify(merge_data_obj);
       var logtop20_fn = 'logtop20' + Math.round(+new Date()/1000) + ".json";
       fs.writeFileSync(dir_jsonlocate+'/'+logtop20_fn, logtop20);
       
       let merge_data_obj_everychannel  =   merge_data(original_tvprogram_basetime_satalitereport_data , original_tvprogram_basetime_iptv_data ); 
       
        let sum_allbaseontime = sum_everychannel_baseontime(merge_data_obj_everychannel); //  sum everychannel base on time slot
        let sum_top20         = sum_everychannel_baseontime(merge_data_obj); // sum  top20  base on timeslot
        
        let logsatalitegender    = JSON.stringify(satalite_gender);
        var logsatalitegender_fn = 'logsatalitegender_' + Math.round(+new Date()/1000) + ".json";
        fs.writeFileSync(dir_jsonlocate+'/'+logsatalitegender_fn, logsatalitegender);
       
        let merge_data_gender_obj  = merge_data_gender_overview(satalite_gender , iptv_gender);
        var path_gendertop20                    = save_log( merge_data_gender_obj, "logmergegendertop20_"  , dir_jsonlocate );

    
      
        let sum_gender_all              = sum_array_gender(merge_data_gender_obj); //  sum everychannel base on time slot
      
        //let obj_satalitegender          =  Object.assign({}, satalite_gender); // case : convert array sum gender all to object
        let gender_satalitetop20        = await find_channeldelete(  "satalite" , satalite_gender); // get satalite gender top 20
        let gender_iptvtop20            = await find_channeldelete(  "iptv"     , iptv_gender); // get satalite gender top 20
        let merge_data_gender_obj_top20 = merge_data_gender_overview(gender_satalitetop20 , gender_iptvtop20); // merge top20 channel
        let sum_gender_alltop20         = sum_array_gender(merge_data_gender_obj_top20); //  sum everychannel base on time slot ( top 20)

        var logprovincesatalite_fn      = save_log( province_region_satalite, "logprovincesatalite_"  , dir_jsonlocate ); // case : save log satalite data
        var logprovinceiptv_fn          = save_log( province_region_iptv, "logprovinceiptv_"  , dir_jsonlocate ); // case : save log iptv data
        
        let obj_provincesatalite = require(logprovincesatalite_fn);
        let obj_provinceiptv     = require(logprovinceiptv_fn);
    
        /** =============== case :   region   ===================  */
        /** sum array province region :  all */
        let merge_data_province_obj   = merge_data_regionoverview(obj_provincesatalite , obj_provinceiptv, "element.province_region"); // get all province region
        let path_regionall  = save_log( merge_data_province_obj, "logprovinceall_"  , dir_jsonlocate ); // save log : province all  
        let sum_array_provinceregion_all  = sum_array_provinceregion(path_regionall); //  sum everychannel base on time slot
        /** sum array province region :  top 20 */
        let addr_satalitetop20        = await find_channeldelete(  "satalite" , obj_provincesatalite); // get satalite gender top 20
        let addr_iptvtop20            = await find_channeldelete(  "iptv"     , obj_provinceiptv); // get satalite gender top 20    
        let merge_data_provincetop20_obj   = merge_data_regionoverview(addr_satalitetop20 , addr_iptvtop20, "element.province_region"); // get all province region
        var path20                    = save_log( merge_data_provincetop20_obj, "logprovincetop20_"  , dir_jsonlocate );
        let sum_array_provinceregion_top20         = sum_array_provinceregion(path20); //  sum everychannel base on time slot ( top 20)



 
        var path_province_region_youtube                    = save_log( province_region_youtube, "logprovince_region_youtube_"  , dir_jsonlocate );

     /** =============== case :   eof region   ===================  */

      /** ==========  case : province   ============= */
      
      /** case  province  step (1) : save log  */
      var path_province_satalite                    = save_log( province_satalite, "logprovincesatalitegroup_"  , dir_jsonlocate );
      var path_province_iptv                        = save_log( province_iptv, "logprovinceiptvgroup_"  , dir_jsonlocate );
      var path_province_youtube                     = save_log( province_youtube, "logprovinceyoutubegroup_"  , dir_jsonlocate );
      /** case  province  step (2) : read json log file  */
      let obj_province_satalite = require(path_province_satalite); 
      let obj_province_iptv     = require(path_province_iptv);
      let obj_province_youtube  = require(path_province_youtube);
      /** case  province  step (3) : combine json data satalite and iptv  */
      let merge_data_provincegroup_obj  = merge_data_province_overview(obj_province_satalite , obj_province_iptv);
      /** case  province  step (4) : save log file from step (3) */
      var path_merge_province           = save_log( merge_data_provincegroup_obj, "logprovincemergedata_"  , dir_jsonlocate );


   
  
      /** case  province  step (5) : find othertv except from 20 tvdigital then delete it (satalite ) */
      let addr_provincesatalitetop20        = await find_channeldelete(  "satalite" , obj_province_satalite); // get satalite gender top 20
      /** case  province  step (6) : find othertv except from 20 tvdigital then delete it ( IPTV  ) */
      let addr_provinceiptvtop20            = await find_channeldelete(  "iptv"     , obj_province_iptv); // get satalite gender top 20    
      /** case  province  step (7) : combine them(from step 5 , 6 ) to one object */
      let merge_data_provinceaddrtop20_obj   = merge_data_province_overview(addr_provincesatalitetop20 , addr_provinceiptvtop20 ); // get all province
      /** case  province  step (8) : save log for using in future */
      var path_proviceaddrtop20                    = save_log( merge_data_provinceaddrtop20_obj, "logprovinceaddrtop20_"  , dir_jsonlocate );
    
 
      let sum_array_province_all  =  sum_array_province(path_merge_province);
      sum_array_province_all = set_provincebangkokbyregionvalue(path_regionall , path_merge_province )
     
    //   let sum_array_province_top20  = sum_array_province(path_proviceaddrtop20); //  sum everychannel base on time slot ( top 20)
    //   sum_array_province_top20      = onaction_minusothertv_province(sum_array_province_all ,  sum_array_province_top20 , date_arr); // psitotal - 20tvdigital
    //   sum_array_province_top20      = set_provincebangkokbyregionvalue(path20 , path_proviceaddrtop20 );
      // var path_top20afterminusvalue = save_log( JSON.parse(sum_array_province_top20), "path_top20afterminusvalue_"  , dir_jsonlocate );  
      /* ========== eof : province  ===================*/


        let pool = await sql.connect(config);
        let req = new sql.Request(pool);
        let filename_ = 'overviewreport_'+seconds_since_epoch(new Date()) +'.xlsx';
      
        $channel_top20array = tvdigitalchannel_ondemand("satalite" , null); // # find top20 channel from satalite channel id
        $channel_top20array_header = tvdigitalchannel_ondemand("satalite" , "header");
        var shortmonthname = "";

       
        let tmpid = generation_randomnumber()  +  "_" + Date.now();
        let result    = await create_newreport(tmpid , $foldername , date , pool); // case : create new temp id
        let insert_id = result.recordsets[0];
       

        
        let old_datearr = date_arr;
        var workbook_all = new excel.Workbook();  // Create a new instance of a Workbook class
        await date_arr.forEach(element => {
         
            element.forEach(v => {

                   /** ตัดปัญหาเรื่องจำ cache บน nodejs เลย require 2 ตัว เจอวิธีที่ดีกว่าเขียนแก้ได้เลยครับ */
                    // case : psi total
                    let sum_array_province_all_a  =  sum_array_province(path_merge_province);
                    sum_array_province_all_a = set_provincebangkokbyregionvalue(path_regionall , path_merge_province )
                    let sum_array_province_all_b  =  sum_array_province(path_merge_province);
                    sum_array_province_all_b = set_provincebangkokbyregionvalue(path_regionall , path_merge_province )
                    // case : other tv
                    // let sum_array_province_top20_a  =   sum_array_province_top20;
                    let sum_array_province_top20_a  = sum_array_province(path_proviceaddrtop20); //  sum everychannel base on time slot ( top 20)
                    sum_array_province_top20_a      = set_provincebangkokbyregionvalue(path20 , path_proviceaddrtop20 );
                    sum_array_province_top20_a      = onaction_minusothertv_province(sum_array_province_all ,  sum_array_province_top20_a , old_datearr); // psitotal - 20tvdigital
                 
                   
                    let sum_array_province_top20_b  = sum_array_province(path_proviceaddrtop20); //  sum everychannel base on time slot ( top 20)
                    sum_array_province_top20_b      = set_provincebangkokbyregionvalue(path20 , path_proviceaddrtop20 );
                    sum_array_province_top20_b      = onaction_minusothertv_province(sum_array_province_all ,  sum_array_province_top20_b , old_datearr); // psitotal - 20tvdigital
                   
                   

                         
                    let youtube_summary_a      = set_provincebangkokbyregionvalue(path_province_region_youtube , path_province_youtube );
                    let youtube_summary_b      = set_provincebangkokbyregionvalue(path_province_region_youtube , path_province_youtube );
                
                    /** eof : ตัดปัญหาเรื่องจำ cache บน nodejs เลย require 2 ตัว */

                     $field =`field_${v[2]}`;
                    var hour    = v[2]; // get hour
                  
                    let top5_province    =   sort_province_top5peak(sum_array_province_all_a , hour); // case : get top 5 province on period
                    let top5low_province =   sort_province_top5low(sum_array_province_all_b , hour); // case : get top 5 province on period

                    let top5_province_top20    =   sort_province_top5peak(sum_array_province_top20_a , hour); // case : get top 5 province on period
                    let top5low_province_top20 =   sort_province_top5low(sum_array_province_top20_b , hour); // case : get top 5 province on period

                    let top5_province_topyt    =   sort_province_top5peak(youtube_summary_a , hour); // case : get top 5 province on period
                    let top5low_province_topyt =   sort_province_top5low(youtube_summary_b , hour); // case : get top 5 province on period
                   


                   

                    

                    var workbook = new excel.Workbook();  // Create a new instance of a Workbook class
                   
                    var worksheet = workbook.addWorksheet('รายงานภาพรวม');   // Add Worksheets to the workbook

                    var worksheet_all = workbook_all.addWorksheet(v[2]);   // Add Worksheets to the workbook
                    
                    /* =========== Create a reusable style =========== */
                    var style = get_wbstyle( workbook  , "numberformat");
                    var text_style_header = get_wbstyle(workbook , "text_style_header"); // create header style 
                    var text_style = get_wbstyle(workbook , "text_style"); // create table cell style
                    /* =========== eof : Create a reusable style =========== */

                    // Set value of cell A1 to 100 as a number type styled with paramaters of style ( แถว , หลัก )
                    worksheet  = create_excelheader(worksheet  , channels , text_style_header , null , date);
                    worksheet_all  = create_excelheader(worksheet_all  , channels , text_style_header , null , date);
                   
                    $row = 5;
                    worksheet.cell($row, 1).string( "PSI TOTAL" ).style(text_style);
                    worksheet_all.cell($row, 1).string( "PSI TOTAL" ).style(text_style);
                    let $psitotal          = `sum_allbaseontime['${$field}']`;
                    $psitotal = eval($psitotal);
                    worksheet.cell($row, 7 ).number( $psitotal  ).style(text_style);
                    worksheet_all.cell($row, 7 ).number( $psitotal  ).style(text_style);
                    set_cell(worksheet , $row , date , text_style , v , merge_data_gender_obj, sum_gender_all , sum_array_provinceregion_all);
                    set_cell(worksheet_all , $row , date , text_style , v , merge_data_gender_obj, sum_gender_all , sum_array_provinceregion_all);
                    // case :  เพศ
                    var $gender_male;
                    var $gender_female;
                    var $gender_none;
                    var $othertv_gender_male;
                    var $othertv_gender_female;
                    var $othertv_gender_none;
                    var $youtube_gender;
                    var $youtube_gender_male;
                    var $youtube_gender_female;
                    var $youtube_gender_none;
                    if(sum_gender_all != null){
                         $field =`field_${v[2]}`;
                         $gender        = get_totalgender(sum_gender_all , $field , 'overview');
                         $othertv_gender= get_totalgender(sum_gender_alltop20 , $field , 'overview');
                         $youtube_gender= get_totalgender(youtube_gender ,$field , 'overview' );
                         $gender_male   = $gender.male > 0 ? $gender.male : 0;
                         $gender_female = $gender.female > 0 ? $gender.female : 0;
                         $gender_none   = $gender.none > 0 ? $gender.none : 0;
                         $othertv_gender_male   = $othertv_gender.male > 0 ? $othertv_gender.male : 0;
                         $othertv_gender_male   = $gender_male - $othertv_gender_male;                
                         $othertv_gender_female = $othertv_gender.female > 0 ? $othertv_gender.female : 0;
                         $othertv_gender_female = $gender_female -  $othertv_gender_female;
                         $othertv_gender_none   = $othertv_gender.none > 0 ? $othertv_gender.none : 0;
                         $othertv_gender_none   = $gender_none  - $othertv_gender_none;

                         $youtube_gender_male   = $youtube_gender.male > 0 ? $youtube_gender.male : 0;
                         $youtube_gender_female = $youtube_gender.female > 0 ? $youtube_gender.female : 0;
                         $youtube_gender_none   = $youtube_gender.none > 0 ? $youtube_gender.none : 0;
                         set_cellgender( worksheet , $row , 8 , $gender_male , text_style);
                         set_cellgender( worksheet , $row , 9 , $gender_female , text_style);
                         set_cellgender( worksheet , $row , 10 , $gender_none , text_style);

                         set_cellgender( worksheet_all , $row , 8 , $gender_male , text_style);
                         set_cellgender( worksheet_all , $row , 9 , $gender_female , text_style);
                         set_cellgender( worksheet_all , $row , 10 , $gender_none , text_style);
                     } 
                    set_provincetop5(worksheet , $row , top5_province , top5low_province , text_style , null , $field);
                    set_provincetop5(worksheet_all , $row , top5_province , top5low_province , text_style , null , $field);
                    ++$row;
                    worksheet.cell($row, 1).string( "OTHER TV" ).style(text_style);
                    worksheet_all.cell($row, 1).string( "OTHER TV" ).style(text_style);
               
                    var $sumtop_20 = `sum_top20['${$field}']`;
                    $sumtop_20     =  eval($sumtop_20);
                    let $other_tv  = parseInt($psitotal) - parseInt($sumtop_20);
                    worksheet.cell($row, 7 ).number( $other_tv  ).style(text_style);
                    worksheet_all.cell($row, 7 ).number( $other_tv  ).style(text_style);

                    set_cell(worksheet , $row , date , text_style , v , null,null , sum_array_provinceregion_all , sum_array_provinceregion_top20);
                    set_cellgender( worksheet , $row , 8  , $othertv_gender_male , text_style);
                    set_cellgender( worksheet , $row , 9  , $othertv_gender_female , text_style);
                    set_cellgender( worksheet , $row , 10 , $othertv_gender_none , text_style);

                    /** work sheet all */
                    set_cell(worksheet_all , $row , date , text_style , v , null,null , sum_array_provinceregion_all , sum_array_provinceregion_top20);
                    set_cellgender( worksheet_all , $row , 8  , $othertv_gender_male , text_style);
                    set_cellgender( worksheet_all , $row , 9  , $othertv_gender_female , text_style);
                    set_cellgender( worksheet_all , $row , 10 , $othertv_gender_none , text_style);
                    set_provincetop5(worksheet , $row , top5_province_top20 , top5low_province_top20 , text_style , null , $field);
                    set_provincetop5(worksheet_all , $row , top5_province_top20 , top5low_province_top20 , text_style , null , $field);
                   // set_provincetop5(worksheet , $row , top5_province , top5low_province , text_style , "othertv" , field);
                    ++$row;
                    worksheet.cell($row, 1).string( "ONLINE (YOUTUBE)" ).style(text_style);
                    worksheet_all.cell($row, 1).string( "ONLINE (YOUTUBE)" ).style(text_style);
                    let yt_total = 0;
                    if(tvprogram_basetime_ytreport_data != null){
                        yt_total = set_sum( tvprogram_basetime_ytreport_data ,$field);
                        yt_total = parseInt(yt_total) > 0  ? parseInt(yt_total) : 0;
                        
                    }
                    worksheet.cell($row, 7 ).number( yt_total  ).style(text_style);
                    
                    set_cell(worksheet , $row , date , text_style , v , null,null , province_region_youtube);
                    set_cellgender( worksheet , $row , 8  , $youtube_gender_male , text_style);
                    set_cellgender( worksheet , $row , 9  , $youtube_gender_female , text_style);
                    set_cellgender( worksheet , $row , 10 , $youtube_gender_none , text_style);
                    set_provincetop5(worksheet , $row , top5_province_topyt , top5low_province_topyt , text_style , null , $field);
                   

                    worksheet_all.cell($row, 7 ).number( yt_total  ).style(text_style);
                    
                    set_cell(worksheet_all , $row , date , text_style , v , null,null , province_region_youtube);
                    set_cellgender( worksheet_all , $row , 8  , $youtube_gender_male , text_style);
                    set_cellgender( worksheet_all , $row , 9  , $youtube_gender_female , text_style);
                    set_cellgender( worksheet_all , $row , 10 , $youtube_gender_none , text_style);
                    set_provincetop5(worksheet_all , $row , top5_province_topyt , top5low_province_topyt , text_style , null , $field);
                    // console.log(merge_data_obj_everychannel); 
                    $loop_count=0;
                    $channel_top20array.forEach(channel_id => {
                        
                    
                        ++$row;
                         /** ==== case : get province data by specific channel ======  */
                        let sum_array_province_specific_a  = sum_array_province(path_proviceaddrtop20 , channel_id); //  sum everychannel base on time slot ( top 20)
                        sum_array_province_specific_a      = set_provincebangkokbyregionvalue(path20 , path_proviceaddrtop20 , channel_id);

                        let sum_array_province_specific_b  = sum_array_province(path_proviceaddrtop20 , channel_id); //  sum everychannel base on time slot ( top 20)
                        sum_array_province_specific_b      = set_provincebangkokbyregionvalue(path20 , path_proviceaddrtop20 , channel_id);

                        let top5_province_specific    =   sort_province_top5peak(sum_array_province_specific_a , hour); // case : get top 5 province on period
                        let top5low_province_specific =   sort_province_top5low(sum_array_province_specific_b , hour); // case : get top 5 province on period
                        /** ==== eof : case get province data by specific chananel ======= */
                        
                        let keyfound      =  get_keybyvalue( merge_data_obj , channel_id);
                        worksheet.cell($row, 1).string( $channel_top20array_header[$loop_count] ).style(text_style); // case : set channel name
                        worksheet_all.cell($row, 1).string( $channel_top20array_header[$loop_count] ).style(text_style); // case : set channel name
                        if(keyfound >= 0 && keyfound != undefined){
                            
                            $value =  `merge_data_obj[keyfound].${$field}`;
                            $value = eval($value);
                            if($value > 0){
                                worksheet.cell($row, 7 ).number( $value  ).style(text_style);
                                worksheet_all.cell($row, 7 ).number( $value  ).style(text_style);
                                
                            }

                        }else{
                            worksheet.cell($row, 7 ).number(  0  ).style(text_style);
                            worksheet_all.cell($row, 7 ).number(  0  ).style(text_style);
                        }
                        // case : set gender value region value
                        let fixed_channelarray  = [];
                        fixed_channelarray.push( channel_id );

                        $region_satalite_self =   find_channeldelete_specificarray(  "satalite" , path20 , fixed_channelarray); // get only this channel gender object
                        // console.log($region_satalite_self);
                        set_cell(worksheet , $row , date , text_style , v , null,null , $region_satalite_self);
                        set_cell(worksheet_all , $row , date , text_style , v , null,null , $region_satalite_self);
                        
                        
                        let obj_gender_self     =  find_channeldelete_specificarray(  "satalite" , path_gendertop20 , fixed_channelarray); // get only this channel gender object
                        // console.log(obj_gender_self);
                        $gender      = get_totalgender(obj_gender_self , $field);
                        $gender_male   = $gender.male > 0 ? $gender.male : 0;
                        $gender_female = $gender.female > 0 ? $gender.female : 0;
                        $gender_none   = $gender.none > 0 ? $gender.none : 0;
                        set_cellgender( worksheet , $row , 8  , $gender_male , text_style);
                        set_cellgender( worksheet , $row , 9  , $gender_female , text_style);
                        set_cellgender( worksheet , $row , 10 , $gender_none , text_style);

                        /** workbook all */
                        set_cellgender( worksheet_all , $row , 8  , $gender_male , text_style);
                        set_cellgender( worksheet_all , $row , 9  , $gender_female , text_style);
                        set_cellgender( worksheet_all , $row , 10 , $gender_none , text_style);


                        set_provincetop5(worksheet , $row , top5_province_specific , top5low_province_specific , text_style , null , $field);
                        set_provincetop5(worksheet_all , $row , top5_province_specific , top5low_province_specific , text_style , null , $field);
                        
                        ++ $loop_count; // count every channel
                    })


                    let $filename  = 'overviewreport_'+ hour +'_'+seconds_since_epoch(new Date()) +'.xlsx';
                    workbook.write(dir+ '/' + $filename);

                    /* ========= case : create excel file ( overview report ) */
                    var start_datetime = add_hour(v[0] , 7);
                    var end_datetime   = add_hour(v[1] , 7);
                    let result    =   create_newreportoverview( null  , insert_id[0].id , date , pool , start_datetime , end_datetime  , avg_minute , "specific_time" , $filename); // case : create new temp id
                    /* ========= (eof) : create excel file ( overview report ) */
                
   
                    
                });
        });
         
      

        /* ========= case : create excel file ( overview report ) */
        var lastarr_key = date_arr[date_arr.length - 1];
        var start_datetime = add_hour(date_arr[0][0][0] , 7);
        var end_datetime   = add_hour(lastarr_key[0][1] , 7);
        if(avg_minute == 30){
            avg_minute = avg_minute * 2 * 24;  // 60 minute * 24 hour
        }
        // let $24hour         = avg_minute * 24;
        let $filename_24hr  = 'overviewreportall_'+ 24 +'_'+seconds_since_epoch(new Date()) +'.xlsx';
        workbook_all.write(dir+ '/' + $filename_24hr);

        let result_    =   create_newreportoverview( null  , insert_id[0].id , date , pool , start_datetime , end_datetime  , avg_minute , "all" , $filename_24hr); // case : create new temp id
        /* ========= (eof) : create excel file ( overview report ) */
       
        
      
}


function onaction_minusothertv_province(sum_array_province_all = null,  sum_array_province_top20 = null , date_arr = null){
    
    let oldarray   = sum_array_province_top20;
    let count = 0;
    Object.keys(sum_array_province_top20).forEach(key => {     
            var region_code =  sum_array_province_top20[key].region_code;
             //if(region_code == 12){
                let find_arraybyregion         = get_keybyvalue_provinceregioncode(sum_array_province_all ,  region_code);
             
                 //console.log("before : "+sum_array_province_top20[key].field_0_0);
                if(find_arraybyregion != undefined && find_arraybyregion != null){
                    
                    for (const [key_find, value] of Object.entries(find_arraybyregion)) {
                        if(key_find != 'province_name' && key_find != 'region_code'){
                            // console.log(value + ' - ' + eval("sum_array_province_top20[key]." + key_find));
                            let setnewvalue = "sum_array_province_top20[key]." + key_find + " = " +  value + " - " + eval("sum_array_province_top20[key]." + key_find);
                            eval(setnewvalue);
                            //console.log("key :  " +key);
                            //console.log("after : "+sum_array_province_top20[key].field_0_0);
                         }
                    
                    }
                    
                }
            // }
           
            
            

    });
    // console.log(sum_array_province_top20[2].field_0_0);
    return sum_array_province_top20;
}
function get_keybyvalue_provinceregioncode(arr , value , field = null){
    // var field = "object[key].";
    //Object.keys(object).find(key => object[key].region_code == value);

    var picked = arr.find(o => o.region_code == value);
    return picked;
}
function findKey(o, value  , start_datetime , end_datetime ) 
{
    
    var keyArr=[];
    $reach_devices = 0;
    o.forEach(function callback(element, key) {
       
        if (element.channels_id == value && set_starttime(element.created) >= start_datetime &&  set_endtime(element.created) <= end_datetime) 
        {
            //keyArr[] = element;
            keyArr.push(element);
        }
    })
    if(keyArr.length >0 )
    {
    return keyArr;
    }
    else
    {
        return null;
    }
}

function get_keybyvalue_channelsid_byperiodtime(arr , value ,  start_datetime , end_datetime , field = null ){

    // case : get this channel value by condition period time and channel id
    //var picked = arr.find(o => o.channels_id == value  && set_starttime(o.created) >= start_datetime &&  set_endtime(o.created) <= end_datetime );
    
    let picked = findKey(arr , value , start_datetime , end_datetime  )
    
    if(picked != undefined && picked != null){
       
    }else{
        picked = null;
    }
   
    return picked;
}

function get_keybyvalue_reportavgviewevery10minute(object , value , field = null){

    // case : get this channel value by condition period time and channel id
    // var picked = arr.find(o => o.h_m == value  );
    
    // if(picked != undefined && picked != null){
       
    // }else{
    //     picked = null;
    // }
    // return picked;
    return Object.keys(object).find(key => object[key].h_m == value);
}
function get_keybyvalue_byperiodtime(arr  ,  start_datetime , end_datetime , field = null ){

    // case : get this channel value by condition period time and channel id

    var picked = arr.find(o => set_starttime(o.created) >= start_datetime &&  set_endtime(o.created) <= end_datetime );

    if(picked != undefined && picked != null){
       
    }else{
        picked = null;
    }
    return picked;
}



function get_keybyvalue_channelid(object , value , field = null){

  

    return Object.keys(object).find(key => object[key].channels_id == value);
}
function save_log(merge_data , filename_prefix  , dir_jsonlocate , notuseprefix = null){


    let log    = JSON.stringify(merge_data);
    if(notuseprefix != null){
        var log_fn = filename_prefix + ".json";
    }else{
        var log_fn = filename_prefix+ '' + Math.round(+new Date()/1000) + ".json";
    }
    fs.writeFileSync(dir_jsonlocate+'/'+log_fn, log);
    let pathfile  = dir_jsonlocate+'/'+log_fn;

    return pathfile;
}
 
function set_cell(worksheet , $row , date , text_style , datearr_value , merge_data_gender_obj , sum_gender_all 
    , merge_data_province_obj = null
    , merge_data_top20 = null  , report_type = null){
    var dayname = get_dayname( date );
    var wkendwkday  =get_wkdaywkend( date , dayname );
    // case :   set month short name
    var shortmonthname = get_monthshortname( date );
    worksheet.cell($row, 2).string( shortmonthname ).style(text_style); // create short month name
    worksheet.cell($row, 3).string( wkendwkday ).style(text_style); // create wkend wkday text
    worksheet.cell($row, 4).string( dayname ).style(text_style); // create dayname text
    var dt = new Date( date );
    var date_text = dt.getDate() +  "/"  + (dt.getMonth() + 1) +  "/"  + dt.getFullYear();
    worksheet.cell($row, 5).string( date_text ).style(text_style); // create date text


    var starttime = get_starttime_notconvert( datearr_value );
    var endtime   = get_endtime_notconvert( datearr_value );
    if(report_type == "overview_daily"){
        starttime = "05:00:00";
        endtime   = "24:00:00";
    }

    /** =============== case : ถ้าไม่ใช่ report ข้อมูลรายละเอียดต่อนาที =======================   */
    if(report_type != 'avg_viewer'){
        worksheet.cell(3, 2).string( starttime + "-" + endtime ).style(text_style); // create date text
        worksheet.cell($row, 6).string( starttime + "-" + endtime ).style(text_style); // create date text


        // case :  ภาค
        $region       = [];
        $region_top20 = [];
        if(merge_data_province_obj != null){
            // case : psi total
            $field =`field_${datearr_value[2]}`;
            $region      = get_totalregion(merge_data_province_obj , $field );  
            
            if(merge_data_top20 == null){
                set_regioncell(worksheet , $row , text_style ,null ,  $region , null , report_type );
            }
        }

        if(merge_data_top20 != null){
            $field =`field_${datearr_value[2]}`;
            $region_top20      = get_totalregion(merge_data_top20 , $field );     
            set_regioncell(worksheet , $row , text_style , 'othertv' , $region , $region_top20 , report_type);
        }
    }
    /** =============== eof case : ถ้าไม่ใช่ report ข้อมูลรายละเอียดต่อนาที =======================   */
    
    

}
function set_regioncell(worksheet , $row , text_style , type = null , $region_satalite = null , $region_othertv = null , report_type = null){
    $region_text = "";
    $region_bk = "";
    $region_ct = ""; // กลาง
    $region_e = ""; // ตะวันออก
    $region_nt = ""; // ตะวันออกเฉียงเหนือ
    $region_n = ""; // เหนือ
    $region_s = ""; // ใต้
    $region_w = ""; // ตะวันตก
    $region_none= ""; // ระบุไม่ได้
    if($region.length > 0){
        $region_bk  = $region_satalite[7][1]; // satalite 
        $region_ct  = $region_satalite[0][1];
        $region_e   = $region_satalite[1][1];
        $region_nt  = $region_satalite[2][1];
        $region_n  = $region_satalite[3][1];
        $region_s  = $region_satalite[4][1];
        $region_w  = $region_satalite[5][1];
        $region_none  = $region_satalite[6][1];
        if(type == "othertv" && $region_satalite  != null){
            $regionothertv_bk  = $region_othertv[7][1];
            $regionothertv_ct  = $region_othertv[0][1];
            $regionothertv_e   = $region_othertv[1][1];
            $regionothertv_nt  = $region_othertv[2][1];
            $regionothertv_n  = $region_othertv[3][1];
            $regionothertv_s  = $region_othertv[4][1];
            $regionothertv_w  = $region_othertv[5][1];
            $regionothertv_none  = $region_othertv[6][1];

            $region_bk =    $region_bk - $regionothertv_bk;
            $region_ct =    $region_ct - $regionothertv_ct;
            $region_e =    $region_e - $regionothertv_e;
            $region_nt =    $region_nt - $regionothertv_nt;
            $region_n =    $region_n - $regionothertv_n;
            $region_s =    $region_s - $regionothertv_s;
            $region_w =    $region_w - $regionothertv_w;
            $region_none =    $region_none - $regionothertv_none;
        }

        // case : report ภาพรวมแสดงลักษณะ
        if(report_type == 'overview_perminute' || report_type == 'overview_daily'){
            worksheet.cell($row, 12 ).number( $region_bk).style(text_style);
            worksheet.cell($row, 13 ).number( $region_ct).style(text_style);
            worksheet.cell($row, 14 ).number( $region_e).style(text_style);
            worksheet.cell($row, 15 ).number( $region_nt).style(text_style);
            worksheet.cell($row, 16 ).number( $region_n).style(text_style);
            worksheet.cell($row, 17 ).number( $region_s).style(text_style);
            worksheet.cell($row, 18 ).number( $region_w).style(text_style);
            worksheet.cell($row, 19 ).number( $region_none).style(text_style);
        }else{
            worksheet.cell($row, 11 ).number( $region_bk).style(text_style);
            worksheet.cell($row, 12 ).number( $region_ct).style(text_style);
            worksheet.cell($row, 13 ).number( $region_e).style(text_style);
            worksheet.cell($row, 14 ).number( $region_nt).style(text_style);
            worksheet.cell($row, 15 ).number( $region_n).style(text_style);
            worksheet.cell($row, 16 ).number( $region_s).style(text_style);
            worksheet.cell($row, 17 ).number( $region_w).style(text_style);
            worksheet.cell($row, 18 ).number( $region_none).style(text_style);
        }
    }
}
function set_cellgender(worksheet , $row , $column , value , text_style){
    worksheet.cell($row, $column).number( value ).style(text_style); // create date text
}

function get_starttime_notconvert(datearr_value){
    var starttime = new Date(datearr_value[0]);
    var st_h  = starttime.getHours();
    st_h =  st_h < 10 ? "0"+st_h : st_h;
    var st_m  = starttime.getMinutes();
    st_m =  st_m < 10 ? "0"+st_m : st_m;
    var st_c  = starttime.getSeconds();
    st_c =  st_c < 10 ? "0"+st_c : st_c;

    starttime     =st_h + ":" +st_m  + ":" + st_c;
    return starttime;
    
}
function get_endtime_notconvert(datearr_value){
    var endtime = new Date(datearr_value[1]);
    var et_h  = endtime.getHours();
    et_h =  et_h < 10 ? "0"+et_h : et_h;
    var et_m  = endtime.getMinutes();
    et_m =  et_m < 10 ? "0"+et_m : et_m;
    var et_c  = endtime.getSeconds();
    et_c =  et_c < 10 ? "0"+et_c : et_c;

    endtime     =et_h + ":" +et_m  + ":" + et_c;
    return endtime;
    
}
function get_wkdaywkend(date , dayname){
    var wkendwkday = "";
   
    // case :   set weekend weekday 
    if(dayname == "Sat" || dayname == "Sun"){
        wkendwkday = "WKEND";
    }else{
        wkendwkday = "WKDAY";
    }
   return wkendwkday;
}

function create_excelheader(worksheet , channels , text_style_header , report = null , date = null , report_type = null , avg_minute = null){
    
    var year = get_dateobject(date, "year");
    var dt = new Date( date );
    var date_text = dt.getDate() +  "/"  + (dt.getMonth() + 1) +  "/"  + dt.getFullYear();
    if(report_type == 'avg_viewer'){
        /**=========== case : กรณี report ข้อมูลต่อนาที ========================================================= */
        worksheet.cell(1, 1).string("ปริมาณรับชมทุก" + avg_minute +"นาทีตามเวลาออกอากาศ").style(text_style_header);
        /**=========== eof case : กรณี report ข้อมูลต่อนาที ========================================================= */
    }else{
        worksheet.cell(1, 1).string("ปริมาณการรับชมภาพรวมตามเวลาออกอากาศ").style(text_style_header);
        worksheet.cell(2, 1).string("วันที่").style(text_style_header);
        worksheet.cell(3, 1).string(date_text).style(text_style_header);
    }
    worksheet.cell(4, 1).string("Channels").style(text_style_header);
    worksheet.cell(4, 2).string("MONTH").style(text_style_header);
    worksheet.cell(4, 3).string("WKDAY / WKEND").style(text_style_header);
    worksheet.cell(4, 4).string("DAY").style(text_style_header);
    worksheet.cell(4, 5).string("DD/MM/YYYY").style(text_style_header);


    /**============ case : set header สำหรับ report ข้อมูลรายละเอียดเลือกเวลา ( report ภาพรวม sheet2 ) =============================  */
    if(report_type == 'overview_perminute'){
        worksheet.cell(4, 6).string("Time").style(text_style_header);
        worksheet.cell(4, 7).string("จำนวนคนรับชมสะสม10นาที").style(text_style_header);
        worksheet.cell(4, 8).string("จำนวนเครื่องรวม(เข้าถึง)").style(text_style_header);
        worksheet.cell(4, 9).string("เพศชาย").style(text_style_header);
        worksheet.cell(4, 10).string("เพศหญิง").style(text_style_header);
        worksheet.cell(4, 11).string("ระบุไม่ได้(จำนวนเครื่อง)").style(text_style_header);
        worksheet.cell(4, 12).string("พื้นที่กรุงเทพ(จำนวนเครื่อง)").style(text_style_header);
        worksheet.cell(4, 13).string("พื้นที่ภาคกลาง(จำนวนเครื่อง)").style(text_style_header);
        worksheet.cell(4, 14).string("พื้นที่ภาคตะวันออก(จำนวนเครื่อง)").style(text_style_header);
        worksheet.cell(4, 15).string("พื้นที่ภาคตะวันออกเฉียงเหนือ(จำนวนเครื่อง)").style(text_style_header);
        worksheet.cell(4, 16).string("พื้นที่ภาคเหนือ(จำนวนเครื่อง)").style(text_style_header);
        worksheet.cell(4, 17).string("พื้นที่ภาคใต้(จำนวนเครื่อง)").style(text_style_header);
        worksheet.cell(4, 18).string("พื้นที่ภาคตะวันตก(จำนวนเครื่อง)").style(text_style_header);
        worksheet.cell(4, 19).string("ระบุไม่ได้(จำนวนเครื่อง)").style(text_style_header);
     /**============ (eof ) case : set header สำหรับ report ข้อมูลรายละเอียดเลือกเวลา ( report ภาพรวม sheet2 ) =============================  */
    }else{
    /**============ case : set header สำหรับ report ข้อมูลรายละเอียดต่อนาที ( report ภาพรวม sheet 3.1 , 3.2 ) =============================  */
        if(report_type != "avg_viewer" && report_type != "overview_daily"){
            worksheet.cell(4, 6).string("Time").style(text_style_header);
            worksheet.cell(4, 7).string("จำนวนเครื่องรวม(เข้าถึง)").style(text_style_header);
            worksheet.cell(4, 8).string("เพศชาย").style(text_style_header);
            worksheet.cell(4, 9).string("เพศหญิง").style(text_style_header);
            worksheet.cell(4, 10).string("ระบุไม่ได้(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 11).string("พื้นที่กรุงเทพ(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 12).string("พื้นที่ภาคกลาง(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 13).string("พื้นที่ภาคตะวันออก(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 14).string("พื้นที่ภาคตะวันออกเฉียงเหนือ(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 15).string("พื้นที่ภาคเหนือ(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 16).string("พื้นที่ภาคใต้(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 17).string("พื้นที่ภาคตะวันตก(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 18).string("ระบุไม่ได้(จำนวนเครื่อง)").style(text_style_header);
    /**============ case : (eof) set header สำหรับ report ข้อมูลรายละเอียดต่อนาที ( report ภาพรวม sheet 3.1 , 3.2 ) =============================  */
        }

        /**============ case : set header สำหรับ report ข้อมูลภาพรวม ( report ภาพรวม sheet 1 ) =============================  */
        if(report_type == "overview_daily"){
            worksheet.cell(4, 6).string("Time").style(text_style_header);
            worksheet.cell(4, 7).string("ค่าเฉลี่ยรับชมสะสม").style(text_style_header);
            worksheet.cell(4, 8).string("จำนวนเครื่องรวม(เข้าถึง)").style(text_style_header);
            worksheet.cell(4, 9).string("เพศชาย").style(text_style_header);
            worksheet.cell(4, 10).string("เพศหญิง").style(text_style_header);
            worksheet.cell(4, 11).string("ระบุไม่ได้(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 12).string("พื้นที่กรุงเทพ(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 13).string("พื้นที่ภาคกลาง(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 14).string("พื้นที่ภาคตะวันออก(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 15).string("พื้นที่ภาคตะวันออกเฉียงเหนือ(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 16).string("พื้นที่ภาคเหนือ(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 17).string("พื้นที่ภาคใต้(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 18).string("พื้นที่ภาคตะวันตก(จำนวนเครื่อง)").style(text_style_header);
            worksheet.cell(4, 19).string("ระบุไม่ได้(จำนวนเครื่อง)").style(text_style_header);

            worksheet.cell(4, 20).string("พื้นที่จังหวัด (เรียง 5 จังหวัดสูงสุด ต่ำสุด) ").style(text_style_header);
            worksheet.column(20).setWidth(30);
            worksheet.cell(4, 21).string("ลำดับ 1 (สูงสุด) ").style(text_style_header);
            worksheet.cell(4, 22).string("ลำดับ 2 (สูงสุด) ").style(text_style_header);
            worksheet.cell(4, 23).string("ลำดับ 3 (สูงสุด) ").style(text_style_header);
            worksheet.cell(4, 24).string("ลำดับ 4 (สูงสุด) ").style(text_style_header);
            worksheet.cell(4, 25).string("ลำดับ 5 (สูงสุด) ").style(text_style_header);
            worksheet.cell(4, 26).string("ลำดับ 1 (ต่ำสุด) ").style(text_style_header);
            worksheet.cell(4, 27).string("ลำดับ 2 (ต่ำสุด) ").style(text_style_header);
            worksheet.cell(4, 28).string("ลำดับ 3 (ต่ำสุด) ").style(text_style_header);
            worksheet.cell(4, 29).string("ลำดับ 4 (ต่ำสุด) ").style(text_style_header);
            worksheet.cell(4, 30).string("ลำดับ 5 (ต่ำสุด) ").style(text_style_header);
   
        }
         /**============ case : (eof) set header สำหรับ report ข้อมูลรายละเอียดต่อนาที ( report ภาพรวม sheet 3.1 , 3.2 ) =============================  */

      
        if(report_type != "overview_perminute"  && report_type != "overview_daily" && report_type != "avg_viewer"){
            worksheet.cell(4, 19).string("พื้นที่จังหวัด (เรียง 5 จังหวัดสูงสุด ต่ำสุด) ").style(text_style_header);
            worksheet.column(19).setWidth(30);
            worksheet.cell(4, 20).string("ลำดับ 1 (สูงสุด) ").style(text_style_header);
            worksheet.cell(4, 21).string("ลำดับ 2 (สูงสุด) ").style(text_style_header);
            worksheet.cell(4, 22).string("ลำดับ 3 (สูงสุด) ").style(text_style_header);
            worksheet.cell(4, 23).string("ลำดับ 4 (สูงสุด) ").style(text_style_header);
            worksheet.cell(4, 24).string("ลำดับ 5 (สูงสุด) ").style(text_style_header);
            worksheet.cell(4, 25).string("ลำดับ 1 (ต่ำสุด) ").style(text_style_header);
            worksheet.cell(4, 26).string("ลำดับ 2 (ต่ำสุด) ").style(text_style_header);
            worksheet.cell(4, 27).string("ลำดับ 3 (ต่ำสุด) ").style(text_style_header);
            worksheet.cell(4, 28).string("ลำดับ 4 (ต่ำสุด) ").style(text_style_header);
            worksheet.cell(4, 29).string("ลำดับ 5 (ต่ำสุด) ").style(text_style_header);
        }

    }

    
    return worksheet;

}
const get_channels = async (channels_id  = null) => {
    let pool = await sql.connect(config);
  
    let $query_channel     = build_query( "channel" ,null , channels_id , null , null );
    let pool_channels = await pool.request().query($query_channel);
    let channels_obj = pool_channels.recordsets[0];
    let channels;
    if(channels_obj.length != 0){
        channels = JSON.parse(JSON.stringify(channels_obj));
        //channels   =  channels_obj;
       
    }else{
        return [{
            "status": false,
            "result_code": "-005",
            "result_desc": "not found channel"
        }]
    }
    return channels;
  

}

async function get_deviceviewer_report(date, channels_id) {


    let pool = await sql.connect(config);
   
    // case :  channel
    let channels = await get_channels(channels_id);
    if(channels[0].status != undefined){
        return channels;
    }
    // case : thaipbs 
    let iptv_channel_id = get_iptvchannel_id( channels_id ); // case : get iptv channel id 
    
    var month = get_dateobject(date, "month");
    var year = get_dateobject(date, "year");
    var rating_data_table = "rating_data_" + year + "_" + month;
    var channel_daily_devices_summary_table = "channel_daily_devices_summary_"+year+"_"+month;

    let $query_tvprogram= get_tvprogram( channels_id  , date);
    var tvprogram_selectquery = "";

  

    let tvprogram     = await pool.request()
    .query($query_tvprogram);
    let tvprogram_recordset = tvprogram.recordsets;
    let tvprogram_basetime_satalitereport_data;
    let tvprogram_basetime_iptvreport_data;
    let rating_data_daily_satalite;
    let rating_data_daily_iptv;
    let tvprogram_basetime_ytreport_data;
    let satalite_gender;
    let iptv_gender;
    let province_region_satalite;
    let province_region_iptv;
    let channeldailyrating_data;
    let channeldailyrating_data_psitotal;
    let channeldailyrating_data_othertv;
    tvprogram_recordset_obj = "";
    if(tvprogram_recordset.length > 0){
        var tvprogram_recordset_obj = JSON.parse(JSON.stringify(tvprogram_recordset));
        let $query_satalite_minute = build_query( "minute" , rating_data_table , channels_id , date  , null , null, tvprogram_recordset_obj);
        let $query_IPTV     = build_query( "minute_s3app" , rating_data_table , iptv_channel_id , date , null , null , tvprogram_recordset_obj );
      

        rating_data_daily_satalite = await pool.request()
        .query($query_satalite_minute.temptable);
        rating_data_daily_iptv     = await pool.request()
        .query($query_IPTV.temptable);
        // case : drop old temp table and create new one
        $var_temptable = `#temp_data`;
        // case : write new query
        let $query_satalite = build_query( "tvprogram_basetime_satalitereport" , rating_data_table ,  null , date , null , null , tvprogram_recordset_obj );
        
        // case : crete new temp table satalite
        $query_createtemptable = "SELECT * INTO " + $var_temptable +" FROM ( "+ $query_satalite.temptable + " ) as r1";  // case : create new temporary table 
       
        let req = new sql.Request(pool);
        await req.batch($query_createtemptable);
       
        $query_tvprogrambaseonsatalite = ` SELECT merge_s3remote_id,channels_id ${$query_satalite.select_condition} FROM  #temp_data  inner join channels on #temp_data.channels_id = channels.id where channels.active = 1 group by channels_id,merge_s3remote_id`; // case : select temporary table data
       
        // case :  create new temp gender
        $var_temptable_gender ="#temp_gender"; 
        $query_createtempgender = "select * into "+$var_temptable_gender+" from (select devices_id as dvid ,gender from "+channel_daily_devices_summary_table+" group by devices_id,gender) r1";  // case : create new temporary table 
     
        let req_tempgender = new sql.Request(pool);
        await req_tempgender.batch($query_createtempgender);
        // case  : (query) group by gender ( satalite )
        let $query_satalite_gender = build_query( "groupby_gender_satalite" ,null , channels_id , null , null , null , tvprogram_recordset_obj, null , $var_temptable );
        // console.log($query_satalite_gender);
        // return;
        let query_tempgender_satalite = await req_tempgender.batch($query_satalite_gender.temptable);
        satalite_gender = query_tempgender_satalite.recordsets[0];
       
        // case : (query) group by province region satalite data
        let $query_satalite_addr = build_query( "groupby_addr_satalite" ,null , channels_id , null , null , null , tvprogram_recordset_obj, null , $var_temptable );
       
        let query_groupbyaddr_satalite = await req_tempgender.batch($query_satalite_addr.temptable);
        province_region_satalite = query_groupbyaddr_satalite.recordsets[0];


        // case :  (drop) temp satalite
        let query_temptable = await req.batch($query_tvprogrambaseonsatalite);
        await req.batch( "drop table " + $var_temptable ); // case : drop temp table
        tvprogram_basetime_satalitereport_data = query_temptable.recordset;
        
         // case : crete new temp table iptv
         $var_temptable_iptv = `#temp_data_iptv`;
         let $query_iptv = build_query( "tvprogram_basetime_iptvreport_s3app" , rating_data_table ,  null , date , null , null , tvprogram_recordset_obj );
         //tvprogram_basetime_iptvreport_data = get_iptvratingdata( $query_iptv , $var_temptable_iptv , pool);
         
         $query_createtemptable_iptv = "SELECT * INTO " + $var_temptable_iptv +" FROM ( "+ $query_iptv.temptable + " ) as r1";  // case : create new temporary table 
   
         let reqs3app = new sql.Request(pool);
         await reqs3app.batch($query_createtemptable_iptv);
     
         $query_tvprogrambaseiptv = ` SELECT tvchannels_id ${$query_iptv.select_condition} FROM  #temp_data_iptv group by tvchannels_id`; // case : select temporary table data
       
         let query_temptable_iptv = await reqs3app.batch($query_tvprogrambaseiptv);

        // case  : (query) group by gender ( iptv )
        let $query_iptv_gender;
        if(satalite_gender[0] != null){
            let $tvchannels_id      =satalite_gender[0].merge_s3remote_id;
            $query_iptv_gender = build_query( "groupby_gender_iptv" ,null , $tvchannels_id , null , null , null , tvprogram_recordset_obj, null , $var_temptable_iptv );
            let query_tempgender_iptv = await req_tempgender.batch($query_iptv_gender.temptable);
            iptv_gender = query_tempgender_iptv.recordsets[0];
        }else{
            iptv_gender = null;
        }
        
        // case : (query) group by province region iptv data
        if(province_region_satalite[0] != null){
            let $tvchannels_id_groupaddr = province_region_satalite[0].merge_s3remote_id;
            
            let $query_iptv_addr = build_query( "groupby_addr_iptv" ,null , $tvchannels_id_groupaddr , null , null , null , tvprogram_recordset_obj, null , $var_temptable_iptv );
         
            let query_groupbyaddr_iptv = await req_tempgender.batch($query_iptv_addr.temptable);
            province_region_iptv = query_groupbyaddr_iptv.recordsets[0];
        }else{
            province_region_iptv = null;
        }
     

        // casae : (drop) temp gender
        await req_tempgender.batch( "drop table " + $var_temptable_gender ); // case : drop temp table

         // case :  (drop) temp iptv
         await reqs3app.batch( "drop table " + $var_temptable_iptv ); // case : drop temp table
         tvprogram_basetime_iptvreport_data = query_temptable_iptv.recordset;

         // case : create new temp youtube view
         $var_temptable_youtube = `#temp_data_youtube`;
         let youtube_data_table = "youtube_rating_log_" + year + "_" + month;
         let $query_youtube = build_query( "youtubeviewreport_satalite" , rating_data_table ,  null , date , null , null , tvprogram_recordset_obj ,  youtube_data_table);
         //tvprogram_basetime_iptvreport_data = get_iptvratingdata( $query_youtube , $var_temptable_youtube , pool);
         
         $query_createtemptable_youtube = "SELECT * INTO " + $var_temptable_youtube +" FROM ( "+ $query_youtube.temptable + " ) as r1";  // case : create new temporary table 
         let reqs3ratingyt = new sql.Request(pool);
         await reqs3ratingyt.batch($query_createtemptable_youtube);

         $query_tvprogrambaseyoutube = ` SELECT devices_id ${$query_youtube.select_condition} FROM  #temp_data_youtube group by devices_id`; // case : select temporary table data
         let query_temptable_yt = await reqs3ratingyt.batch($query_tvprogrambaseyoutube);
         await reqs3ratingyt.batch( "drop table " + $var_temptable_youtube ); // case : drop temp table
         tvprogram_basetime_ytreport_data = query_temptable_yt.recordset;

        /** ================ case : get channel daily rating top 20 channel ============================  */
                
        let $query_getchanneldailyrating = build_query( "get_channeldailyrating_dailyreport" , null ,  null , date , null , null , null );
        let execute_queryraw = await reqs3ratingyt.batch($query_getchanneldailyrating.rawquery);
        channeldailyrating_data= execute_queryraw.recordset;

        let $query_getchanneldailyrating_psitotal = build_query( "get_channeldailyrating_dailyreport_psitotal" , null ,  null , date , null , null , tvprogram_recordset_obj );
        let execute_queryraw_psitotal = await reqs3ratingyt.batch($query_getchanneldailyrating_psitotal.rawquery);
        channeldailyrating_data_psitotal= execute_queryraw_psitotal.recordset;


        let $query_getchanneldailyrating_othertv = build_query( "get_channeldailyrating_dailyreport_othertv" , null ,  null , date , null , null , tvprogram_recordset_obj );
        let execute_queryraw_othertv = await reqs3ratingyt.batch($query_getchanneldailyrating_othertv.rawquery);
        channeldailyrating_data_othertv= execute_queryraw_othertv.recordset;

      
        /** ============================================================================================ */

       

    }

    
    var arr_data = set_arrdata( [] );
   
    if (rating_data_daily_satalite != null ) {
        let rating_data_daily_satalite_obj = rating_data_daily_satalite.recordsets[0];
        var rating_data_daily_satalite_ = JSON.parse(JSON.stringify(rating_data_daily_satalite_obj));
  
        if (rating_data_daily_satalite_ != null) {

            //  arr_data = set_satalitetotal_arr( arr_data , rating_data_daily_satalite_); // case : set satalite data
            if(rating_data_daily_iptv != null){
                let rating_data_daily_iptv_obj = rating_data_daily_iptv.recordsets[0];
             
                var rating_data_daily_iptv_ = JSON.parse(JSON.stringify(rating_data_daily_iptv_obj));
                arr_data = merge_andsortallofthisfuck( rating_data_daily_satalite_ , rating_data_daily_iptv_);
            }
            else{
                arr_data = rating_data_daily_satalite_;
            }
           
           

           
             let $filename =   await write_excelfile( arr_data  , channels , tvprogram_recordset_obj 
                , tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data , channels_id
                , tvprogram_basetime_ytreport_data , satalite_gender , iptv_gender 
                , province_region_satalite , province_region_iptv , date  , channeldailyrating_data , channeldailyrating_data_psitotal , channeldailyrating_data_othertv);
              
            const file = $filename;
          
            return   [{
                "status": true,
                "data": file,
                "result_code": "000",
                "result_desc": "Success"
            }];
              
           
           
        } else {
            return [{
                "status": false,
                "result_code": "-005",
                "result_desc": "not found data"
            }];
        }


    }
}
async function get_sataliteratingdata(){
    
}
async function get_iptvratingdata($query_iptv ,$var_temptable_iptv , pool ){
       
    $query_createtemptable_iptv = "SELECT * INTO " + $var_temptable_iptv +" FROM ( "+ $query_iptv.temptable + " ) as r1";  // case : create new temporary table 
    let reqs3app = new sql.Request(pool);
    await reqs3app.batch($query_createtemptable_iptv);

    $query_tvprogrambaseiptv = ` SELECT tvchannels_id ${$query_iptv.select_condition} FROM  #temp_data_iptv group by tvchannels_id`; // case : select temporary table data
    let query_temptable_iptv = await reqs3app.batch($query_tvprogrambaseiptv);
    await reqs3app.batch( "drop table " + $var_temptable_iptv ); // case : drop temp table
    let tvprogram_basetime_iptvreport_data = query_temptable_iptv.recordset;
    return tvprogram_basetime_iptvreport_data;
}
function set_satalitetotal_arr(arr_data , rating_data_daily_satalite_){
    rating_data_daily_satalite_.forEach(element => {
        element.forEach(value => {
           
            var _h = value.ชั่วโมงที่;
            var _m = parseInt(value.นาทีที่);
            var _v = parseInt(value.กล่องรับชม);
            _v = _v > 0 ? _v : 0;
            arr_data[_h][_m][2] += _v; 
            
            
        });
    });

    return arr_data;
}
function set_iptvtotal_arr(arr_data , rating_data_daily_iptv_){
    rating_data_daily_iptv_.forEach(element => {
        element.forEach(value => {
           
            var _h = value.ชั่วโมงที่;
            var _m = parseInt(value.นาทีที่);
            var _v = parseInt(value.กล่องรับชม);
            _v = _v > 0 ? _v : 0;
            arr_data[_h][_m][2] += _v; 
            
            
        });
    });

    return arr_data;
}
function set_arrdata(arr_data){
    for($time =0 ; $time<=23; $time ++){
     
        var text      = $time < 10 ? "0"+$time+":00-"+"0"+$time+":59" :  $time+":00-"+$time+":59";
        var arr_minute= [];
        for($minute =0;$minute<=59;$minute ++){
            arr_minute.push([ text , $minute , 0]);
        }
        arr_data.push(arr_minute);
    }

    return arr_data;
}
function get_iptvchannel_id(channels_id){
    let iptv_channel_id;
    if(channels_id == 252){ // case : tpbs
        iptv_channel_id = 64;
    }
    return iptv_channel_id;

}
function build_query(type, rating_data_table = null, channels_id = null, date = null  , report = null , select_condition = null , tvprogram_recordset_obj = null , youtube_data_table = null , temp_table = null , avg_perminute = null , start_datetime_string = null , end_datetime_string = null , condition_obj = null) {
    let $query;
    $20channel_where  = list20tvchannel_ondemand();
    $20channel_where_notin = listothertv20tvchannel_ondemand();
    if (type == 'minute' ) {
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        tvprogram_recordset_obj[0].forEach(element => {
            
        
            var start_time =  set_starttime( element.start_time );
            var end_time   =  set_endtime( element.end_time );
            select_condition +=   ` ,Count(case when r1.d_${element.id} >= 1 then 1 end) as 'field_${element.id}'`;    
            subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${date} ${start_time}' AND startview_datetime < '${date} ${end_time}' THEN 1 END ) AS 'd_${element.id}'`;
    
        });


        $query    =  "SELECT channels.id as channels_id,merge_s3remote_id,r1.hour as hour ,r1.MINUTE as minute ,(CONVERT(varchar(12),r1.hour) + '_' + CONVERT(varchar(12),r1.MINUTE)) as h_m  "+
        select_condition+" FROM ( SELECT  DATEPART(HOUR, startview_datetime) as hour,DATEPART(MINUTE, startview_datetime) as MINUTE,devices_id,count(*) as count_allratingrecord ,channels_id"+subquery_select_codition+" FROM dbo."+rating_data_table+"   WHERE startview_datetime >= '"+date+" 00:00:01' AND startview_datetime <= '"+date+" 23:59:59' AND channels_id > 0  AND channels_id = "+channels_id+" GROUP BY  channels_id, DATEPART(YEAR, startview_datetime),DATEPART(MONTH, startview_datetime),DATEPART(DAY, startview_datetime),DATEPART(HOUR, startview_datetime),DATEPART(minute , startview_datetime),devices_id )r1 inner join channels on r1.channels_id = channels.id where channels.active = 1 group by r1.hour,r1.MINUTE,channels.id,channels.merge_s3remote_id   order by r1.hour , r1.MINUTE ";
       
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

       
            
    }else if (type == 'overview_minute' ) {
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when r1.d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });

    

        $query    =  "SELECT channels.id as channels_id,merge_s3remote_id,r1.hour as hour ,r1.MINUTE as minute ,CONVERT(varchar(12),merge_s3remote_id) + '_'+(CONVERT(varchar(12),r1.hour) + '_' + CONVERT(varchar(12),r1.MINUTE)) AS h_m  "+
        select_condition+" FROM ( SELECT  DATEPART(HOUR, startview_datetime) as hour,CAST(DATEPART(MINUTE, startview_datetime)/"+avg_perminute+" AS char(2)) as MINUTE,devices_id,count(*) as count_allratingrecord ,channels_id"+subquery_select_codition+" FROM "+rating_data_table+"   WHERE startview_datetime >= '"+date+" 00:00:01' AND startview_datetime <= '"+date+" 23:59:59' AND channels_id > 0  GROUP BY  channels_id, DATEPART(YEAR, startview_datetime),DATEPART(MONTH, startview_datetime),DATEPART(DAY, startview_datetime),DATEPART(HOUR, startview_datetime),DATEPART(MINUTE, startview_datetime)/"+avg_perminute+" ,devices_id )r1 inner join channels on r1.channels_id = channels.id where channels.active = 1 group by r1.hour,r1.MINUTE,channels.id,channels.merge_s3remote_id   order by r1.hour , r1.MINUTE ";
        
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }else if (type == 'overview_minute_api' ) {
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        // console.log(date_arr);
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when r1.d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });

    
        let condition_where_channelid = "";
        if(channels_id != null){
             condition_where_channelid = " AND channels_id = " + channels_id;
        }
        $query    =  "SELECT channels.id as channels_id,merge_s3remote_id,r1.hour as hour ,r1.MINUTE as minute ,CONVERT(varchar(12),merge_s3remote_id) + '_'+(CONVERT(varchar(12),r1.hour) + '_' + CONVERT(varchar(12),r1.MINUTE)) AS h_m  "+
        select_condition+" FROM ( SELECT  DATEPART(HOUR, startview_datetime) as hour,CAST(DATEPART(MINUTE, startview_datetime)/"+avg_perminute+" AS char(2)) as MINUTE,devices_id,count(*) as count_allratingrecord ,channels_id"+subquery_select_codition+" FROM "+rating_data_table+"   WHERE startview_datetime > '"+start_datetime_string+"' AND startview_datetime < '"+end_datetime_string+"' AND channels_id > 0  "+condition_where_channelid+" GROUP BY  channels_id, DATEPART(YEAR, startview_datetime),DATEPART(MONTH, startview_datetime),DATEPART(DAY, startview_datetime),DATEPART(HOUR, startview_datetime),DATEPART(MINUTE, startview_datetime)/"+avg_perminute+" ,devices_id )r1 inner join channels on r1.channels_id = channels.id where channels.active = 1 group by r1.hour,r1.MINUTE,channels.id,channels.merge_s3remote_id    order by r1.hour , r1.MINUTE    ";
       // console.log($query);return; 
       // order by r1.hour , r1.MINUTE   
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }else if (type == 'overview_minute_perminute' ) {
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when r1.d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });


        $query    =  "SELECT channels.id as channels_id,merge_s3remote_id,r1.hour as hour ,r1.MINUTE as minute ,CONVERT(varchar(12),merge_s3remote_id) + '_'+(CONVERT(varchar(12),r1.hour) + '_' + CONVERT(varchar(12),r1.MINUTE)) AS h_m  "+
        
        select_condition+" FROM ( SELECT  DATEPART(HOUR, startview_datetime) as hour,CAST(DATEPART(MINUTE,dateadd(minute,(datediff(minute,0,startview_datetime)/"+avg_perminute+")*"+avg_perminute+",0)) AS char(2)) as MINUTE,devices_id,count(*) as count_allratingrecord ,channels_id"+subquery_select_codition+" FROM "+rating_data_table+"   WHERE startview_datetime >= '"+date+" 00:00:01' AND startview_datetime <= '"+date+" 23:59:59' AND channels_id > 0  GROUP BY  channels_id, DATEPART(YEAR, startview_datetime),DATEPART(MONTH, startview_datetime),DATEPART(DAY, startview_datetime),DATEPART(HOUR, startview_datetime),dateadd(minute,(datediff(minute,0,startview_datetime)/"+avg_perminute+")*"+avg_perminute+",0)  ,devices_id )r1 inner join channels on r1.channels_id = channels.id where channels.active = 1 group by r1.hour,r1.MINUTE,channels.id,channels.merge_s3remote_id   order by r1.hour , r1.MINUTE ";
    
        
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }else if (type == 'overview_daily_satalite' ) {
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when r1.d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });


        $query    =  "SELECT channels.id as channels_id ,CONVERT(varchar(12),merge_s3remote_id) + '_'+'5' + '_' + '24' AS h_m  "+
        
        select_condition+" FROM ( SELECT devices_id,count(*) as count_allratingrecord ,channels_id"+subquery_select_codition+" FROM "+rating_data_table+"   WHERE startview_datetime >= '"+date+" 00:00:01' AND startview_datetime <= '"+date+" 23:59:59' AND channels_id > 0  GROUP BY  channels_id, DATEPART(YEAR, startview_datetime),DATEPART(MONTH, startview_datetime),DATEPART(DAY, startview_datetime),devices_id )r1 inner join channels on r1.channels_id = channels.id where channels.active = 1 group by channels.id,channels.merge_s3remote_id   order by channels.id ";
        
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }else if (type == 'overview_daily_satalite_api' ) {
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        var start_datetime;
        var     end_datetime ;
        date_arr.forEach(element => {
            element.forEach(v => {

                start_datetime = v[0];
                end_datetime   = v[1];
                var h_m        = v[2];
                // select_condition +=   ` ,Count(case when r1.d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                select_condition +=   ` ,Count(case when r1.d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });

        let subwhere = " and ( channels_id  = 273 or channels_id = 277 or channels_id =271 or channels_id = 263 or channels_id = 257   )  ";
        if(channels_id != null){
            subwhere = " and channels_id = " + channels_id + " ";
        }
        $query    =  "SELECT channels.id as channels_id ,channels.channel_name , sum(count_allratingrecord) as total_devices,CONVERT(varchar(12),merge_s3remote_id) + '_'+'5' + '_' + '24' AS h_m  "+
        
        select_condition+" FROM ( SELECT devices_id,count(*) as count_allratingrecord ,channels_id"+subquery_select_codition+" FROM "+rating_data_table+"   WHERE startview_datetime >= '"+start_datetime+"' AND startview_datetime <= '"+end_datetime+"' AND channels_id > 0  GROUP BY  channels_id, DATEPART(YEAR, startview_datetime),DATEPART(MONTH, startview_datetime),DATEPART(DAY, startview_datetime),devices_id )r1 inner join channels on r1.channels_id = channels.id where channels.active = 1  " + subwhere +"  group by channels.id,channels.channel_name,channels.merge_s3remote_id   order by channels.id ";
        // console.log($query);return;
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }else if(type == 'overview_minute_s3app'){
        
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when r1.d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });

       
        $query    =  "SELECT r1.tvchannels_id,r1.hour as hour ,r1.MINUTE as minute,CONVERT(varchar(12),tvchannels_id) + '_'+(CONVERT(varchar(12),r1.hour) + '_' + CONVERT(varchar(12),r1.MINUTE)) AS h_m "+
        select_condition+" FROM ( SELECT  DATEPART(HOUR, startview_datetime) as hour,DATEPART(MINUTE, startview_datetime) / "+avg_perminute+" as MINUTE,devices_id,count(*) as count_allratingrecord ,tvchannels_id"+subquery_select_codition+" FROM "+rating_data_table+" WHERE startview_datetime >= '"+date+" 00:00:01' AND startview_datetime <= '"+date+" 23:59:59' AND tvchannels_id > 0   GROUP BY  tvchannels_id, DATEPART(YEAR, startview_datetime),DATEPART(MONTH, startview_datetime),DATEPART(DAY, startview_datetime),DATEPART(HOUR, startview_datetime),DATEPART(minute , startview_datetime) / "+avg_perminute+",devices_id )r1 group by r1.hour,r1.MINUTE,r1.tvchannels_id order by r1.tvchannels_id,r1.hour,r1.MINUTE ";
     
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
        
   
    }else if(type == 'overview_minute_s3app_api'){
        
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when r1.d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });

        let condition_where_channelid = "";
        if(channels_id != null){
             condition_where_channelid = " AND tvchannels_id = " + channels_id;
        }
        $query    =  "SELECT r1.tvchannels_id,r1.hour as hour ,r1.MINUTE as minute,CONVERT(varchar(12),tvchannels_id) + '_'+(CONVERT(varchar(12),r1.hour) + '_' + CONVERT(varchar(12),r1.MINUTE)) AS h_m "+
        select_condition+" FROM ( SELECT  DATEPART(HOUR, startview_datetime) as hour,DATEPART(MINUTE, startview_datetime) / "+avg_perminute+" as MINUTE,devices_id,count(*) as count_allratingrecord ,tvchannels_id"+subquery_select_codition+" FROM "+rating_data_table+" WHERE startview_datetime > '"+start_datetime_string+ "' AND startview_datetime < '"+end_datetime_string+"' AND tvchannels_id > 0  "+condition_where_channelid+" GROUP BY  tvchannels_id, DATEPART(YEAR, startview_datetime),DATEPART(MONTH, startview_datetime),DATEPART(DAY, startview_datetime),DATEPART(HOUR, startview_datetime),DATEPART(minute , startview_datetime) / "+avg_perminute+",devices_id )r1 group by r1.hour,r1.MINUTE,r1.tvchannels_id  order by  r1.tvchannels_id,r1.hour,r1.MINUTE ";
        //order by r1.tvchannels_id,r1.hour,r1.MINUTE
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
        
   
    }else if(type == 'overview_minute_s3app_perminute'){
        
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when r1.d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });

       
        $query    =  "SELECT r1.tvchannels_id,r1.hour as hour ,r1.MINUTE as minute,CONVERT(varchar(12),tvchannels_id) + '_'+(CONVERT(varchar(12),r1.hour) + '_' + CONVERT(varchar(12),r1.MINUTE)) AS h_m "+
        select_condition+" FROM ( SELECT  DATEPART(HOUR, startview_datetime) as hour,CAST(DATEPART(MINUTE,dateadd(minute,(datediff(minute,0,startview_datetime)/"+avg_perminute+")*"+avg_perminute+",0)) AS char(2)) as MINUTE,devices_id,count(*) as count_allratingrecord ,tvchannels_id"+subquery_select_codition+" FROM "+rating_data_table+" WHERE startview_datetime >= '"+date+" 00:00:01' AND startview_datetime <= '"+date+" 23:59:59' AND tvchannels_id > 0   GROUP BY  tvchannels_id, DATEPART(YEAR, startview_datetime),DATEPART(MONTH, startview_datetime),DATEPART(DAY, startview_datetime),DATEPART(HOUR, startview_datetime),dateadd(minute,(datediff(minute,0,startview_datetime)/"+avg_perminute+")*"+avg_perminute+",0),devices_id )r1 group by r1.hour,r1.MINUTE,r1.tvchannels_id order by r1.tvchannels_id,r1.hour,r1.MINUTE ";
     
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
        
   
    }else if(type == 'overview_daily_s3app'){
        
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when r1.d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });

       
        $query    =  "SELECT r1.tvchannels_id,CONVERT(varchar(12),tvchannels_id) + '_'+'5' + '_' + '24' AS h_m "+
        select_condition+" FROM ( SELECT  devices_id,count(*) as count_allratingrecord ,tvchannels_id"+subquery_select_codition+" FROM "+rating_data_table+" WHERE startview_datetime >= '"+date+" 00:00:01' AND startview_datetime <= '"+date+" 23:59:59' AND tvchannels_id > 0   GROUP BY  tvchannels_id, DATEPART(YEAR, startview_datetime),DATEPART(MONTH, startview_datetime),DATEPART(DAY, startview_datetime),devices_id )r1 group by r1.tvchannels_id order by r1.tvchannels_id";
     
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
        
   
    }else if(type == 'overview_daily_s3app_api'){
        
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        var start_datetime;
        var     end_datetime ;
        date_arr.forEach(element => {
            element.forEach(v => {

                 start_datetime = v[0];
                 end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when r1.d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });

        let subwhere = " (r1.tvchannels_id = 45 or r1.tvchannels_id = 39 or r1.tvchannels_id = 65 or r1.tvchannels_id =69 or r1.tvchannels_id =76 ) ";
        if(channels_id != null){
            subwhere = " r1.tvchannels_id= " + channels_id + " ";
        }
        $query    =  "SELECT r1.tvchannels_id , sum(count_allratingrecord) as total_devices,CONVERT(varchar(12),tvchannels_id) + '_'+'5' + '_' + '24' AS h_m "+
        select_condition+" FROM ( SELECT  devices_id,count(*) as count_allratingrecord ,tvchannels_id"+subquery_select_codition+" FROM "+rating_data_table+" WHERE startview_datetime >= '"+start_datetime+"' AND startview_datetime <= '"+end_datetime+"' AND tvchannels_id > 0   GROUP BY  tvchannels_id, DATEPART(YEAR, startview_datetime),DATEPART(MONTH, startview_datetime),DATEPART(DAY, startview_datetime),devices_id )r1 where "+ subwhere +"  group by r1.tvchannels_id order by r1.tvchannels_id";
     
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
        
   
    }
    else if(type == 'minute_s3app'){
        
            let subquery_select_codition = "";
            let select_condition         = "";
            var count = 0;
            tvprogram_recordset_obj[0].forEach(element => {
                
            
                var start_time =  set_starttime( element.start_time );
                var end_time   =  set_endtime( element.end_time );
                select_condition +=   ` ,Count(case when r1.d_${element.id} >= 1 then 1 end) as 'field_${element.id}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${date} ${start_time}' AND startview_datetime < '${date} ${end_time}' THEN 1 END ) AS 'd_${element.id}'`;
        
            });


            $query    =  "SELECT r1.tvchannels_id,r1.hour as hour ,r1.MINUTE as minute,(CONVERT(varchar(12),r1.hour) + '_' + CONVERT(varchar(12),r1.MINUTE)) as h_m  "+
            select_condition+" FROM ( SELECT  DATEPART(HOUR, startview_datetime) as hour,DATEPART(MINUTE, startview_datetime) as MINUTE,devices_id,count(*) as count_allratingrecord ,tvchannels_id"+subquery_select_codition+" FROM "+config_s3app.database+".dbo."+rating_data_table+" WHERE startview_datetime >= '"+date+" 00:00:01' AND startview_datetime <= '"+date+" 23:59:59' AND tvchannels_id > 0  AND tvchannels_id = "+channels_id+" GROUP BY  tvchannels_id, DATEPART(YEAR, startview_datetime),DATEPART(MONTH, startview_datetime),DATEPART(DAY, startview_datetime),DATEPART(HOUR, startview_datetime),DATEPART(minute , startview_datetime),devices_id )r1 group by r1.hour,r1.MINUTE,r1.tvchannels_id order by r1.hour , r1.MINUTE ";
         
            var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
            return query_obj;
            
       
    }
    else if(type == "channel"){
        condition_where = "";
        if(channels_id != null ){
            condition_where += " where id  = " + channels_id;
        }
        $query = `select * from ${config.database}.dbo.channels  ${condition_where}`;
    }
    else if(type =="tvprogram_basetime"){
        $query = `SELECT  channels_id
        ,devices_id
        ,COUNT(*) AS count_allratingrecord
        FROM rating_data_2022_2
        WHERE startview_datetime >= '2022-02-23 00:00:01'
        AND startview_datetime <= '2022-02-23 23:59:59'
        GROUP BY  channels_id
                ,devices_id`;
    }else if(type =="tvprogram_basetime_satalitereport"){
      
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        tvprogram_recordset_obj[0].forEach(element => {
            
           
            var start_time =  set_starttime( element.start_time );
            var end_time   =  set_endtime( element.end_time );
            select_condition +=   ` ,Count(case when d_${element.id} >= 1 then 1 end) as 'field_${element.id}'`;    
            subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${date} ${start_time}' AND startview_datetime < '${date} ${end_time}' THEN 1 END ) AS 'd_${element.id}'`;
     
        });


        $query    =  `SELECT  channels_id,devices_id${subquery_select_codition} FROM ${rating_data_table} WHERE startview_datetime >= '${date} 00:00:01' AND startview_datetime <= '${date} 23:59:59' AND channels_id > 0  GROUP BY  channels_id,devices_id`;
     
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;


    }else if(type =="overview_tvprogram_basetime_satalitereport"){
      
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });

        let $start_datetimestring_condition =   `${date} 00:00:01`;
        if(start_datetime_string != null){
            $start_datetimestring_condition = start_datetime_string;
        } 
        let $end_datetimestring_condition   = `${date} 23:59:59`;
        if(end_datetime_string != null){
            $end_datetimestring_condition = end_datetime_string;
        } 
        

        $query    =  `SELECT  channels_id,devices_id${subquery_select_codition} FROM ${rating_data_table} WHERE startview_datetime >= '${$start_datetimestring_condition}' AND startview_datetime <= '${$end_datetimestring_condition}' AND channels_id > 0  GROUP BY  channels_id,devices_id`;
     
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;


    }else if(type =="overview_tvprogram_basetime_satalitereport_statisticapi"){
      
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        var start_datetime;
        var end_datetime;
        date_arr.forEach(element => {
            element.forEach(v => {

                start_datetime = v[0];
                end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });

        //(channels_id = 257 or channels_id = 263 or channels_id = 271  or channels_id = 273 or channels_id = 277)
        let subwhere = " (channels_id = 257 or channels_id = 263 or channels_id = 271  or channels_id = 273 or channels_id = 277) ";
        if(channels_id != null){
           subwhere = " channels_id="+channels_id+" ";
        }


        $query    =  `SELECT  channels_id,devices_id${subquery_select_codition} FROM ${rating_data_table} WHERE startview_datetime >= '${start_datetime}' AND startview_datetime <= '${end_datetime}' AND channels_id > 0  and ${subwhere}  GROUP BY  channels_id,devices_id`;
     
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;


    }else if(type =="overview_basetime_satalitereport"){
        let subquery_select_codition = "";
        let select_period_cond       = "";
        let select_condition         = ` ,COUNT(case WHEN overview >= 1 THEN 1 end) AS 'overview' `;    
        var count = 0;
        tvprogram_recordset_obj[0].forEach(element => {
            
           
            var start_time =  set_starttime( element.start_time );
            var end_time   =  set_endtime( element.end_time );
           
            if(select_period_cond.length > 0){
                select_period_cond += " or ";
            }
            select_period_cond+=  ` (startview_datetime > '${date} ${start_time}' AND startview_datetime < '${date} ${end_time}') `;
           
     
        });
        subquery_select_codition += `,COUNT(CASE WHEN ${select_period_cond} THEN 1 END ) AS 'overview'`;

        $query    =  `SELECT  channels_id,devices_id${subquery_select_codition} FROM ${rating_data_table} WHERE startview_datetime >= '${date} 00:00:01' AND startview_datetime <= '${date} 23:59:59' AND channels_id > 0  GROUP BY  channels_id,devices_id`;
     
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
        
    }else if(type =="overview_basetime_iptvreport"){
        let subquery_select_codition = "";
        let select_period_cond       = "";
        let select_condition         = ` ,COUNT(case WHEN overview >= 1 THEN 1 end) AS 'overview' `;    
        var count = 0;
        tvprogram_recordset_obj[0].forEach(element => {
            
           
            var start_time =  set_starttime( element.start_time );
            var end_time   =  set_endtime( element.end_time );
           
            if(select_period_cond.length > 0){
                select_period_cond += " or ";
            }
            select_period_cond+=  ` (startview_datetime > '${date} ${start_time}' AND startview_datetime < '${date} ${end_time}') `;
           
     
        });
        subquery_select_codition += `,COUNT(CASE WHEN ${select_period_cond} THEN 1 END ) AS 'overview'`;

        $query    =  `SELECT  tvchannels_id,devices_id${subquery_select_codition} FROM ${config_s3app.database}.dbo.${rating_data_table} WHERE startview_datetime >= '${date} 00:00:01' AND startview_datetime <= '${date} 23:59:59' AND tvchannels_id > 0  GROUP BY  tvchannels_id,devices_id`;
      
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
        
    }else if(type =="tvprogram_basetime_iptvreport_s3app"){
     
        // $20channel_where  = list20tvchannel_ondemand("iptv");
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        tvprogram_recordset_obj[0].forEach(element => {
        
            var start_time =  set_starttime( element.start_time );
            var end_time   =  set_endtime( element.end_time );
            select_condition +=   ` ,Count(case when d_${element.id} >= 1 then 1 end) as 'field_${element.id}'`;    
            subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${date} ${start_time}' AND startview_datetime < '${date} ${end_time}' THEN 1 END ) AS 'd_${element.id}'`;
     
        });

        // AND (${$20channel_where})
        $query    =  `SELECT  tvchannels_id,devices_id${subquery_select_codition} FROM ${config_s3app.database}.dbo.${rating_data_table} WHERE startview_datetime >= '${date} 00:00:01' AND startview_datetime <= '${date} 23:59:59' AND tvchannels_id > 0  GROUP BY  tvchannels_id,devices_id`;
   
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;


    }else if(type =="overview_tvprogram_basetime_iptvreport_s3app"){
     
        // $20channel_where  = list20tvchannel_ondemand("iptv");
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });

        let $start_datetimestring_condition =   `${date} 00:00:01`;
        if(start_datetime_string != null){
            $start_datetimestring_condition = start_datetime_string;
        } 
        let $end_datetimestring_condition   = `${date} 23:59:59`;
        if(end_datetime_string != null){
            $end_datetimestring_condition = end_datetime_string;
        } 

        $from_table=`${config_s3app.database}.dbo.${rating_data_table}`;
        if(condition_obj != null){
            if(condition_obj.view_table != null){
                $from_table = condition_obj.view_table;
            }
        }

        // AND (${$20channel_where})
        $query    =  `SELECT  tvchannels_id,devices_id${subquery_select_codition} FROM ${$from_table} WHERE startview_datetime >= '${$start_datetimestring_condition}' AND startview_datetime <= '${$end_datetimestring_condition}' AND tvchannels_id > 0  GROUP BY  tvchannels_id,devices_id`;
   
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;


    }else if(type =="overview_tvprogram_basetime_iptvreport_s3app_api"){
     
        // $20channel_where  = list20tvchannel_ondemand("iptv");
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });

        let temp_table_iptv = "";
        if(condition_obj != null){
            temp_table_iptv = condition_obj.temp_data_iptv;
        }else{
            temp_table_iptv = `${config_s3app.database}.dbo.${rating_data_table}`;
        }

        // AND (${$20channel_where})
        let subwhere = " (tvchannels_id = 45 or tvchannels_id = 39 or tvchannels_id = 65 or tvchannels_id =69 or tvchannels_id =76 ) ";
        if(channels_id != null){
        subwhere = " tvchannels_id="+channels_id+" ";
        }
        $query    =  `SELECT  tvchannels_id,devices_id${subquery_select_codition} FROM ${config_s3app.database}.dbo.${rating_data_table} WHERE startview_datetime >= '${date} 00:00:01' AND startview_datetime <= '${date} 23:59:59' AND tvchannels_id > 0  and ${subwhere}  GROUP BY  tvchannels_id,devices_id`;
        //console.log($query);return;
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;


    }else if(type =="overview_youtubeviewreport_satalite"){
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${start_datetime}' AND startview_datetime < '${end_datetime}' THEN 1 END ) AS 'd_${h_m}'`;
            });
        });
        $query    =  `SELECT  devices_id${subquery_select_codition} FROM ${youtube_data_table} WHERE startview_datetime >= '${date} 00:00:01' AND startview_datetime <= '${date} 23:59:59' GROUP BY devices_id`;
        //console.log($query);
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
    }
    else if(type =="youtubeviewreport_satalite"){
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        tvprogram_recordset_obj[0].forEach(element => {
            
           
            var start_time =  set_starttime( element.start_time );
            var end_time   =  set_endtime( element.end_time );
            select_condition +=   ` ,Count(case when d_${element.id} >= 1 then 1 end) as 'field_${element.id}'`;    
            subquery_select_codition += `,COUNT(CASE WHEN startview_datetime > '${date} ${start_time}' AND startview_datetime < '${date} ${end_time}' THEN 1 END ) AS 'd_${element.id}'`;
     
        });


        $query    =  `SELECT  devices_id${subquery_select_codition} FROM ${youtube_data_table} WHERE startview_datetime >= '${date} 00:00:01' AND startview_datetime <= '${date} 23:59:59' GROUP BY devices_id`;

        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
    }else if(type =="get_channeldailyrating"){
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        $query    =  `select channel_daily_rating.id,channel_daily_rating.channels_id , channel_daily_rating_logs.rating
        , channel_daily_rating_logs.reach_devices , CONVERT(varchar(12),channel_daily_rating.channels_id,channel_daily_rating.channels_id)
        + '_'+ CONVERT(varchar(12),DATEPART(HOUR, channel_daily_rating_logs.created))
        + '_' + CONVERT(varchar(12),DATEPART(MINUTE, channel_daily_rating_logs.created)) as h_m
        ,channel_daily_rating_logs.created from channel_daily_rating 
        inner join channel_daily_rating_logs on  channel_daily_rating.id = channel_daily_rating_logs.channel_daily_rating_id
        where channel_daily_rating.date  = '${date}'   and ( channel_daily_rating_logs.created  <= '${date} 01:00:00'  or channel_daily_rating_logs.created  >= '${date} 05:00:00'  ) and (${$20channel_where}) `;

        var query_obj = { 'temptable': '' , 'rawquery' : $query ,  'select_condition' : select_condition}
        return query_obj;
    }else if(type =="get_channeldailyrating_dailyreport_psitotal"){
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        tvprogram_recordset_obj[0].forEach(element => {
                  
            var start_time =  set_starttime( element.start_time );
            var end_time   =  set_endtime( element.end_time );
            subquery_select_codition += `,SUM(CASE WHEN  r1.created >=  '${date} ${start_time}' AND r1.created <= '${date} ${end_time}' THEN r1.reach_devices Else 0 End  ) AS 'field_${element.id}'`;   
     
        });
        $query    =  `SELECT r1.date ${subquery_select_codition}
        FROM (
            SELECT  "channel_daily_rating_logs".*,"channel_daily_rating".date
            FROM "channel_daily_rating_logs"
            JOIN "channel_daily_rating"
            ON "channel_daily_rating_logs"."channel_daily_rating_id" = "channel_daily_rating"."id"
            where channel_daily_rating.date  = '${date}'
        
        )r1
        group by r1.date
        `;
    
        // and (${$20channel_where})

        var query_obj = { 'temptable': '' , 'rawquery' : $query ,  'select_condition' : select_condition}
        return query_obj;
    }
    else if(type =="get_channeldailyrating_dailyreport_othertv"){
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        tvprogram_recordset_obj[0].forEach(element => {
                  
            var start_time =  set_starttime( element.start_time );
            var end_time   =  set_endtime( element.end_time );
            subquery_select_codition += `,SUM(CASE WHEN  r1.created >=  '${date} ${start_time}' AND r1.created <= '${date} ${end_time}' THEN r1.reach_devices Else 0 End  ) AS 'field_${element.id}'`;   
     
        });
        $query    =  `SELECT r1.date ${subquery_select_codition}
        FROM (
            SELECT  "channel_daily_rating_logs".*,"channel_daily_rating".date
            FROM "channel_daily_rating_logs"
            JOIN "channel_daily_rating"
            ON "channel_daily_rating_logs"."channel_daily_rating_id" = "channel_daily_rating"."id"
            where channel_daily_rating.date  = '${date}'  and (${$20channel_where_notin})
        
        )r1
        group by r1.date
        `;
        // and (${$20channel_where})

        var query_obj = { 'temptable': '' , 'rawquery' : $query ,  'select_condition' : select_condition}
        return query_obj;
    }
    else if(type =="get_channeldailyrating_dailyreport"){
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        $query    =  `select channel_daily_rating.id,channel_daily_rating.channels_id , channel_daily_rating_logs.rating
        , channel_daily_rating_logs.reach_devices , CONVERT(varchar(12),channel_daily_rating.channels_id,channel_daily_rating.channels_id)
        + '_'+ CONVERT(varchar(12),DATEPART(HOUR, channel_daily_rating_logs.created))
        + '_' + CONVERT(varchar(12),DATEPART(MINUTE, channel_daily_rating_logs.created)) as h_m
        ,channel_daily_rating_logs.created from channel_daily_rating 
        inner join channel_daily_rating_logs on  channel_daily_rating.id = channel_daily_rating_logs.channel_daily_rating_id
        where channel_daily_rating.date  = '${date}'  `;
        // and (${$20channel_where})

        var query_obj = { 'temptable': '' , 'rawquery' : $query ,  'select_condition' : select_condition}
        return query_obj;
    }
    else if(type =="get_channeldailyrating_overview"){
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        /** ============= case : requiremnet ลูกค้าให้หาค่าเฉลี่ยสะสม 10 นาทีทุกช่วงเวลา =============================  */
        $query    =  `select channels_id , AVG(r1.reach_devices) as reach_devices , CONVERT(varchar(12),channels_id) + '_' + '5' + '_' + '0'  as h_m FROM ( select channel_daily_rating.id,channel_daily_rating.channels_id , channel_daily_rating_logs.rating
        , channel_daily_rating_logs.reach_devices , CONVERT(varchar(12),channel_daily_rating.channels_id,channel_daily_rating.channels_id)
        + '_'+ CONVERT(varchar(12),DATEPART(HOUR, channel_daily_rating_logs.created))
        + '_' + CONVERT(varchar(12),DATEPART(MINUTE, channel_daily_rating_logs.created)) as h_m
        ,channel_daily_rating_logs.created from channel_daily_rating 
        inner join channel_daily_rating_logs on  channel_daily_rating.id = channel_daily_rating_logs.channel_daily_rating_id
        where channel_daily_rating.date  = '${date}'   and ( channel_daily_rating_logs.created  >= '${date} 05:00:00'  ) and (${$20channel_where}) )r1 	group by r1.channels_id  `;

        var query_obj = { 'temptable': '' , 'rawquery' : $query ,  'select_condition' : select_condition}
        return query_obj;
    }else if(type =="get_provinces"){
        let select_condition = "";
        let $query =  "select * from provinces ";
        var query_obj = { 'temptable': '' , 'rawquery' : $query ,  'select_condition' : select_condition};
        return query_obj;

    }else if(type == "overview_groupby_gender_satalite"){
        let subquery_select_codition = "";
        var count = 0;
        // let select_condition         = ` ,COUNT(case WHEN overview >= 1 THEN 1 end) AS 'overview' `;   
        let select_condition        = '';    
        var count = 0;
        let select_period_cond = "";

        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
           
            });
        });


     
     

        $query = "select     (CASE WHEN #temp_gender.gender IS NULL  THEN 'none' WHEN #temp_gender.gender = ''  THEN 'none1'  WHEN #temp_gender.gender = '-'  THEN 'none2'   ELSE #temp_gender.gender END)  as gender, merge_s3remote_id"+select_condition+",channels_id, count(*) as total_row from "+temp_table+" INNER JOIN channels  ON "+temp_table+".channels_id = channels.id left join #temp_gender on "+temp_table+".devices_id = #temp_gender.dvid where channels.active = 1 group by gender  , channels_id ,channels.merge_s3remote_id order by channels_id asc , #temp_gender.gender asc" ;
        // console.log($query);
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }else if(type == "overview_groupby_gender_satalite_statisticapi"){
        let subquery_select_codition = "";
        var count = 0;
        // let select_condition         = ` ,COUNT(case WHEN overview >= 1 THEN 1 end) AS 'overview' `;   
        let select_condition        = '';    
        var count = 0;
        let select_period_cond = "";

        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
           
            });
        });


        let subwhere = " (channels_id = 257 or channels_id = 263 or channels_id = 271  or channels_id = 273 or channels_id = 277) ";
        if(channels_id != null){
            subwhere = " channels_id="+channels_id + " ";
        }
        
        let table_ = "";
        if(rating_data_table != null){
            table_ = rating_data_table;
        }else{
            table_ = "#temp_gender";
        }

        $query = `select     (CASE WHEN ${table_}.gender IS NULL  THEN 'none' WHEN ${table_}.gender = ''  THEN 'none1'  WHEN ${table_}.gender = '-'  THEN 'none2'   ELSE ${table_}.gender END)  as gender, merge_s3remote_id${select_condition},channels_id, count(*) as total_row from ${temp_table} INNER JOIN channels  ON ${temp_table}.channels_id = channels.id left join ${table_} on ${temp_table}.devices_id = ${table_}.dvid where channels.active = 1 and ${subwhere} group by gender  , channels_id ,channels.merge_s3remote_id order by channels_id asc , ${table_}.gender asc` ;
        //console.log($query);
        // console.log($query);
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }else if(type == "overview_groupby_age_satalite"){
        let subquery_select_codition = "";
        var count = 0;
        // let select_condition         = ` ,COUNT(case WHEN overview >= 1 THEN 1 end) AS 'overview' `;   
        let select_condition        = '';    
        var count = 0;
        let select_period_cond = "";

        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
           
            });
        });


        let subwhere = " (channels_id = 257 or channels_id = 263 or channels_id = 271  or channels_id = 273 or channels_id = 277) ";
        if(channels_id != null){
            subwhere = " channels_id="+channels_id+" ";
        }
        let temp_gender_table = "";
        if(condition_obj != null){
            temp_gender_table = condition_obj.temp_gender;
        }else{
            temp_gender_table = "#temp_gender";
        }

        $query = "select     (CASE WHEN "+temp_gender_table+".age = 0 THEN 0 WHEN "+temp_gender_table+".age IS NULL  THEN 10000 WHEN "+temp_gender_table+".age = ''  THEN 10001  WHEN "+temp_gender_table+".age = '-'  THEN 10002   ELSE "+temp_gender_table+".age END)  as age, merge_s3remote_id"+select_condition+",channels_id, count(*) as total_row from "+temp_table+" INNER JOIN channels  ON "+temp_table+".channels_id = channels.id left join "+temp_gender_table+" on "+temp_table+".devices_id = "+temp_gender_table+".dvid where channels.active = 1 and "+subwhere+" group by age  , channels_id ,channels.merge_s3remote_id order by channels_id asc , "+temp_gender_table+".age asc" ;
      
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }else if(type == "overview_groupby_gender_youtube"){
        let subquery_select_codition = "";
        var count = 0;
        // let select_condition         = ` ,COUNT(case WHEN overview >= 1 THEN 1 end) AS 'overview' `;   
        let select_condition        = '';    
        var count = 0;
        let select_period_cond = "";

        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN d_${h_m} > 0  THEN 1 END )  `;
            });
        });


     
      //  subquery_select_codition += `,COUNT(CASE WHEN ${select_period_cond} THEN 1 END ) AS 'overview'`;

        $query = "select     (CASE WHEN #temp_gender.gender IS NULL  THEN 'none' WHEN #temp_gender.gender = ''  THEN 'none1'  WHEN #temp_gender.gender = '-'  THEN 'none2'   ELSE #temp_gender.gender END)  as gender "+select_condition+", count(*) as total_row from "+temp_table+"  left join #temp_gender on "+temp_table+".devices_id = #temp_gender.dvid group by gender   , #temp_gender.gender " ;
        //console.log($query);
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }
    else if(type == "groupby_gender_satalite"){
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        tvprogram_recordset_obj[0].forEach(element => {
            select_condition +=   ` ,Count(case when d_${element.id} >= 1 then 1 end) as 'field_${element.id}'`;    
        });
        $where =  "";
        if(channels_id != null){
            $where += " where channels_id = " + channels_id +" ";
        }
        $query = "select     (CASE WHEN #temp_gender.gender IS NULL  THEN 'none' WHEN #temp_gender.gender = ''  THEN 'none1'  WHEN #temp_gender.gender = '-'  THEN 'none2'   ELSE #temp_gender.gender END)  as gender, merge_s3remote_id"+select_condition+",channels_id, count(*) as total_row from "+temp_table+" INNER JOIN channels  ON #temp_data.channels_id = channels.id left join #temp_gender on "+temp_table+".devices_id = #temp_gender.dvid  " + $where + " group by  gender  , channels_id ,channels.merge_s3remote_id order by channels_id asc , #temp_gender.gender asc" ;

        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }else if(type == "overview_groupby_gender_iptv"){
        let subquery_select_codition = "";
        var count = 0; 
        let select_condition        = '';    
        let select_period_cond = "";

        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN d_${h_m} > 0  THEN 1 END )  `;
            });
        });
        // $where =  "";
        // if(channels_id != null){
        //     $where += " where tvchannels_id = " + channels_id +" ";
        // }

        $query = "select   (CASE WHEN #temp_gender.gender IS NULL  THEN 'none' WHEN #temp_gender.gender = ''  THEN 'none1'  WHEN #temp_gender.gender = '-'  THEN 'none2'   ELSE #temp_gender.gender END)  as gender"+select_condition+",tvchannels_id, count(*) as total_row from "+temp_table+" left join #temp_gender on "+temp_table+".devices_id = #temp_gender.dvid  group by  gender  , tvchannels_id order by tvchannels_id asc , #temp_gender.gender asc" ;
        
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }else if(type == "overview_groupby_gender_iptv_api"){
        let subquery_select_codition = "";
        var count = 0; 
        let select_condition        = '';    
        let select_period_cond = "";

        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN d_${h_m} > 0  THEN 1 END )  `;
            });
        });
        // $where =  "";
        // if(channels_id != null){
        //     $where += " where tvchannels_id = " + channels_id +" ";
        // }

        let temp_gender_table = "";
        if(condition_obj != null){
            temp_gender_table = condition_obj.temp_gender;
        }else{
            temp_gender_table = "#temp_gender";
        }
        $query = "select   (CASE WHEN "+temp_gender_table+".gender IS NULL  THEN 'none' WHEN "+temp_gender_table+".gender = ''  THEN 'none1'  WHEN "+temp_gender_table+".gender = '-'  THEN 'none2'   ELSE "+temp_gender_table+".gender END)  as gender"+select_condition+",tvchannels_id, count(*) as total_row from "+temp_table+" left join "+temp_gender_table+" on "+temp_table+".devices_id = "+temp_gender_table+".dvid  group by  gender  , tvchannels_id order by tvchannels_id asc , "+temp_gender_table+".gender asc" ;
        
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }else if(type == "overview_groupby_age_iptv"){
        let subquery_select_codition = "";
        var count = 0; 
        let select_condition        = '';    
        let select_period_cond = "";

        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN d_${h_m} > 0  THEN 1 END )  `;
            });
        });
        // $where =  "";
        // if(channels_id != null){
        //     $where += " where tvchannels_id = " + channels_id +" ";
        // }
        let temp_gender_table = "";
        if(condition_obj != null){
            temp_gender_table = condition_obj.temp_gender;
        }else{
            temp_gender_table = "#temp_gender";
        }
        $query = "select   (CASE WHEN "+temp_gender_table+".age = 0 THEN 0 WHEN "+temp_gender_table+".age IS NULL  THEN 10000 WHEN "+temp_gender_table+".age = ''  THEN 10001  WHEN "+temp_gender_table+".age = '-'  THEN 10002   ELSE "+temp_gender_table+".age END)  as age"+select_condition+",tvchannels_id, count(*) as total_row from "+temp_table+" left join "+temp_gender_table+" on "+temp_table+".devices_id = "+temp_gender_table+".dvid  group by  age  , tvchannels_id order by tvchannels_id asc , "+temp_gender_table+".age asc" ;
        
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }
    
    else if(type == "groupby_gender_iptv"){
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        tvprogram_recordset_obj[0].forEach(element => {
            select_condition +=   ` ,Count(case when d_${element.id} >= 1 then 1 end) as 'field_${element.id}'`;    
        });
        $where =  "";
        if(channels_id != null){
            $where += " where tvchannels_id = " + channels_id +" ";
        }
        $query = "select   (CASE WHEN #temp_gender.gender IS NULL  THEN 'none' WHEN #temp_gender.gender = ''  THEN 'none1'  WHEN #temp_gender.gender = '-'  THEN 'none2'   ELSE #temp_gender.gender END)  as gender"+select_condition+",tvchannels_id, count(*) as total_row from "+temp_table+" left join #temp_gender on "+temp_table+".devices_id = #temp_gender.dvid "+$where+" group by  gender  , tvchannels_id order by tvchannels_id asc , #temp_gender.gender asc" ;
        
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;

    }
    else if(type == "overview_groupby_addr_satalite"){
       let subquery_select_codition = "";
        var count = 0;
        // let select_condition         = ` ,COUNT(case WHEN overview >= 1 THEN 1 end) AS 'overview' `;  
        let select_condition         =  '';    
        var count = 0;
        let select_period_cond = "";
        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                 subquery_select_codition += `,COUNT(CASE WHEN d_${h_m} >= 1  THEN 1 END )  as field_${h_m} `;
            });
        });


       //  subquery_select_codition += `,COUNT(CASE WHEN ${select_period_cond} THEN 1 END ) AS 'overview'`;

        $query = " select merge_s3remote_id,channels_id,(CASE WHEN province_region IS NULL  THEN 'none' WHEN province_region = ''  THEN 'none1'  WHEN province_region = '-'  THEN 'none2'   ELSE province_region END)  as province_region " + subquery_select_codition + " from "+temp_table+" left join device_addresses on "+temp_table+".devices_id = device_addresses.devices_id INNER JOIN channels  ON "+temp_table+".channels_id = channels.id  where channels.active = 1 group by channels_id,province_region,"+temp_table+".channels_id , merge_s3remote_id"
      
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
    }else if(type == "overview_groupby_addr_satalite_api"){
        let subquery_select_codition = "";
         var count = 0;
         // let select_condition         = ` ,COUNT(case WHEN overview >= 1 THEN 1 end) AS 'overview' `;  
         let select_condition         =  '';    
         var count = 0;
         let select_period_cond = "";
         let date_arr = tvprogram_recordset_obj;
         date_arr.forEach(element => {
             element.forEach(v => {
 
                 var start_datetime = v[0];
                 var end_datetime   = v[1];
                 var h_m        = v[2];
                 select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                  subquery_select_codition += `,COUNT(CASE WHEN d_${h_m} >= 1  THEN 1 END )  as field_${h_m} `;
             });
         });
 
 
        //  subquery_select_codition += `,COUNT(CASE WHEN ${select_period_cond} THEN 1 END ) AS 'overview'`;
         let subwhere = " (channels_id = 257 or channels_id = 263 or channels_id = 271  or channels_id = 273 or channels_id = 277) ";
         if(channels_id != null){
            subwhere = " channels_id="+channels_id+" ";
         }
         $query = " select merge_s3remote_id,channels_id,(CASE WHEN province_region IS NULL  THEN 'none' WHEN province_region = ''  THEN 'none1'  WHEN province_region = '-'  THEN 'none2'   ELSE province_region END)  as province_region " + subquery_select_codition + " from "+temp_table+" left join device_addresses on "+temp_table+".devices_id = device_addresses.devices_id INNER JOIN channels  ON "+temp_table+".channels_id = channels.id  where channels.active = 1  and "+subwhere+" group by channels_id,province_region,"+temp_table+".channels_id , merge_s3remote_id"
       
         var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
         return query_obj;
     }else if(type == "overview_groupby_provinceaddr_satalite"){
        let subquery_select_codition = "";
         var count = 0;
         // let select_condition         = ` ,COUNT(case WHEN overview >= 1 THEN 1 end) AS 'overview' `;  
         let select_condition         =  '';    
         var count = 0;
         let select_period_cond = "";
         let date_arr = tvprogram_recordset_obj;
         date_arr.forEach(element => {
             element.forEach(v => {
 
                 var start_datetime = v[0];
                 var end_datetime   = v[1];
                 var h_m        = v[2];
                 select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                  subquery_select_codition += `,COUNT(CASE WHEN d_${h_m} >= 1  THEN 1 END )  as field_${h_m} `;
             });
         });
 
 
        //  subquery_select_codition += `,COUNT(CASE WHEN ${select_period_cond} THEN 1 END ) AS 'overview'`;
 
         $query = " select merge_s3remote_id,channels_id,region_code,provinces.province_name " + subquery_select_codition + " from "+temp_table+" left join device_addresses on "+temp_table+".devices_id = device_addresses.devices_id INNER JOIN channels  ON "+temp_table+".channels_id = channels.id  INNER JOIN provinces on  provinces.province_code = device_addresses.region_code  where channels.active = 1 and device_addresses.province_region IS NOT NULL and device_addresses.province_region  != '' and  device_addresses.province_region  != '-'  and region_code != '' and region_code is not null   and province_name is not null  and region_code > 0 group by channels_id,region_code , provinces.province_name,"+temp_table+".channels_id , merge_s3remote_id"
       
         var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
         return query_obj;
     }else if(type == "overview_groupby_provinceaddr_satalite_api"){
        let subquery_select_codition = "";
         var count = 0;
         // let select_condition         = ` ,COUNT(case WHEN overview >= 1 THEN 1 end) AS 'overview' `;  
         let select_condition         =  '';    
         var count = 0;
         let select_period_cond = "";
         let date_arr = tvprogram_recordset_obj;
         date_arr.forEach(element => {
             element.forEach(v => {
 
                 var start_datetime = v[0];
                 var end_datetime   = v[1];
                 var h_m        = v[2];
                 select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                  subquery_select_codition += `,COUNT(CASE WHEN d_${h_m} >= 1  THEN 1 END )  as field_${h_m} `;
             });
         });
 
         let subwhere = " (channels_id = 257 or channels_id = 263 or channels_id = 271  or channels_id = 273 or channels_id = 277) ";
         if(channels_id != null){
            subwhere = " channels_id="+channels_id+" ";
         }
        //  subquery_select_codition += `,COUNT(CASE WHEN ${select_period_cond} THEN 1 END ) AS 'overview'`;
 
         $query = " select merge_s3remote_id,channels_id,region_code,provinces.province_name " + subquery_select_codition + " from "+temp_table+" left join device_addresses on "+temp_table+".devices_id = device_addresses.devices_id INNER JOIN channels  ON "+temp_table+".channels_id = channels.id  INNER JOIN provinces on  provinces.province_code = device_addresses.region_code  where channels.active = 1  and "+subwhere+" and device_addresses.province_region IS NOT NULL and device_addresses.province_region  != '' and  device_addresses.province_region  != '-'  and region_code != '' and region_code is not null   and province_name is not null  and region_code > 0 group by channels_id,region_code , provinces.province_name,"+temp_table+".channels_id , merge_s3remote_id"
       
         var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
         return query_obj;
     }else if(type == "overview_groupby_addr_youtube"){
        let subquery_select_codition = "";
         var count = 0;
         // let select_condition         = ` ,COUNT(case WHEN overview >= 1 THEN 1 end) AS 'overview' `;  
         let select_condition         =  '';    
         var count = 0;
         let select_period_cond = "";
         let date_arr = tvprogram_recordset_obj;
         date_arr.forEach(element => {
             element.forEach(v => {
 
                 var start_datetime = v[0];
                 var end_datetime   = v[1];
                 var h_m        = v[2];
                 select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                  subquery_select_codition += `,COUNT(CASE WHEN d_${h_m} >= 1  THEN 1 END )  as field_${h_m} `;
             });
         });
 
 
        //  subquery_select_codition += `,COUNT(CASE WHEN ${select_period_cond} THEN 1 END ) AS 'overview'`;
 
         $query = " select (CASE WHEN province_region IS NULL  THEN 'none' WHEN province_region = ''  THEN 'none1'  WHEN province_region = '-'  THEN 'none2'   ELSE province_region END)  as province_region " + subquery_select_codition + " from "+temp_table+" left join device_addresses on "+temp_table+".devices_id = device_addresses.devices_id  group by province_region"
       
         var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
         return query_obj;
     }
     else if(type == "overview_groupby_provinceaddr_youtube"){
        let subquery_select_codition = "";
         var count = 0;
         // let select_condition         = ` ,COUNT(case WHEN overview >= 1 THEN 1 end) AS 'overview' `;  
         let select_condition         =  '';    
         var count = 0;
         let select_period_cond = "";
         let date_arr = tvprogram_recordset_obj;
         
         date_arr.forEach(element => {
             element.forEach(v => {
              
                 var start_datetime = v[0];
                 var end_datetime   = v[1];
                 var h_m        = v[2];
                 select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                 
                //  if(count != 0){
                //     subquery_select_codition += ",";
                //  }
                 subquery_select_codition += `,COUNT(CASE WHEN d_${h_m} >= 1  THEN 1 END )  as field_${h_m} `;
                 //++ count;
             });
         });
 
 
        //  subquery_select_codition += `,COUNT(CASE WHEN ${select_period_cond} THEN 1 END ) AS 'overview'`;
 
         $query = " select  region_code,provinces.province_name" + subquery_select_codition + " from "+temp_table+" left join device_addresses on "+temp_table+".devices_id = device_addresses.devices_id  INNER JOIN provinces on  provinces.province_code = device_addresses.region_code and device_addresses.province_region IS NOT NULL and device_addresses.province_region  != '' and  device_addresses.province_region  != '-'  and region_code != '' and region_code is not null   and province_name is not null  and region_code > 0 group by  provinces.province_name,region_code ";
       
         var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
         return query_obj;
     }
    else if(type == "groupby_addr_satalite"){
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        tvprogram_recordset_obj[0].forEach(element => {
            select_condition +=   ` ,Count(case when d_${element.id} >= 1 then 1 end) as 'field_${element.id}'`;    
        });
             
        $where =  "";
        if(channels_id != null){
            $where +=" where "+temp_table+".channels_id = " + channels_id + " ";
        }

        $query = " select merge_s3remote_id,channels_id,(CASE WHEN province_region IS NULL  THEN 'none' WHEN province_region = ''  THEN 'none1'  WHEN province_region = '-'  THEN 'none2'   ELSE province_region END)  as province_region " + select_condition + " from "+temp_table+" left join device_addresses on "+temp_table+".devices_id = device_addresses.devices_id INNER JOIN channels  ON "+temp_table+".channels_id = channels.id "+$where+" group by province_region,"+temp_table+".channels_id , merge_s3remote_id"
      
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
    }
    else if(type == "overview_groupby_addr_iptv"){
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0; 
        let select_period_cond = "";

        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN d_${h_m} > 0  THEN 1 END )  `;
            });
        });
        // $where =  "";
        // if(channels_id != null){
        //     $where = "where "+temp_table+".tvchannels_id = " + channels_id + " ";
        // }

        $query = "select tvchannels_id,(CASE WHEN province_region IS NULL  THEN 'none' WHEN province_region = ''  THEN 'none1'  WHEN province_region = '-'  THEN 'none2'   ELSE province_region END)  as province_region" + select_condition + " from "+temp_table+" left join device_addresses on "+temp_table+".devices_id = device_addresses.devices_id   group by province_region,"+temp_table+".tvchannels_id"
       
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
    }
    else if(type == "overview_groupby_provinceaddr_iptv"){
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0; 
        let select_period_cond = "";

        let date_arr = tvprogram_recordset_obj;
        date_arr.forEach(element => {
            element.forEach(v => {

                var start_datetime = v[0];
                var end_datetime   = v[1];
                var h_m        = v[2];
                select_condition +=   ` ,Count(case when d_${h_m} >= 1 then 1 end) as 'field_${h_m}'`;    
                subquery_select_codition += `,COUNT(CASE WHEN d_${h_m} > 0  THEN 1 END )  `;
            });
        });

        $query = "select tvchannels_id,region_code,provinces.province_name " + select_condition + " from "+temp_table+" left join device_addresses on "+temp_table+".devices_id = device_addresses.devices_id INNER JOIN provinces on  provinces.province_code = device_addresses.region_code and device_addresses.province_region IS NOT NULL and device_addresses.province_region  != '' and  device_addresses.province_region  != '-'  and region_code != '' and region_code is not null   and province_name is not null  and region_code > 0   group by  "+temp_table+".tvchannels_id,provinces.province_name,region_code"
       
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
    }
    else if(type == "groupby_addr_iptv"){
        let subquery_select_codition = "";
        let select_condition         = "";
        var count = 0;
        tvprogram_recordset_obj[0].forEach(element => {
            select_condition +=   ` ,Count(case when d_${element.id} >= 1 then 1 end) as 'field_${element.id}'`;    
        });
        $where =  "";
        if(channels_id != null){
            $where = "where "+temp_table+".tvchannels_id = " + channels_id + " ";
        }

        $query = "select tvchannels_id,(CASE WHEN province_region IS NULL  THEN 'none' WHEN province_region = ''  THEN 'none1'  WHEN province_region = '-'  THEN 'none2'   ELSE province_region END)  as province_region" + select_condition + " from "+temp_table+" left join device_addresses on "+temp_table+".devices_id = device_addresses.devices_id  "+$where+" group by province_region,"+temp_table+".tvchannels_id"
       
        var query_obj = { 'temptable': $query , 'select_condition' : select_condition}
        return query_obj;
    }
    return $query;

}
function set_starttime(start_time){
    var start =set_timezone(start_time, "Asia/Jakarta");
    var st_h  = start.getUTCHours();
    st_h =  st_h < 10 ? "0"+st_h : st_h;
    var st_m  = start.getUTCMinutes();
    st_m =  st_m < 10 ? "0"+st_m : st_m;
    var st_c  = start.getUTCSeconds();
    st_c =  st_c < 10 ? "0"+st_c : st_c;

    return st_h + ":" + st_m + ":" + st_c;
}
function set_starttime_notconvert(str){
  //  start = moment.tz(startString + ' 05:00', 'Asia/Jakarta');
    const m = moment.tz(str , 'Asia/Jakarta');
    return m.format("HH:mm:ss");
}
function set_endtime_notconvert(str){
    const m = moment.tz(str , 'Asia/Jakarta');
    return m.format("HH:mm:ss");
}
function set_endtime(end_time , type = null){
    var end =set_timezone(end_time, "Asia/Jakarta");
    var end_h  = end.getUTCHours();
    end_h =  end_h < 10 ? "0"+end_h : end_h;
    var end_m  = end.getUTCMinutes();
    end_m =  end_m < 10 ? "0"+end_m : end_m;
    var end_s = end.getUTCSeconds();
    end_s =  end_s < 10 ? "0"+end_s : end_s;
    if(type != null && type == 'fix_59sec'){
        end_s =  "59";
    }
    return end_h + ":" + end_m + ":" + end_s;
}
function set_starttime_notincludesec(start_time){
    var start =set_timezone(start_time, "Asia/Jakarta");
    var st_h  = start.getUTCHours();
    st_h =  st_h < 10 ? "0"+st_h : st_h;
    var st_m  = start.getUTCMinutes();
    st_m =  st_m < 10 ? "0"+st_m : st_m;


    return st_h + ":" + st_m;
}

function set_endtime_notincludesec(end_time){
    var end =set_timezone(end_time, "Asia/Jakarta");
    var end_h  = end.getUTCHours();
    end_h =  end_h < 10 ? "0"+end_h : end_h;
    var end_m  = end.getUTCMinutes();
    end_m =  end_m < 10 ? "0"+end_m : end_m;

    return end_h + ":" + end_m;
}


    /* all function below for postgresql database */
    async function get_deviceviewerbehaviour(devices_id, datereport) {

        // database instance;
        const pool = new Pool(config_pg);

        // pool.query('SELECT * FROM devices_viewerbehavior ORDER BY id ASC', (error, results) => {})
        const result = await pool.query('SELECT * FROM devices_viewerbehavior where devices_id = $1 ORDER BY id ASC', [devices_id]);
        if (!result || !result.rows || !result.rows.length) return {
            "status": false,
            "result_code": "-005",
            "result_desc": "not found data"
        };
        return {
            "status": true,
            data: result.rows,
            "result_code": "000",
            "result_desc": "Success"
        };

    }
    function get_keybyvalue_minute(object, value , type = null) {
        if(type != null){
            return Object.keys(object).find(key => object[key].h_m == value);
        }else{
            return Object.keys(object).find(key => object[key].h_m == value);
        }
    }

    function get_keybyvalue_gender(object, value , type = null , report_type  = null) {
        return Object.keys(object).find(key => object[key].gender == value);
    }

    function get_keybyvalue_genderoverview(object, value , type = null , report_type  = null , tvchannels_id = null) {
        
    
        
        return Object.keys(object).find(key => object[key].gender == value && object[key].tvchannels_id == tvchannels_id);
    }

    function get_keybyvalue_provinceoverview(object, value , type = null , report_type  = null , tvchannels_id = null , province_region = null) {
        
    
        
        return Object.keys(object).find(key => object[key].region_code == value  && object[key].tvchannels_id == tvchannels_id);
    }

    function get_keybyvalue_ageoverview(object, value , type = null , report_type  = null , tvchannels_id = null , province_region = null) {
        
        if(type == "satalite"){
            return Object.keys(object).find(key => object[key].age == value    && object[key].merge_s3remote_id == tvchannels_id);
        }else{
            return Object.keys(object).find(key => object[key].age == value    && object[key].tvchannels_id == tvchannels_id);
        }
    }

    function get_keybyvalue_region(object, value , type = null) {
  
        return Object.keys(object).find(key => object[key].province_region == value);

        
    }

    function get_keybyvalue_regionoverview(object, value , type = null , tvchannels_id = null) {
        if(type != null){
            if(type =='element.province_region'){
                return Object.keys(object).find(key => object[key].province_region == value && object[key].tvchannels_id == tvchannels_id);
            }else{
                 return Object.keys(object).find(key => object[key].tvchannels_id == value);
            }
        }else{
         
                return Object.keys(object).find(key => object[key].channels_id == value);
            
        }
    }

    function get_keybyvalue(object, value , type = null) {
        if(type != null){
            if(type =='element.province_region'){
                return Object.keys(object).find(key => object[key].province_region == value);
            }else if(type == "find_provincekey"){
                if(value == null){
                    return Object.keys(object).find(key => object[key].region_code == null ||  object[key].region_code == '' ||  object[key].region_code == NaN ||  object[key].region_code == 'NaN')
                }else{
                    return Object.keys(object).find(key => object[key].region_code == value); // case : find province bangkok
                }
            }else{
                return Object.keys(object).find(key => object[key].tvchannels_id == value);
            }
        }else{
         
                return Object.keys(object).find(key => object[key].channels_id == value);
            
        }
    }

    function merge_andsortallofthisfuck(tvprogram_basetime_satalitereport_data  ,tvprogram_basetime_iptvreport_data , channels_id){
        var key = 0;
        var merge_data = [];
        // case : summarize two object
        
        tvprogram_basetime_satalitereport_data.forEach(element => {
           
            let h_m =  element.h_m;
            if(has_white_space(h_m)){
                h_m   = removeSpaces(h_m);
            }
            let iptv_key = get_keybyvalue_minute( tvprogram_basetime_iptvreport_data ,h_m  , "iptv");
            
            if(iptv_key >= 0 && iptv_key != undefined){
                
                element =  sum_objectbykey( element , tvprogram_basetime_iptvreport_data[iptv_key]);
            }
            merge_data.push(element);
           
            ++ key;
          
        })

        
        

        return merge_data;
    }
    function get_thistvprogram(tvprogram_basetime_satalitereport_data  ,tvprogram_basetime_iptvreport_data , channels_id){
        var key = 0;
        var merge_data = [];
     
        // case : summarize two object
        tvprogram_basetime_satalitereport_data.forEach(element => {
            let tvchannel_id =  element.merge_s3remote_id;
         
            let iptv_key = get_keybyvalue( tvprogram_basetime_iptvreport_data ,tvchannel_id  , "iptv");
            if(iptv_key >= 0 && iptv_key != undefined){
                element =  sum_objectbykey( element , tvprogram_basetime_iptvreport_data[iptv_key]);
            }
            merge_data.push(element);
            ++ key;
          
        })
        // case : sort object
        let tvprogram_sortable = tvprogram_sortables( merge_data , channels_id);
        return tvprogram_sortable;
    }
    function merge_data_regionoverview(tvprogram_basetime_satalitereport_data  ,tvprogram_basetime_iptvreport_data , keyspecific =null){
        var key = 0;
        var merge_data = [];
        let key_;
        // case : summarize two object
       
        var c = 0;
        var sum_Test = 0;
        tvprogram_basetime_satalitereport_data.forEach(element => {
         
            if(keyspecific == null){ key_ =  element.merge_s3remote_id; }else { key_ = eval(keyspecific); }
            let findbytype =  keyspecific == null ? "iptv" : keyspecific;
            
            let tvchannels_id  = element.merge_s3remote_id;
            
            let iptv_key = get_keybyvalue_regionoverview( tvprogram_basetime_iptvreport_data ,key_  , findbytype , tvchannels_id);
            // console.log(iptv_key +  " tvchaannel id = > "  + tvchannels_id);
            if(iptv_key >= 0 && iptv_key != undefined){
               // ++ c;
                element =  sum_objectbykey( element , tvprogram_basetime_iptvreport_data[iptv_key]);
            }

            sum_Test += element.field_0_0;
            merge_data.push(element);
            
            ++ key;
          
        })
        
     //   console.log(sum_Test);
      
        return merge_data;
    }
    function merge_data(tvprogram_basetime_satalitereport_data  ,tvprogram_basetime_iptvreport_data , keyspecific =null){
        var key = 0;
        var merge_data = [];
        let key_;
        // case : summarize two object
       
        var c = 0;

        

        tvprogram_basetime_satalitereport_data.forEach(element => {
         
            if(keyspecific == null){ key_ =  element.merge_s3remote_id; }else { key_ = eval(keyspecific); }
            let findbytype =  keyspecific == null ? "iptv" : keyspecific;
           
            let iptv_key = get_keybyvalue( tvprogram_basetime_iptvreport_data ,key_  , findbytype);
            
            if(iptv_key >= 0 && iptv_key != undefined){
               // ++ c;
                element =  sum_objectbykey( element , tvprogram_basetime_iptvreport_data[iptv_key]);
            }
            merge_data.push(element);
            
            ++ key;
          
        })

        
      
        return merge_data;
    }

    function merge_data_original(original_tvprogram_basetime_satalitereport_data  ,original_tvprogram_basetime_iptv_data , keyspecific =null){
        var key = 0;
        var merge_data = [];
        let key_;
        // case : summarize two object
        original_tvprogram_basetime_satalitereport_data.forEach(element => {
         
            if(keyspecific == null){ key_ =  element.merge_s3remote_id; }else { key_ = eval(keyspecific); }
            let findbytype =  keyspecific == null ? "iptv" : keyspecific;
           
            let iptv_key = get_keybyvalue( original_tvprogram_basetime_iptv_data ,key_  , findbytype);
           
            if(iptv_key >= 0 && iptv_key != undefined){
                element =  sum_objectbykey( element , original_tvprogram_basetime_iptv_data[iptv_key]);
            }
            merge_data.push(element);
            
            ++ key;
          
        })
        
        return merge_data;
    }

    function merge_data_gender(tvprogram_basetime_satalitereport_data  ,tvprogram_basetime_iptvreport_data ){
        var key = 0;
        var merge_data = [];
        // case : summarize two object
      
        tvprogram_basetime_satalitereport_data.forEach(element => {
            
            let gender =  element.gender;
            let iptv_key = get_keybyvalue_gender( tvprogram_basetime_iptvreport_data ,gender  , "iptv");
            if(iptv_key >= 0 && iptv_key != undefined){
                element =  sum_objectbykey( element , tvprogram_basetime_iptvreport_data[iptv_key]);
            }
         
            merge_data.push(element);
            
            ++ key;
          
        })
        return merge_data;
    }

    function merge_data_gender_overview(tvprogram_basetime_satalitereport_data  ,tvprogram_basetime_iptvreport_data ){
        var key = 0;
        var merge_data = [];
        // case : summarize two object
      
        tvprogram_basetime_satalitereport_data.forEach(element => {
            
            let gender =  element.gender;
            var merge_s3remote_id = element.merge_s3remote_id;
           
            let iptv_key = get_keybyvalue_genderoverview( tvprogram_basetime_iptvreport_data ,gender  , "iptv" , null , merge_s3remote_id);

            if(iptv_key >= 0 && iptv_key != undefined){
                element =  sum_objectbykey( element , tvprogram_basetime_iptvreport_data[iptv_key]);
            }
           
            merge_data.push(element);
            
            ++ key;
          
        })
      
        return merge_data;
    }
    function merge_data_province_overview(tvprogram_basetime_satalitereport_data  ,tvprogram_basetime_iptvreport_data ){
        var key = 0;
        var merge_data = [];
        // case : summarize two object
      
        tvprogram_basetime_satalitereport_data.forEach(element => {
            
            let region_code =  element.region_code;
            var merge_s3remote_id = element.merge_s3remote_id;
            var province_region = element.province_region;
           
            let iptv_key = get_keybyvalue_provinceoverview( tvprogram_basetime_iptvreport_data ,region_code  , "iptv" , null , merge_s3remote_id , province_region);
            if(iptv_key >= 0 && iptv_key != undefined){
                element =  sum_objectbykey( element , tvprogram_basetime_iptvreport_data[iptv_key]);
            }
           
            merge_data.push(element);
            
            ++ key;
          
        })
      
        return merge_data;
    }
    function merge_data_age_overview(tvprogram_basetime_satalitereport_data  ,tvprogram_basetime_iptvreport_data ){
        var key = 0;
        var merge_data = [];
        // case : summarize two object
      
        tvprogram_basetime_satalitereport_data.forEach(element => {
            // console.log(element);
            // console.log("--------");
            let age =  element.age;
            var merge_s3remote_id = element.merge_s3remote_id;
        
            let iptv_key = get_keybyvalue_ageoverview( tvprogram_basetime_iptvreport_data ,age  , "iptv" , null , merge_s3remote_id , null);
            if(iptv_key >= 0 && iptv_key != undefined){
                element =  sum_objectbykey( element , tvprogram_basetime_iptvreport_data[iptv_key]);
            }
            merge_data.push(element);
          
            ++ key;
          
        })

        /*================  case : check iptv have age x but satalite not have x =============================== */
        tvprogram_basetime_iptvreport_data.forEach(element => {
            let age =  element.age;
            var merge_s3remote_id = element.tvchannels_id;
            // console.log(merge_s3remote_id);
            let satalite_key = get_keybyvalue_ageoverview( tvprogram_basetime_satalitereport_data ,age  , "satalite" , null , merge_s3remote_id , null);
            if(satalite_key == undefined){
                let channels_id  = get_tvchannel_id_top5statisticapi( merge_s3remote_id );
                if(channels_id != undefined){
                    // console.log("merge_s3remote_id " + merge_s3remote_id +" age " + age);
                    element.channels_id = channels_id;
                    merge_data.push(element);
                }
            }
         
        })
        /*================ (eof) case : check iptv have age x but satalite not have x =============================== */
       
        return merge_data;
    }

    function tvprogram_sortables(merge_data , channels_id){
        let thischannel_tvprogramkey = get_keybyvalue( merge_data ,channels_id );
        var tvprogram_obj = merge_data[thischannel_tvprogramkey];
       
        if(typeof merge_data[thischannel_tvprogramkey] != undefined) { delete merge_data[thischannel_tvprogramkey].channels_id;} // case : delete this object key
        if(typeof merge_data[thischannel_tvprogramkey] != undefined) { delete merge_data[thischannel_tvprogramkey].merge_s3remote_id};
        if(typeof merge_data[thischannel_tvprogramkey] != undefined) { delete merge_data[thischannel_tvprogramkey].tvchannels_id};
        const tvprogram_sortable = Object.fromEntries(
            Object.entries(tvprogram_obj).sort(([,a],[,b]) => b - a)
        );

        return tvprogram_sortable;
    }

    function list20tvchannel_ondemand(type = null){

        
            // 463 ch3
            // 525 altv
            // 271 ch9 ( mcot hd)
            // 413 ch5
            // 416 ch7
            // 251 nbt
            // 268 channel8
            // 264 work point
            // 272 one 31
            // 277 PPTV
            // 270 Mono 29
            // 275 Amarin TV
            // 263 Nation
            // 257 TNN 
            // 273 Thairath TV
            // 266 GMM25 ( 71)
            // 259 JKN 18
            // 265 True 4u
            // 568 T Sport 
            // 252 Thai PBS
            

        $where_in = "";
        if(type == null){ // case : null = satalite
            $where_in =  " channels_id = 463 or  channels_id = 525  or  channels_id = 271  or  channels_id = 413  or  channels_id = 416 ";
            $where_in += " or  channels_id = 251    or channels_id = 268  ";
            $where_in += " or channels_id = 264  or channels_id = 272  or channels_id = 277   or channels_id = 270 or channels_id = 275   ";
            $where_in += " or channels_id = 263 or channels_id = 257 or channels_id = 273  or channels_id = 266  or channels_id = 259  ";
            $where_in += " or channels_id = 265  or channels_id = 568 or channels_id = 252";

          //  console.log($where_in);
        }else{
            // 
            $where_in  = " tvchannels_id = 66 or tvchannels_id = 65 or  tvchannels_id = 62  or  tvchannels_id = 67 or  tvchannels_id = 63 ";
            $where_in += " or tvchannels_id = 3 or tvchannels_id = 2 or  tvchannels_id = 48  or  tvchannels_id = 76 or  tvchannels_id = 73 ";
            $where_in += " or tvchannels_id = 74 or tvchannels_id = 39 or  tvchannels_id = 45  or  tvchannels_id = 69  or  tvchannels_id = 71"; 
            $where_in += " or  tvchannels_id = 70 or  tvchannels_id = 68   or  tvchannels_id = 64  or tvchannels_id = 139 or tvchannels_id=225 ";
        }
        return $where_in;
    }
    function listothertv20tvchannel_ondemand(type = null){

        
        // 463 ch3
        // 525 altv
        // 271 ch9 ( mcot hd)
        // 413 ch5
        // 416 ch7
        // 251 nbt
        // 268 channel8
        // 264 work point
        // 272 one 31
        // 277 PPTV
        // 270 Mono 29
        // 275 Amarin TV
        // 263 Nation
        // 257 TNN 
        // 273 Thairath TV
        // 266 GMM25 ( 71)
        // 259 JKN 18
        // 265 True 4u
        // 568 T Sport 
        // 252 Thai PBS
        

    $where_in = "";
    if(type == null){ // case : null = satalite
        $where_in =  " channels_id != 463 and  channels_id != 525  and  channels_id != 271  and  channels_id != 413  and  channels_id != 416 ";
        $where_in += " and  channels_id != 251    and channels_id != 268  ";
        $where_in += " and channels_id != 264  and channels_id != 272  and channels_id != 277   and channels_id != 270 and channels_id != 275   ";
        $where_in += " and channels_id != 263 and channels_id != 257 and channels_id != 273  and channels_id != 266  and channels_id != 259  ";
        $where_in += " and channels_id != 265  and channels_id != 568 and channels_id != 252";
    }else{
        // 
        $where_in  = " tvchannels_id != 66 and tvchannels_id != 65 and  tvchannels_id != 62  and  tvchannels_id != 67 and  tvchannels_id != 63 ";
        $where_in += " and tvchannels_id != 3 and tvchannels_id != 2 and  tvchannels_id != 48  and  tvchannels_id != 76 and  tvchannels_id != 73 ";
        $where_in += " and tvchannels_id != 74 and tvchannels_id != 39 and  tvchannels_id != 45  and  tvchannels_id != 69  and  tvchannels_id != 71"; 
        $where_in += " and  tvchannels_id != 70 and  tvchannels_id != 68   and  tvchannels_id != 64  and tvchannels_id != 139 and tvchannels_id != 225 and tvchannels_id != 272  ";
    }
    return $where_in;
}

    function tvdigitalchannel_ondemand(type = null , useby = null){
       
            
        var arr;
        if(type == "iptv"){
            
            // case : iptv
            if(useby == null){
                arr = [66 ,225, 65 , 62 , 67 ,63 , 3 , 2 , 48 , 76 , 73 , 74 , 39 ,45 , 69,71 , 70 ,68 , 64 , 139];
            }else{

            }
        }else{
            // case : satalite
            if(useby == null){
                arr = [252, 525,463 , 413  ,416, 271 , 268 , 251 , 264 , 272 , 277 , 270 , 275 , 263 , 257 , 273 , 266 , 259 , 265, 568  ];
            }else{
                // 252 Thai PBS
                // 525 altv
                // 463 ch3
                // 413 ch5
                // 416 ch7
                // 271 ch9 ( mcot hd)
                // 268 channel8
                // 251 nbt
                // 264 work point
                // 272 one 31
                // 277 PPTV
                // 270 Mono 29
                // 275 Amarin TV
                // 263 Nation
                // 257 TNN 16 ใส่เป็น TNN
                // 273 Thairath TV
                // 266 GMM25 ( 71)
                // 259 JKN 18
                // 265 True 4u
                // 568 T-SPORTS 7 
            
                arr = ["THAI PBS", "ALTV", "CH-3" , "CH-5"  ,"CH-7", "MCOT-HD" , "CH-8" , "NBT" , "WORKPOINT" , "ONE" , "PPTV" , "MONO29" , "AMARIN TV"
                 , "NATION TV" , "TNN" , "THAIRATH TV" 
                , "GMM25" , "JKN18" , "TRUE4U", "T SPORTS"];
            }
        }
        return arr;

    }
    function sum_objectbykey(...objs) {
        return objs.reduce((a, b) => {
          for (let k in b) {
              
              if(k != 'tvchannel_id' && k!= 'channels_id' && k != 'merge_s3remote_id' ){
                if( k == 'h_m' || k =='age' || k=='hour' || k =='minute' || k =='gender' || k == 'total_row' || k == 'province_region' || k == 'region_code' || k == 'province_name' ){
                  
                    if (b.hasOwnProperty(k)) { a[k] = b[k] } ;
                }else{
                    if (b.hasOwnProperty(k))
                    a[k] = (a[k] || 0) + b[k];
                }
              }else{
                if (b.hasOwnProperty(k))
                a[k] = b[k];
              }
          }
          return a;
        }, {});
      }
    
      function set_iptvdefaultObjifzero(originaldata , channel_arr){
            var checkarray = [];
            originaldata.forEach(function(ec, idx, obj) { // case : get array value not exist on channel array
                if(channel_arr.includes(ec.tvchannels_id )){
                    checkarray.push(ec.tvchannels_id );
                }
                
            });
            let difference = channel_arr.filter(x => !checkarray.includes(x));

            
            if(difference.length > 0){
                let defaultobj  = Object.keys(originaldata[0]);
           
                difference.forEach(function(no, ix, oj) {
                    const obj = {};
                    for (const key of defaultobj) {
                        if(key == 'tvchannels_id'){ obj[key] = no;}else {obj[key] = 0; }
                    }
                    originaldata.push(obj);
                    
                });
            }
            return originaldata;
      }
       function find_channeldelete_specificarray(type , path , fixed_channelarray = null){
        //let data     = require(path);

        const rawdata = readFileSync(path);
        var data = JSON.parse(rawdata);
        
        channel_arr = fixed_channelarray;
        let originaldata = data;
        let newdata = data;
        // check original data for null value
        

        newdata.forEach(function(e, index, object) {
            if(type != "iptv"){ 
                // if(e.channels_id  == 525 ){
                //     console.log(originaldata);
                // }

                if(!channel_arr.includes(e.channels_id )){
                   
                   delete originaldata[index];
                }
            }
       });
       let neworiginaldata = originaldata.filter(function( element ) {
           
            return element != undefined;
        });
        

        return neworiginaldata;
 
    }
    async function find_channeldelete(type , data , fixed_channelarray = null){
        let channel_arr =  tvdigitalchannel_ondemand(type);
        if(fixed_channelarray != null ){
            channel_arr = fixed_channelarray;
        }

        let originaldata = data;
        if(type == "iptv"){
            originaldata =  set_iptvdefaultObjifzero(originaldata , channel_arr);
        }
        
        let newdata = data;
        // check original data for null value
        

        newdata.forEach(function(e, index, object) {
            if(type != "iptv"){

                if(!channel_arr.includes(e.channels_id )){
                   
                   delete originaldata[index];
                }
            }else{
                if(type =="iptv"){
                  
                    if(!channel_arr.includes(e.tvchannels_id)){
                       
                        delete originaldata[index];
                    }
                }
            }
       });
       let neworiginaldata = originaldata.filter(function( element ) {
           
            return element != undefined;
        });
        
       
        return neworiginaldata;
 
    }
    function sum_everychannel_baseontime(array){

       
        var newArr = [];

        array.forEach(function(e, index, object) {
            for ( var property in array[index] ) {
              
                if(newArr[property]==undefined && property !='tvchannels_id' && property != 'channels_id' && property != 'merge_s3remote_id'){
                    newArr[property] = 0;
                    newArr[property] += parseInt(array[index][property]);
                }else{
                    if(property !='tvchannels_id' && property != 'channels_id' && property != 'merge_s3remote_id'){
                    newArr[property] += parseInt(array[index][property]);
                    }
                }
               
              }
          
        });
    
        return newArr;
    }

    function sum_array_gender(array){

       
        var newArr = {};

        array.forEach(function(e, index, object) {
            for ( var property in array[index] ) {
                
                if(property == 'gender'){
                    var gender_value = array[index][property];
                    if(newArr[gender_value] ==undefined){
                        newArr[gender_value] = []; 
                        newArr[gender_value][property] = gender_value; // set gender value into array
                    }
                }else{
                    if(newArr[gender_value][property]==undefined && property !='tvchannels_id' && property != 'channels_id' && property != 'merge_s3remote_id'){
                            var value = array[index][property];
                            newArr[gender_value][property] = 0;
                            newArr[gender_value][property] += parseInt(value);
                    }else{
                        if(property !='tvchannels_id' && property != 'channels_id' && property != 'merge_s3remote_id'){
                            var value = array[index][property];
                            newArr[gender_value][property] += parseInt(value);
                        }
                    }
                }
              }
          
        });
     
        var return_array = []; // case : reindex before return
        for (const [key, value] of Object.entries(newArr)) {
            return_array.push(value);
        }
        return return_array;
    }

    
    function sum_array_province(path_regionall , channels_id = null){

      // case :  เอาเฉพาะแค่ช่องของตัวเอง เช่น TPBS ...
        if(channels_id != null){
            let fixed_channelarray  = [];
            fixed_channelarray.push( channels_id );
            array =   find_channeldelete_specificarray(  "satalite" , path_regionall , fixed_channelarray); // get only this channel gender object
        }else{

            const rawdata = readFileSync(path_regionall);
            var array = JSON.parse(rawdata);
          
        }
        var newArr = {};
        region_bangkok = 0;
        array.forEach(function(e, index, object) {
            for ( var property in array[index] ) {
                // case : set default region property
                var region_code = parseInt(array[index]['region_code']);
                var province_name = array[index]['province_name'];
                var channels_id_self   = array[index]['channels_id'];
              
                
                if(region_code != undefined && region_code != null   && province_name !=  null && region_code > 0){
                       
                        if(newArr[region_code] == undefined){
                            newArr[region_code] = [];
                            newArr[region_code]['region_code']       =  region_code;
                            newArr[region_code]["province_name"]=  array[index]['province_name'];
                        }
                       // console.log("property : " + property);
                       // console.log(newArr[region_code]);
                       

                        if(property !='tvchannels_id'
                        && property != 'channels_id'  
                        && property != 'id'  
                        && property != 'merge_s3remote_id'
                        && property != 'province_region'
                        && property != 'province_name'
                        && property != 'region_code'
                    ){
                      
                        if(typeof newArr[region_code][property] != undefined  && newArr[region_code][property] != null){
                        
                                var value = array[index][property];
                                value     = parseInt(value) > 0 ? parseInt(value) : 0 ;
                                newArr[region_code][property] += value;
                               
                                if(region_code == 10 && property == 'field_0_0'){
                                    region_bangkok += value;
                                }
                        }else{
                    
                                var value = array[index][property];
                                value     = parseInt(value) > 0 ? parseInt(value) : 0 ;
                                newArr[region_code][property] = 0;
                                newArr[region_code][property] += value;

                                if(region_code == 10 && property == 'field_0_0'){
                                    region_bangkok += value;
                                }
                                
                        }
                    }
                }
              }
          
        });
        var return_array = []; // case : reindex before return
        for (const [key, value] of Object.entries(newArr)) {
            return_array.push(value);
        }
        
        // console.log(" region bangkok : " + region_bangkok);
        //console.log(return_array);
      
        return return_array;
    }


    function sum_array_provinceregion(path_regionall , channels_id = null){

        // let array = require(path_regionall);
        if(channels_id != null){
            let fixed_channelarray  = [];
            fixed_channelarray.push( channels_id );
            array =   find_channeldelete_specificarray(  "satalite" , path_regionall , fixed_channelarray); // get only this channel gender object
        }else{

            const rawdata = readFileSync(path_regionall);
            var array = JSON.parse(rawdata);
          
        }
        // const rawdata = readFileSync(path_regionall);
        // var array = JSON.parse(rawdata);
        var newArr = {};
        array.forEach(function(e, index, object) {
            for ( var property in array[index] ) {
                // case : set default region property
                var province_region = array[index]['province_region'];
                if(property == "province_region" &&  newArr[province_region] == undefined){
                    newArr[province_region] = [];
                    newArr[province_region][property] =  province_region;
                }
               //  console.log(array[index][property]);
                if(property !='tvchannels_id'
                && property != 'channels_id'  
                && property != 'id'  
                && property != 'merge_s3remote_id'){
                    
                    if(newArr[province_region][property] != undefined ){
                       
                        if(property == 'province_region'){   
                                newArr[province_region][property] = array[index][property];
                        }else{ 
                     
                                newArr[province_region][property] += parseInt(array[index][property]);  
                        }
                    }else{
                
                            var value = array[index][property];
                            value     = parseInt(value) > 0 ? parseInt(value) : 0 ;
                            newArr[province_region][property] = value;
                    }
                }
              }
          
        });
        var return_array = []; // case : reindex before return
        for (const [key, value] of Object.entries(newArr)) {
            return_array.push(value);
        }
       
        return return_array;
    }


   function get_starttime(value){

        var start =set_timezone(value.start_time, "Asia/Jakarta");      
        var st_h  = start.getUTCHours();
        st_h =  st_h < 10 ? "0"+st_h : st_h;
        var st_m  = start.getUTCMinutes();
        st_m =  st_m < 10 ? "0"+st_m : st_m;

        start =  st_h + ":" + st_m;
        
        return start;
   }
   function get_endtime(value){

        var end =set_timezone(value.end_time, "Asia/Jakarta");                
        var end_h  = end.getUTCHours();
        end_h =  end_h < 10 ? "0"+end_h : end_h;
        var end_m  = end.getUTCMinutes();
        end_m =  end_m < 10 ? "0"+end_m : end_m;
        end =  end_h + ":" + end_m;
        return end;
   }
//    function sleep(ms) {
//     return new Promise((resolve) => {
//       setTimeout(resolve, ms);
//     });
//   }
   async function statistics(datestring = null , start_datetime = null , end_datetime = null , _channel_id = null ){
        let pool = await sql.connect(config);

        let date     = init_getdatefromdatetime( datestring , "yymmdd" ); 
        var is_fake_val = false;
        if(date == '2023-08-26'){
            date = '2023-08-19';
            is_fake_val = true;
        }
        var timestart= start_datetime;
        var timeend  = end_datetime;
        var date_arr = get_timeinterval(date , date  , null , "statistic_api" , timestart , timeend); // get time from javascript generate 
        
        let avg_minute = null;
        if(date_arr.length != 0){
            let channels = await get_channels( _channel_id );
           // console.log(channels);
            if(channels[0].status != undefined){
                return channels;
            }else{
                let pool = await sql.connect(config);
            
              
              
                    // case : overview report
               
                    let tvprogram_basetime_satalitereport_data;
                    let tvprogram_basetime_iptvreport_data;
                    let rating_data_daily_satalite;
                    let rating_data_daily_iptv;
                    let tvprogram_basetime_ytreport_data;
                    let satalite_gender;
                    let iptv_gender;
                    let youtube_gender;
                    let province_region_satalite;
                    let province_region_iptv;
                    var month = get_dateobject(date, "month");
                    var year = get_dateobject(date, "year");
                    let rating_data_table  = get_ratingdatatable( date );
                    let rating_data_table_iptv  = get_ratingdatatable( date  , "iptv");
                    
                    let province_region_youtube;
                    let province_satalite;
                    let province_iptv;
                    let province_youtube;
                    let channeldailyrating_data;
                    let strtotime = new Date().getTime();
                    strtotime = strtotime  + "" + Math.floor(100000 + Math.random() * 900000) + "" + _channel_id;
                    let tempdata_table = "temp_data_" + strtotime;
              
                    let provinces_all;
                    // let channel_daily_devices_summary_table = get_channeldailydevicetbl(date);
                        let channel_daily_devices_summary_table = query_deviceusertable("statistic_api");
                        let iptv_channel_id = null;
                        if(_channel_id != null){
                             iptv_channel_id = get_iptvchannel_id( _channel_id ); // case : get iptv channel id 
                        }
                        let $query_satalite_minute = build_query( "overview_daily_satalite_api" , rating_data_table , _channel_id , date  , null , null, date_arr ,null , null , avg_minute );
                        let $query_IPTV_s3pp     = build_query( "overview_daily_s3app_api" , "[S3Application].dbo." +rating_data_table_iptv  , iptv_channel_id , date , null , null , date_arr  , null ,null , avg_minute );
                       // console.log($query_IPTV_s3pp.temptable);
                        let req_dropsatalite = new sql.Request(pool);
                        let req_dropiptv     = new sql.Request(pool);
                       
                     
                        rating_data_daily_satalite = await pool.request()
                        .query($query_satalite_minute.temptable);
                      
                        rating_data_daily_iptv     = await pool.request()
                        .query($query_IPTV_s3pp.temptable);
                     
                        // case : drop old temp table and create new one
                        //$var_temptable = `#temp_data`;
                        $var_temptable = tempdata_table;
                        // case : write new query
                        // return;
                        let $query_satalite = build_query( "overview_tvprogram_basetime_satalitereport_statisticapi" , rating_data_table ,  _channel_id , date , null , null , date_arr );
                        
                       // return {"status":true};
                        // case : crete new temp table satalite
                        // $query_createtemptable = "SELECT * INTO " + $var_temptable +" FROM ( "+ $query_satalite.temptable + " ) as r1";  // case : create new temporary table 
                        let req = new sql.Request(pool);
                        // await req.batch($query_createtemptable);

                        // case :create view 
                       $query_crateview = " CREATE VIEW "+tempdata_table+" AS  " + $query_satalite.temptable;
                       await req.batch($query_crateview);

                      // console.log(1);

                     
                       // await sleep(1);
                        $query_tvprogrambaseonsatalite = ` SELECT merge_s3remote_id,channels_id ${$query_satalite.select_condition} FROM  ${$var_temptable}  inner join channels on #temp_data.channels_id = channels.id where channels.active = 1 group by channels_id,merge_s3remote_id`; // case : select temporary table data
                        
                  

                       

                        
                        $var_temptable_gender ="view_gender_" + strtotime; 
                   
                        $query_createtempgender = " CREATE VIEW "+$var_temptable_gender+" AS  select r2.devices_id as dvid ,r2.age,r2.gender from "+channel_daily_devices_summary_table+" group by r2.devices_id,r2.gender ,r2.age";
                       
 
                        try{
                            // file not presenet         
                            await req.batch($query_createtempgender);
                           // console.log(2);
                          
                        } catch (err){
                           // console.log(err);
                           
                        }
                  

                        // await sleep(1000); // delay 1 second .. 

                        // case  : (query) group by gender ( satalite )
                        let $query_satalite_gender = build_query( "overview_groupby_gender_satalite_statisticapi" , $var_temptable_gender , _channel_id , null , null , null , date_arr, null , $var_temptable );
                        // let query_tempgender_satalite = await req_tempgender.batch($query_satalite_gender.temptable);
                        let query_tempgender_satalite = await req.batch($query_satalite_gender.temptable);
                        satalite_gender = query_tempgender_satalite.recordsets[0];

                      
                        //console.log($query_satalite_gender);
                        let condition_obj = { "temp_gender" : $var_temptable_gender};
                         // case  : (query) group by age ( satalite )
                         let $query_satalite_age   = build_query( "overview_groupby_age_satalite" ,null , _channel_id , null , null , null , date_arr, null , $var_temptable  , null , null, null , condition_obj);
                          
                        //  let query_tempgender_age  = await req_tempgender.batch($query_satalite_age.temptable);
                        let query_tempgender_age  = await req.batch($query_satalite_age.temptable);
                         satalite_age = query_tempgender_age.recordsets[0];
                        // return {"status":true};  
                      
                        // case : (query) group by province region satalite data
                        let $query_satalite_addr = build_query( "overview_groupby_addr_satalite_api" ,null , _channel_id , null , null , null , date_arr, null , $var_temptable );
                        // let query_groupbyaddr_satalite = await req_tempgender.batch($query_satalite_addr.temptable);
                        let query_groupbyaddr_satalite = await req.batch($query_satalite_addr.temptable);
                        province_region_satalite = query_groupbyaddr_satalite.recordsets[0];        
                     
                        let $query_province_addr = build_query( "overview_groupby_provinceaddr_satalite_api" ,null , _channel_id , null , null , null , date_arr, null , $var_temptable );
                        // let query_groupbyprovinceaddr_satalite = await req_tempgender.batch($query_province_addr.temptable);
                        let query_groupbyprovinceaddr_satalite = await req.batch($query_province_addr.temptable);
                        province_addrsatalite = query_groupbyprovinceaddr_satalite.recordsets[0];  
                       // console.log($query_province_addr.temptable);
                     
                        // case :  (drop) temp satalite
                        //let query_temptable = await req.batch($query_tvprogrambaseonsatalite);
                       
                        // await req.batch( "drop table " + $var_temptable ); // case : drop temp table
                       
                        //tvprogram_basetime_satalitereport_data = query_temptable.recordset;
    
                        // case : crete new temp table iptv
                        $var_temptable_iptv = `temp_data_iptv_${strtotime}`;
                        condition_obj.temp_data_iptv = $var_temptable_iptv;
                   
                        // create view iptv
                        let $query_iptv = build_query( "overview_tvprogram_basetime_iptvreport_s3app_api" , rating_data_table_iptv ,  iptv_channel_id , date , null , null , date_arr  , null , null ,null , null , null , condition_obj);
                        $query_crateview = " CREATE VIEW "+$var_temptable_iptv+" AS   "+ $query_iptv.temptable + "  ";
                        //console.log($query_crateview);return;
                        await req.batch($query_crateview);
                       // console.log(3);
                        
                         $query_tvprogrambaseiptv = ` SELECT tvchannels_id ${$query_iptv.select_condition} FROM  ${condition_obj.temp_data_iptv} group by tvchannels_id`; // case : select temporary table data
                        
                        //  let query_temptable_iptv = await reqs3app.batch($query_tvprogrambaseiptv);
                        let query_temptable_iptv = await req.batch($query_tvprogrambaseiptv);
    
                        // case  : (query) group by gender ( iptv )
                        let $query_iptv_gender;
                        $query_iptv_gender = build_query( "overview_groupby_gender_iptv_api" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv  , null ,null ,null , condition_obj);
                        //console.log($query_iptv_gender.temptable);return;
                        // let query_tempgender_iptv = await req_tempgender.batch($query_iptv_gender.temptable);
                        let query_tempgender_iptv = await req.batch($query_iptv_gender.temptable);
                        iptv_gender = query_tempgender_iptv.recordsets[0];
                        //return;

                        // case  : (query) group by gender ( iptv )
                        let $query_iptv_age;
                        $query_iptv_age = build_query( "overview_groupby_age_iptv" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv , null ,null , null , condition_obj);
                   
                        // let query_tempage_iptv = await req_tempgender.batch($query_iptv_age.temptable);
                        let query_tempage_iptv = await req.batch($query_iptv_age.temptable);
                        iptv_age = query_tempage_iptv.recordsets[0];
                       
                        
                        // case : (query) group by province region iptv data
                        let $query_iptv_addr = build_query( "overview_groupby_addr_iptv" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv );                
                        //console.log($query_iptv_addr);
                        //return;
                        // let query_groupbyaddr_iptv = await req_tempgender.batch($query_iptv_addr.temptable);
                        let query_groupbyaddr_iptv = await req.batch($query_iptv_addr.temptable);
                        province_region_iptv = query_groupbyaddr_iptv.recordsets[0];
            
    
                        // case : (query) group by province  iptv data
                        let $query_provinceiptv_addr = build_query( "overview_groupby_provinceaddr_iptv" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv );                
                        // let query_groupbyprovinceaddr_iptv = await req_tempgender.batch($query_provinceiptv_addr.temptable);
                        let query_groupbyprovinceaddr_iptv = await req.batch($query_provinceiptv_addr.temptable);
                        province_addriptv = query_groupbyprovinceaddr_iptv.recordsets[0];
                      
                
                         // case :  (drop) temp iptv
                        //  await reqs3app.batch( "drop table " + $var_temptable_iptv ); // case : drop temp table
                       // await req.batch( "drop table " + $var_temptable_iptv ); // case : drop temp table
                         tvprogram_basetime_iptvreport_data = query_temptable_iptv.recordset;
                
                  
                         //let reqs3rating_daily = new sql.Request(pool);
                  
                        // case : (query) get all province
                        let $query_getallprovinces = build_query( "get_provinces" , null ,  null , null , null , null , null ); 
                        // let execute_queryprovince = await reqs3rating_daily.batch($query_getallprovinces.rawquery);
                        let execute_queryprovince = await req.batch($query_getallprovinces.rawquery);
                        provinces_all= execute_queryprovince.recordset;


                          /** ================ case : get channel daily rating top 20 channel ============================  */
                        
                        //   let $query_getchanneldailyrating = build_query( "get_channeldailyrating_overview" , null ,  null , date , null , null , date_arr );       
                        //   let execute_queryraw = await reqs3rating_daily.batch($query_getchanneldailyrating.rawquery);
                        //   channeldailyrating_data= execute_queryraw.recordset;
                         /** ============================================================================================ */
                
                
                        // casae : (drop) temp gender
                        
                        // await req.batch( "drop table " + $var_temptable_gender ); // case : drop temp table
                         await req.batch( "drop view " + $var_temptable ); // case : drop temp table
                        await req.batch( "drop view " + $var_temptable_gender ); // case : drop temp table
                        await req.batch( "drop view " + $var_temptable_iptv ); // case : drop view iptv
                       // console.log(4);
                        var arr_data = set_arrdata( [] );
                        let send_back_arr;
                        if (rating_data_daily_satalite != null ) {
                            let rating_data_daily_satalite_obj = rating_data_daily_satalite.recordsets[0];
                            var rating_data_daily_satalite_ = JSON.parse(JSON.stringify(rating_data_daily_satalite_obj));
                           
                            if (rating_data_daily_satalite_ != null) {
                                 /** ============  case : create folder ========== */
                                $foldername = get_dateobject(date , "year") + "_"  +  get_dateobject(date , "month")+"_" + get_dateobject(date , "day");
                                let dir_jsonlocate     = "./json/api_statistic/logs/" + $foldername;
                                if (!fs.existsSync(dir_jsonlocate)){
                                    fs.mkdirSync(dir_jsonlocate);
                                }
                                /** ============  case : eof create folder ========== */

                                if(rating_data_daily_iptv != null){
                                    let rating_data_daily_iptv_obj = rating_data_daily_iptv.recordsets[0];

                                    /** ================ case : step (1)  merge gender iptv and satalite  ==========================   */
                                    let merge_gender;
                                    merge_gender  = merge_data_gender_overview(satalite_gender , iptv_gender);
                                   
                                    /** ================ (eof) case : step (1)  merge gender iptv and satalite  ==========================   */
                                    
                                     /** ================ case : step (2)  merge province iptv and satalite && region data  ==========================   */
                                    let merge_data_provincegroup_obj  = merge_data_province_overview(province_addrsatalite , province_addriptv); // merge provice object
                                   
                                    let merge_data_region_obj   = merge_data_regionoverview(province_region_satalite , province_region_iptv, "element.province_region"); // get all province region
                                    let path_regionall          = save_log( merge_data_region_obj, "logprovinceall_"  , dir_jsonlocate ); // save log : province all  
                                    var path_merge_province     = save_log( merge_data_provincegroup_obj, "logprovincemergedata_"  , dir_jsonlocate );
                                    let sum_array_province_all  = merge_data_provincegroup_obj;

                                    /** ================  (eof) case : step (2)  merge province iptv and satalite && region data ==========================   */

                                     /** ================ case : step (3)  combine age data from satalite , iptv ==========================   */
                                     let merge_data_age_obj  = merge_data_age_overview(satalite_age , iptv_age); // merge provice object
                                     // let path_age          = save_log( merge_data_age_obj, "logage_"  , dir_jsonlocate ); // save log : province all
                                     /** ===================================================================================  */
                                   
                                    arr_data = merge_andsortallofthisfuck( rating_data_daily_satalite_ , rating_data_daily_iptv_obj);
                                    // console.log(rating_data_daily_satalite_);return;
                                  
                                    send_back_arr = set_returnobject("statistics" , date_arr , arr_data , merge_gender , timestart , timeend 
                                    , provinces_all , sum_array_province_all
                                    , path_regionall , path_merge_province , merge_data_age_obj ); // case :  set return object
                                   //console.log(merge_gender);return;
                                
                                }
                                else{
                                    //  arr_data = rating_data_daily_satalite_;

                                    //  /** ================ case :  gender ==========================   */
                                    //  let merge_gender;
                                    //  merge_gender  = merge_data_gender_overview(satalite_gender , null);

                                    //  /** ================ (eof) case :  gender ==========================   */
                                    //  send_back_arr = set_returnobject("statistics" , date_arr , arr_data , merge_gender , timestart , timeend , provinces_all , sum_array_province_all); // case :  set return object

                                     return [{
                                        "status": false,
                                        "result_code": "400",
                                        "result_desc": "can't find internet tv data ."
                                    }];

                                }
                               
                                
                              
                                return   [{
                                    "status": true,
                                    "data": send_back_arr,
                                    "result_code": "200",
                                    "result_desc": "success"
                                }];
                                  
                               
                               
                            } else {
                                return [{
                                    "status": false,
                                    "result_code": "400",
                                    "result_desc": "can't find data on this period , please try again."
                                }];
                            }
                    
                    
                        }
    
                
                
               
            }
    
          
        }else{
            return [{
                "status": false,
                "result_code": "400",
                "result_desc": "startdatetime or enddatetime should not be empty."
            }];
        }

        

   }

    function findKeysByProgramId(objArray, targetProgramId) {
    const matchingKeys = [];
 
    for (let obj of objArray) {
     
      // Check if the current object has the specified program_id
      if (obj.program_id === targetProgramId.toString()) {
        // Get all keys (property names) for the matching object
   
        matchingKeys.push(obj);
      }
    }
  
    return matchingKeys;
  }
 

  
async function tvprogram_ratings(datestring = null , start_datetime = null , end_datetime = null , channels_id = null , program_id = null  , service_used = null){
    let pool = await sql.connect(config);

    var is_fake_val = false;
    if(datestring == '2023-08-26'){
        datestring = '2023-08-19';
        is_fake_val = true;
    }
    console.log(datestring);
    let date     = init_getdatefromdatetime( datestring , "yymmdd" ); 

    // find program statistic
    
    let start_datetime_string =  datestring  +  " " + start_datetime; // declare : start datetime
    let end_datetime_string   =  datestring  +  " " + end_datetime; //   declare : end   datetime
    let $query  = get_tvprogram_specificdatetime(start_datetime_string , end_datetime_string  , channels_id , program_id);
   
    
    let tvprogram = await pool.request()
        .query($query);
    if (tvprogram != null) {
        let tvprogram_obj = tvprogram.recordsets;
        var tvprogram_obj_ = JSON.parse(JSON.stringify(tvprogram_obj));
        let reach_device_statistic = await get_tvprogram_rating_statistic( date , channels_id , null , null , config);
         if(tvprogram_obj_[0] != undefined){
            var return_data=[];
            for (const element of tvprogram_obj_[0]) {
            // tvprogram_obj_[0].forEach(element => {
                /** ========= case : set datetime compliatable with sql server database  =================  */
               
                let findKeysByProgramIds =  findKeysByProgramId(reach_device_statistic , element.program_id);
             
                
                let starttime =  set_starttime_notconvert(element.start_datetime );
                let endtime   =  set_endtime_notconvert(element.end_datetime);
                starttime     = datestring +  " " + starttime;
                endtime     =   datestring +  " " + endtime;
                /** ========= (eof) case : set datetime compliatable with sql server database  =================  */
                let ratings =      await get_tvprogram_rating( starttime , endtime , element.channel_id , null , date , config , service_used );
                rating = ratings[0].rating;
                
                let reach_devices =    ratings[0].reach_devices;
                rating = round_xdecimalplace(rating , 3);
                rating = rating > 0 ? rating :  0;
                reach_devices = reach_devices > 0 ? reach_devices :  0;
                element.rating = rating;
                element.reach_devices = round_xdecimalplace(reach_devices );
                element.reach_device_unique = findKeysByProgramIds[0]?.reach_device_unique | 0;
                element.total_device_psi = findKeysByProgramIds[0]?.total_device_psi | 0;
                element.total_device_psi_unique = findKeysByProgramIds[0]?.total_device_psi_unique | 0;
                element.othertv_device_psi = findKeysByProgramIds[0]?.othertv_device_psi | 0;
                element.othertv_device_psi_unique = findKeysByProgramIds[0]?.othertv_device_psi_unique | 0;
                element.youtube_device = findKeysByProgramIds[0]?.youtube_device | 0;
         
                if(service_used != null){
                 
                    element.minute_diff = ratings[0].minute_diff;
                    element.sec_diff    = ratings[0].sec_diff;
                }
                
            }

            if(is_fake_val){
                j = JSON.stringify(tvprogram_obj_[0]);
                j = j.replace(/2023-08-19/g,"2023-08-26");
                tvprogram_obj_[0] = JSON.parse(j);
            }

            return [{
                "status": true,
                data: tvprogram_obj_[0],
                "result_code": "200",
                "result_desc": "Success"
            }];

        }
    
     
       
    }else{
        return [{
            "status": false,
            "result_code": "400",
            "result_desc": "data not found."
        }];
    }

    

}

function find_minutediff(start_datetime_string , end_datetime_string){
  
//     var ms = moment(end_datetime_string , "YYYY-MM-DD HH:mm:ss").diff(moment(start_datetime_string , "YYYY-MM-DD HH:mm:ss"));
//     var d =  moment.duration(ms);
 
//    // var s =   moment.utc(ms).format("mm");
//     var s  =moment.utc(ms.).format("HH:mm:ss");
    var start =  moment(start_datetime_string); 
    var end   =  moment(end_datetime_string);
    let s = end.diff(start , "minute"); // 86400000
    return parseInt(s);

}

function find_seconddiff(start_datetime_string , end_datetime_string){
    var start =  moment(start_datetime_string); 
    var end   =  moment(end_datetime_string);
    let s = end.diff(start , "seconds"); 
    return parseInt(s);
}
async function channel_ratings(datestring = null , start_datetime = null , end_datetime = null , channels_id = null , program_id = null ){
    let pool = await sql.connect(config);

    var is_fake_val = false;
    if(datestring =='2023-08-26'){
        datestring ='2023-08-19';
        is_fake_val = true;
    }
    let recheck_isit_top5channel =  get_channelname_api(channels_id);
    if(recheck_isit_top5channel == undefined ||  recheck_isit_top5channel == null){
        return [{ status : false , result_code : 405 ,  result_desc : "not authorized to access this channel" }];
    }
    let date     = init_getdatefromdatetime( datestring , "yymmdd" ); 
    var timestart= start_datetime + ":00";
    var timeend  = end_datetime+ ":00";


    var date_arr = get_timeinterval(date , date  , null , "channel_rating_api" , timestart  , timeend ); // get time from javascript generate 
    let start_datetime_string =  datestring  +  " " + timestart; // declare : start datetime
    let end_datetime_string   =  datestring  +  " " + timeend; //   declare : end   datetime
    /** =============== case check : morethan 15 minute =================  */
    let minute_diff = find_minutediff(start_datetime_string , end_datetime_string);
    if(minute_diff > 15){
        return [{ status : false , result_code : 405 ,  result_desc : "not authorized to request data more than 15 minute" }];
    }
   
    /** =============== (eof) check : morethan 15 minute =================  */
    let get_all_channel_status = null;
    if(channels_id != null){ get_all_channel_status  = true; }

    let rating_data_daily_satalite;
    let rating_data_daily_iptv;
    let channel_daily_devices_summary_table = query_deviceusertable("statistic_api");
    let rating_data_table  = get_ratingdatatable( date );
    let rating_data_table_iptv  = get_ratingdatatable( date  , "iptv");
    let iptv_channel_id  = get_channel_id_top5statisticapi( channels_id );
    let strtotime = new Date().getTime();
    strtotime = strtotime  + "" + Math.floor(100000 + Math.random() * 900000);
    $var_temptable = `temp_data_${strtotime}`;
    $var_temptable_gender =`temp_gender_${strtotime}`; 
    $var_temptable_iptv = `temp_data_iptv_${strtotime}`;
    let satalite_gender;
    let province_region_satalite;
    let province_region_iptv;
    let view_satalite = "view_satalite_" + strtotime;
    let view_iptv     = "view_iptv_" + strtotime;
    let temptableratingsatalite =  await create_temptblsatalite(pool ,  date  , config , channels_id , start_datetime_string , end_datetime_string , view_satalite);     
    let temptableratingiptv     =  await create_temptbliptv(pool ,  date  , config , iptv_channel_id , start_datetime_string , end_datetime_string , view_iptv);
    //console.log(temptableratingiptv);
    var temptableratingdata     = temptableratingsatalite[0].data;
    var temptableiptvdata       = temptableratingiptv[0].data;

  
    /**========== step(1) : query builder ==================================  */
    let $query_satalite_minute = build_query( "overview_minute_api" , rating_data_table , channels_id , date  , null , null, date_arr ,null , null , 1 , start_datetime_string , end_datetime_string );    
    //console.log(5555);
    let $query_IPTV_s3pp     = build_query( "overview_minute_s3app_api" , "[S3Application].dbo." +rating_data_table_iptv  , iptv_channel_id , date , null , null , date_arr  , null ,null , 1 , start_datetime_string , end_datetime_string );
    // console.log($query_IPTV_s3pp.temptable);return;
    let $query_count_all_devicesregister = count_deviceregister( end_datetime_string );
 
 
    /**========== (eof) step(1) : query builder ==================================  */
    /**========== step(2) :execute query ==================================  */
    rating_data_daily_satalite = await pool.request()
    .query($query_satalite_minute.temptable);
    rating_data_daily_iptv     = await pool.request()
    .query($query_IPTV_s3pp.temptable);
    let all_devices_register     = await pool.request()
    .query($query_count_all_devicesregister);

    
    // case : query create temp table
    let $query_satalite = build_query( "overview_tvprogram_basetime_satalitereport" , temptableratingdata ,  null , date , null , null , date_arr  , null , null , null , start_datetime_string , end_datetime_string );
   
  
     // case :  create new temp gender
    //  $query_createtempgender = "select * into "+$var_temptable_gender+" from (select r2.devices_id as dvid ,r2.age,r2.gender from "+channel_daily_devices_summary_table+" group by r2.devices_id,r2.gender ,r2.age) r1";  // case : create new temporary table 
    //  //  console.log($query_createtempgender);return; 
    //  let req_tempgender = new sql.Request(pool);
    // await req_tempgender.batch($query_createtempgender);       

    // case :create view 
    let req_tempgender = new sql.Request(pool);
    $query_crateview = " CREATE VIEW "+$var_temptable_gender+" AS  select r2.devices_id as dvid ,r2.age,r2.gender from "+channel_daily_devices_summary_table+" group by r2.devices_id,r2.gender ,r2.age ";
    await req_tempgender.batch($query_crateview);
   
      // case : crete new temp table satalite
    //   $query_createtemptable = "SELECT * INTO " + $var_temptable +" FROM ( "+ $query_satalite.temptable + " ) as r1";  // case : create new temporary table 
    //  let req = new sql.Request(pool);
    //  await req.batch($query_createtemptable);

     $query_crateview = " CREATE VIEW "+$var_temptable+" AS   "+ $query_satalite.temptable + "  ";
    //console.log($query_crateview);return;
     await req_tempgender.batch($query_crateview);

  

    // console.log(temptableiptvdata);return;
  

    // case : crete new temp table iptv
   
    $var_temptable_iptv = `temp_data_iptv_${strtotime}`;

    let condition = { 'view_table' : temptableiptvdata};
    let $query_iptv = build_query( "overview_tvprogram_basetime_iptvreport_s3app" , temptableiptvdata ,  null , date , null , null , date_arr , null , null  , null ,start_datetime_string , end_datetime_string , condition);
 
    // $query_createtemptable_iptv = "SELECT * INTO " + $var_temptable_iptv +" FROM ( "+ $query_iptv.temptable + " ) as r1";  // case : create new temporary table 
    // let reqs3app = new sql.Request(pool);
    // await reqs3app.batch($query_createtemptable_iptv);

    $query_crateview = " CREATE VIEW "+$var_temptable_iptv+" AS   "+ $query_iptv.temptable + "  ";
   // console.log(temptableiptvdata);return;
    await req_tempgender.batch($query_crateview);
 
 


    let condition_obj = { "temp_gender" : $var_temptable_gender};
    
    // case  : (query) group by gender ( satalite )
    let $query_satalite_gender = build_query( "overview_groupby_gender_satalite_statisticapi" ,$var_temptable_gender , channels_id , null , null , null , date_arr, null , $var_temptable  , null , null , null ,condition_obj);

    let query_tempgender_satalite = await req_tempgender.batch($query_satalite_gender.temptable);
    satalite_gender = query_tempgender_satalite.recordsets[0];
    
    let $query_iptv_gender;
    $query_iptv_gender = build_query( "overview_groupby_gender_iptv_api" ,null , iptv_channel_id , null , null , null , date_arr, null , $var_temptable_iptv , null , null , null ,condition_obj);
    let query_tempgender_iptv = await req_tempgender.batch($query_iptv_gender.temptable);
    iptv_gender = query_tempgender_iptv.recordsets[0];
    

    /**======================== case : query get record set province  =====================================================  */
    // case : (query) group by province region satalite data
    let $query_satalite_addr = build_query( "overview_groupby_addr_satalite_api" ,null , channels_id , null , null , null , date_arr, null , $var_temptable );
    let query_groupbyaddr_satalite = await req_tempgender.batch($query_satalite_addr.temptable);
    province_region_satalite = query_groupbyaddr_satalite.recordsets[0];        
  
    let $query_province_addr = build_query( "overview_groupby_provinceaddr_satalite_api" ,null , channels_id , null , null , null , date_arr, null , $var_temptable );
    let query_groupbyprovinceaddr_satalite = await req_tempgender.batch($query_province_addr.temptable);
    province_addrsatalite = query_groupbyprovinceaddr_satalite.recordsets[0];  
    
   // case : (query) group by province region iptv data
   let $query_iptv_addr = build_query( "overview_groupby_addr_iptv" ,null , iptv_channel_id , null , null , null , date_arr, null , $var_temptable_iptv );                
   let query_groupbyaddr_iptv = await req_tempgender.batch($query_iptv_addr.temptable);
   province_region_iptv = query_groupbyaddr_iptv.recordsets[0];
 

   // case : (query) group by province  iptv data
   let $query_provinceiptv_addr = build_query( "overview_groupby_provinceaddr_iptv" ,null , iptv_channel_id , null , null , null , date_arr, null , $var_temptable_iptv );                
   let query_groupbyprovinceaddr_iptv = await req_tempgender.batch($query_provinceiptv_addr.temptable);
   province_addriptv = query_groupbyprovinceaddr_iptv.recordsets[0];

   let reqs3rating_daily = new sql.Request(pool);

     // case : (query) get all province
     let $query_getallprovinces = build_query( "get_provinces" , null ,  null , null , null , null , null ); 
     let execute_queryprovince = await reqs3rating_daily.batch($query_getallprovinces.rawquery);
    let provinces_all= execute_queryprovince.recordset;


    // let condition_obj = { "temp_gender" : $var_temptable_gender};
    // case  : (query) group by age ( satalite )
    let $query_satalite_age   = build_query( "overview_groupby_age_satalite" ,null , channels_id , null , null , null , date_arr, null , $var_temptable  , null , null, null , condition_obj);
    //  let query_tempgender_age  = await req_tempgender.batch($query_satalite_age.temptable);
  
    let query_tempgender_age  = await reqs3rating_daily.batch($query_satalite_age.temptable);
    satalite_age = query_tempgender_age.recordsets[0];
    // case  : (query) group by gender ( iptv )
    let $query_iptv_age;
    $query_iptv_age = build_query( "overview_groupby_age_iptv" ,null , null , null , null , null , date_arr, null , $var_temptable_iptv , null ,null , null , condition_obj);
    // console.log($query_iptv_age.temptable);return;
    // let query_tempage_iptv = await req_tempgender.batch($query_iptv_age.temptable);
    let query_tempage_iptv = await reqs3rating_daily.batch($query_iptv_age.temptable);
    iptv_age = query_tempage_iptv.recordsets[0];


     /**======================== (eof) case : query get record set province  =====================================================  */


    let merge_data_gender_obj  = merge_data_gender(satalite_gender , iptv_gender);
 
    /**========== (eof) step(2) :execute query ==================================  */
    
    /**==================== step(3) :get record set  ==================================  */
    let rating_data_daily_obj = rating_data_daily_satalite.recordsets[0];
    var rating_data_satalite_obj = JSON.parse(JSON.stringify(rating_data_daily_obj));
    let rating_data_daily_iptv_obj = rating_data_daily_iptv.recordsets[0];
    var rating_data_iptv_obj = JSON.parse(JSON.stringify(rating_data_daily_iptv_obj));
    let all_devices_register_obj = all_devices_register.recordsets[0];
    var $count_all_devicesregister_obj = JSON.parse(JSON.stringify(all_devices_register_obj));
    // console.log(rating_data_satalite_obj);return;
    /**==================== step(3) :get record set  ==================================  */
    let total_devices_register = $count_all_devicesregister_obj[0].all_devices;

     /** ================ case : step (3.1)  combine age data from satalite , iptv ==========================   */
  let merge_data_age_obj  = merge_data_age_overview(satalite_age , iptv_age); // merge provice object
  //console.log(merge_data_age_obj);
  // let path_age          = save_log( merge_data_age_obj, "logage_"  , dir_jsonlocate ); // save log : province all
  /** ===================================================================================  */

    /** =================  step(4) : drop temp table ========================================= */
    await req_tempgender.batch( "drop view " + $var_temptable ); // case : drop temp table
    await req_tempgender.batch( "drop view " + $var_temptable_iptv ); // case : drop temp table
    await req_tempgender.batch(" drop view " + $var_temptable_gender);
    
    await req_tempgender.batch( "drop view " + view_satalite ); // case : drop temp table
    await req_tempgender.batch( "drop view " + view_iptv ); // case : drop temp table
 
    
 

    
    /** =================  step(4) : ( eof ) drop temp table ========================================= */
    /**==================== step(5) :prepare data for return it to front ==================================  */
   
       
    /**===================== step(5.1)  merge data province ========================  */
         let merge_data_provincegroup_obj  = merge_data_province_overview(province_addrsatalite , province_addriptv); // merge provice object
      //  console.log(merge_data_provincegroup_obj);return;                          
        let merge_data_region_obj   = merge_data_regionoverview(province_region_satalite , province_region_iptv, "element.province_region"); // get all province region
        // let path_regionall          = save_log( merge_data_region_obj, "logprovinceall_channelapi"  , dir_jsonlocate ); // save log : province all  
        // var path_merge_province     = save_log( merge_data_provincegroup_obj, "logprovincemergedata_channelapi_"  , dir_jsonlocate );
        let sum_array_province_all  = merge_data_provincegroup_obj;

       // let init_set_arrayprovince_api   = set_arrayprovince_api(sum_array_province_all , provinces_all);
       // console.log(init_set_arrayprovince_api);
    /**===================== step(5.1) (eof) merge data province ========================  */

    let merge_data_obj    =  merge_andsortallofthisfuck(rating_data_satalite_obj , rating_data_iptv_obj ); // case : merge top20channel satalite and iptv
    merge_data_obj        =  resort_array_data( merge_data_obj  , datestring  , merge_data_gender_obj ,  sum_array_province_all  , provinces_all , channels_id  , merge_data_age_obj);


    let all_devices       = merge_data_obj.reduce((total, obj) => obj.reach_device + total,0);
    // console.log(all_devices);
    let rating  = all_devices > 0 ?   (all_devices / total_devices_register) * 100 : 0;
     /**==================== (eof) step(5) :prepare data for return it to front ==================================  */


    if (merge_data_obj != null) {
            if(is_fake_val){
                j = JSON.stringify(merge_data_obj);
                j = j.replace(/2023-08-19/g,"2023-08-26");
                merge_data_obj = JSON.parse(j);
            }
          
            return [{
                "status": true,
                "channel_id" : channels_id,
                "channel_name"  : get_channelname_api(channels_id),
                "cumulative_views" : all_devices,
                "rating" : round_xdecimalplace(rating , 4),
                 data: merge_data_obj,
                "result_code": "200",
                "result_desc": "Success"
            }];       
    }else{
        return [{
            "status": false,
            "result_code": "400",
            "result_desc": "data not found."
        }];
    }

    

}
const removeSpaces = str => str.replace(/\s/g, '');

function has_white_space(s) {
    return (/\s/).test(s);
  }
function resort_array_data(merge_data , datestring , merge_data_gender_obj = null , sum_array_province_all = null , provinces_all = null , channels_id = null , merge_data_age_obj = null){
    let return_data = [];
    merge_data.forEach(element => {
        // console.log(element);
        let arr_data = {};
        let hour  = element.hour;
        let minute=element.minute;
        let next_minute = parseInt(minute) + 1;
        let minute_string = prefix_timezero(minute);
        if(has_white_space(minute_string)){
            minute_string   = removeSpaces(minute_string);
        }
        let nextminute_string = prefix_timezero(next_minute);
        if(has_white_space(nextminute_string)){
          nextminute_string = removeSpaces(nextminute_string);
        }
        let hour_string   =  prefix_timezero(hour);
        let startdatetime_string =  datestring  + " " + hour_string +":"+minute_string+":00";
        let enddatetime_string   =  datestring  + " " + hour_string +":"+nextminute_string+":00";
        let value =  "element.field_" + hour + "_" + minute;
        let val   = eval(value);
        // arr_data["hour"]   = parseInt(hour);
        arr_data["datetime"] = startdatetime_string+ " to " +enddatetime_string ;
        // arr_data["minute"]   =  minute;
        /** ================== gender ==================================  */
        $field =`field_${hour}_${minute}`;
        $gender      = get_totalgender(merge_data_gender_obj , $field);
        $gender_male   = $gender.male > 0 ? $gender.male : 0;
        $gender_female = $gender.female > 0 ? $gender.female : 0;
        $gender_none   = $gender.none > 0 ? $gender.none : 0;
        arr_data["gender"] = {};
        arr_data["gender"]["male"] = $gender_male;
        arr_data["gender"]["female"] = $gender_female;
        arr_data["gender"]["none"] = $gender_none;
        // array age
        if(merge_data_age_obj != null){
            /** ======================== case : step ( 2 ) : set array age  ==========================================  */
            let send_back_arrage=  init_setpropertyobject_age( merge_data_age_obj  , $field );
            let send_back_arr_age_notnull     =  send_back_arrage[0];
            let send_back_arr_age_age0        =  send_back_arrage[1];
            // console.log(send_back_arrage);
           /** ================== case : เพิ่มอายุที่ไม่สามารถระบุได้ (age มีค่าเป็น0) =======================  */
            if(send_back_arr_age_age0[channels_id] != undefined){
                if(send_back_arr_age_age0[channels_id].length > 0){
                    send_back_arr_age_notnull[channels_id].push(send_back_arr_age_age0[channels_id][0]);
                }
            }
            /** ================== (eof)case : เพิ่มอายุที่ไม่สามารถระบุได้  ===================  */

            arr_data['age']           = send_back_arr_age_notnull[channels_id];
            /** ======================== case : step ( 2 ) : set array age  ==========================================  */
        }
        
        /** ================== (eof) gender ============================ */
        /**==================== province =================================  */
        let province_arr = set_arrayprovince_api(sum_array_province_all , provinces_all , $field  , channels_id);
        arr_data["province"] = province_arr[channels_id];
        /**=================================================================== */

        
        arr_data["reach_device"]  = val > 0 ? val :  0;
        return_data.push(arr_data);
    });
  
    // return_data["data"].sort((a, b) => a.minute - b.minute);
    // console.log(return_data);
    return return_data;
}
function prefix_timezero(hhmmss){
    let data =  hhmmss < 10 ? "0"+hhmmss : hhmmss;
    return data;

}

function get_channelname_api(key = null){

    let channel ={};
    // channel[257] = "TNN";
    // channel[263] = "Nation";
    // channel[271] = "MCOT";
    // channel[273] = "Thairath TV";
    // channel[277] = "PPTV";
    // channel[252] = "TPBS";
    // channel[525] = "ALTV";

    channel[463] = "CH-3";
    channel[525] = "ALTV";
    channel[271] = "CH-9";
    channel[413] = "CH-5";
    channel[416] = "CH-7";
    channel[251] = "NBT";
    channel[268] = "Channel8";
    channel[264] = "Workpoint";
    channel[272] = "One 31";
    channel[277] = "PPTV";
    channel[270] = "Mono 29";
    channel[275] = "Amarin TV";
    channel[263] = "Nation";
    channel[257] = "TNN";
    channel[273] = "Thairath TV";
    channel[266] = "JKN18";
    channel[259] = "GMM25";
    channel[265] = "True4U";
    channel[568] = "T-Sport";
    channel[252] = "Thai PBS";

    //  channel[525] = "ALTV";

    return channel[key];

}

function get_tvchannel_id_top5statisticapi($iptv_id = null){

    let channel ={};
    // channel[45] = 257;
    // channel[39] = 263;
    // channel[65] = 271;
    // channel[69] = 273;
    // channel[76] = 277;
    // channel[64] = 252;
    // channel[225] = 525;
    channel[66] = 463;
    channel[225] = 525;
    channel[65] = 271;
    channel[62] = 413;
    channel[67] = 416;
    channel[63] = 251;
    channel[3] = 268;
    channel[2] = 264;
    channel[48] = 272;
    channel[76] = 277;
    channel[73] = 270;
    channel[74] = 275;
    channel[39] = 263;
    channel[45] = 257;
    channel[69] = 273;
    channel[70] = 266;
    channel[71] = 259;
    channel[68] = 265;
    channel[272] = 568;
    channel[64] = 252;

    

    

    

    
    if(channel[$iptv_id] != undefined){
        return channel[$iptv_id];
    }else{
        return channel;
    }
}

function get_channel_id_top5statisticapi($channel_id = null){

    let channel ={};
    
    channel[463] = 66;
    channel[525] = 225;
    channel[271] = 65;
    channel[413] = 62;
    channel[416] = 67;
    channel[251] = 63;
    channel[268] = 3;
    channel[264] = 2;
    channel[272] = 48;
    channel[277] = 76;
    channel[270] = 73;
    channel[275] = 74;
    channel[263] = 39;
    channel[257] = 45;
    channel[273] = 69;
    channel[266] = 70;
    channel[259] = 71;
    channel[265] = 68;
    channel[568] = 272;
    channel[252] = 64;
    
    
    // channel[257] = 45;
    // channel[263] = 39;
    // channel[271] = 65;
    // channel[273] = 69;
    // channel[277] = 76;

    // channel[252] = 64;
    // channel[525] = 225;

    
    if(channel[$channel_id] != undefined){
        return channel[$channel_id];
    }else{
        return channel;
    }
}
function set_newarr_province_statisticaapi(){
    let new_arr_statistic_api = {};
    // new_arr_statistic_api[257] = [];
    // new_arr_statistic_api[263] = [];
    // new_arr_statistic_api[271] = [];
    // new_arr_statistic_api[273] = [];
    // new_arr_statistic_api[277] = [];
    // new_arr_statistic_api[252] = [];
    // new_arr_statistic_api[525] = [];

    new_arr_statistic_api[463] = [];
    new_arr_statistic_api[525] = [];
    new_arr_statistic_api[271] = [];
    new_arr_statistic_api[413] = [];
    new_arr_statistic_api[416] = [];
    new_arr_statistic_api[251] = [];
    new_arr_statistic_api[268] = [];
    new_arr_statistic_api[264] = [];
    new_arr_statistic_api[272] = [];
    new_arr_statistic_api[277] = [];
    new_arr_statistic_api[270] = [];
    new_arr_statistic_api[275] = [];
    new_arr_statistic_api[263] = [];
    new_arr_statistic_api[257] = [];
    new_arr_statistic_api[273] = [];
    new_arr_statistic_api[266] = [];
    new_arr_statistic_api[259] = [];
    new_arr_statistic_api[265] = [];
    new_arr_statistic_api[568] = [];
    new_arr_statistic_api[252] = [];
    return new_arr_statistic_api;
}
function set_newarr_age_statisticaapi(){
    let new_arr_statistic_api = {};
    // new_arr_statistic_api[257] = [];
    // new_arr_statistic_api[263] = [];
    // new_arr_statistic_api[271] = [];
    // new_arr_statistic_api[273] = [];
    // new_arr_statistic_api[277] = [];
    //  new_arr_statistic_api[252] = [];
    //   new_arr_statistic_api[525] = [];
    new_arr_statistic_api[463] = [];
    new_arr_statistic_api[525] = [];
    new_arr_statistic_api[271] = [];
    new_arr_statistic_api[413] = [];
    new_arr_statistic_api[416] = [];
    new_arr_statistic_api[251] = [];
    new_arr_statistic_api[268] = [];
    new_arr_statistic_api[264] = [];
    new_arr_statistic_api[272] = [];
    new_arr_statistic_api[277] = [];
    new_arr_statistic_api[270] = [];
    new_arr_statistic_api[275] = [];
    new_arr_statistic_api[263] = [];
    new_arr_statistic_api[257] = [];
    new_arr_statistic_api[273] = [];
    new_arr_statistic_api[266] = [];
    new_arr_statistic_api[259] = [];
    new_arr_statistic_api[265] = [];
    new_arr_statistic_api[568] = [];
    new_arr_statistic_api[252] = [];
    return new_arr_statistic_api;
}

function set_province_child_dict(){
    let child_dict = {};
    child_dict['province_code'] = '';
    child_dict['province_name'] = '';
    child_dict['latitude']      = '';
    child_dict['longitude']     = '';
    child_dict["reach_device"]  = '';
    return child_dict;
}
// case : set property of object -> age
function init_setpropertyobject_age(merge_data_age_obj = null , $field = null){
    let send_back_arrage      = set_newarr_age_statisticaapi();
    let $age_key_none = set_newarr_age_statisticaapi();
    let sum_rechdevicetest = 0;
    merge_data_age_obj.forEach(element => {
           

        let channels_id = element.channels_id;
        let $reach_devices = eval("element." + $field);
        $reach_devices     = parseInt($reach_devices) > 0  ? $reach_devices : 0;
        if(channels_id == 263 ){
            sum_rechdevicetest += $reach_devices;
        }
        if(send_back_arrage[channels_id] != undefined){ // case : check channel id != undefind 
            let push_array  = {};
            if(element.age > 0 && element.age != 10000 && element.age != 10001 && element.age != 10001){  // condition  : if check age > 0 && age != null (age =  10000) , age != ''  (age = 10001) , age != '-' (age =10002) 
                push_array['age'] =  element.age;
                push_array["reach_device"] =$reach_devices;
                send_back_arrage[channels_id].push(push_array);
            }else{ // condition  : if check age == 0  then
               
                if($age_key_none[channels_id][0] != undefined){ // condition  : if check age == 0  then
               
                    $age_key_none[channels_id][0]['reach_device'] += $reach_devices;
                }else{
                    push_array['age'] =  "ระบุไม่ได้";
                    push_array["reach_device"] =$reach_devices;
                    $age_key_none[channels_id].push(push_array);
                   
                }
            }// (eif ) condition  : if check age == 0  then
         } // (eif) case : check channel id != undefind 

    });

    // console.log(sum_rechdevicetest);
    // console.log("--------------");

    return [ send_back_arrage , $age_key_none ];
}

function set_arrayprovince_api(sum_array_province_all ,provinces_all , $field , _channels_id){
    /** ======================== case : step (1 ) : set array province  ==========================================  */
    let send_back_arrprovince = {};
    send_back_arrprovince[_channels_id] = [];
   // let prefix_hour = h_m; // get : prefix hour
   // $field =`field_${prefix_hour}`;
    sum_array_province_all.forEach(element => {
       
        let found_key=  Object.keys(provinces_all).find(key => provinces_all[key].province_code == element['region_code']);
        let channels_id = element.channels_id;
        let region_code =element['region_code'];
        if(found_key >= 0 && found_key != undefined && send_back_arrprovince[channels_id] != undefined){
           // new_arr[element.channels_id] = {};
           if(region_code >= 0){
                let push_array  = {};
                push_array['province_code'] = region_code;
                push_array['province_name'] = element['province_name'];
                push_array['latitude']      = provinces_all[found_key].latitude;
                push_array['longitude']     = provinces_all[found_key].longitude;
                push_array["reach_device"] =eval("element." + $field);
                send_back_arrprovince[channels_id].push(push_array);
            }
        }
    });
     /** ======================== (eof) case : step (1 ) : set array province  ==========================================  */
     return send_back_arrprovince;
}
   function set_returnobject($report_type = null , date_arr = null , arr_data = null , merge_gender = null 
    , timestart = null , timeend = null
    , provinces_all = null , sum_array_province_all = null
    , path_regionall = null , path_merge_province = null , merge_data_age_obj = null ){
       
        let send_back_arr = [];
        // let send_back_arrprovince = [];
        let prefix_hour = date_arr[0][0][2]; // get : prefix hour
        $field =`field_${prefix_hour}`;
        
        /** ======================== case : step (1 ) : set array province  ==========================================  */
        let send_back_arrprovince = set_newarr_province_statisticaapi();
       
        sum_array_province_all.forEach(element => {
           
            let found_key=  Object.keys(provinces_all).find(key => provinces_all[key].province_code == element['region_code']);
            let channels_id = element.channels_id;
            let region_code =element['region_code'];
            if(found_key >= 0 && found_key != undefined && send_back_arrprovince[channels_id] != undefined){
               // new_arr[element.channels_id] = {};
               if(region_code >= 0){
                    let push_array  = {};
                    push_array['province_code'] = region_code;
                    push_array['province_name'] = element['province_name'];
                    push_array['latitude']      = provinces_all[found_key].latitude;
                    push_array['longitude']     = provinces_all[found_key].longitude;
                    push_array["reach_device"] =eval("element." + $field);
                    send_back_arrprovince[channels_id].push(push_array);
                }
            }
        });
         /** ======================== (eof) case : step (1 ) : set array province  ==========================================  */
        
         /** ======================== case : step ( 2 ) : set array age  ==========================================  */
        let send_back_arrage=  init_setpropertyobject_age( merge_data_age_obj  , $field );
        let send_back_arr_age_notnull     =  send_back_arrage[0];
        let send_back_arr_age_age0        =  send_back_arrage[1];

        /** ======================== case : step ( 2 ) : set array age  ==========================================  */
       
        if($report_type == "statistics"){
            //console.log(merge_gender);
             arr_data.forEach(element => {
                let return_data = {};
                var channels_id  = element.channels_id;
                
                // $field =`field_${prefix_hour}`;
                // case :  เพศ
                var merge_gender_obj = lodash.filter(merge_gender, x => x.channels_id == channels_id);   // case : get all object (multiple key ) by key value
                $gender      = get_totalgender(merge_gender_obj , $field);
                $gender_male   = $gender.male > 0 ? $gender.male : 0;
                $gender_female = $gender.female > 0 ? $gender.female : 0;
                $gender_none   = $gender.none > 0 ? $gender.none : 0;

                return_data["channels_id"]   = channels_id;
                return_data["channel_name"]  = get_channelname_api(channels_id);
                return_data["period"]        = timestart + "-" + timeend;
                return_data["reach_device"] =eval("element." + $field);
                return_data["gender"] = $gender;
                return_data['provinces']     = send_back_arrprovince[channels_id];

                /** ================== case : เพิ่มอายุที่ไม่สามารถระบุได้ (age มีค่าเป็น0) =======================  */
                if(send_back_arr_age_age0[channels_id] != undefined){
                if(send_back_arr_age_age0[channels_id].length > 0){
                    send_back_arr_age_notnull[channels_id].push(send_back_arr_age_age0[channels_id][0]);
                }
                }
                /** ================== (eof)case : เพิ่มอายุที่ไม่สามารถระบุได้  ===================  */

                return_data['age']           = send_back_arr_age_notnull[channels_id];
                return_data["total_devices"] = element.total_devices;
              

                send_back_arr.push(return_data);
              
            });
       }
        
       //console.log(send_back_arr);
       return send_back_arr;
   }

   function dateReturn(date){
    var DateCast = new Date(Date.parse(date));
    var year = DateCast.getFullYear();
    var month = DateCast.getMonth() + 1;
    var day = DateCast.getDate();
    var hours = DateCast.getHours();
    var minutes = DateCast.getMinutes();
    var seconds = DateCast.getSeconds();

    hours =  hours < 10 ? "0"+hours : hours;
    minutes =  minutes < 10 ? "0"+minutes : minutes;
    seconds =  seconds < 10 ? "0"+seconds : seconds;

    if(year == '2023' && month =='8' && day =='26'){
        return '2023-08-19' +" "+ hours + ":" + minutes + ":" + seconds;
    }
    return 0;

   }
   async function tvprogram(start_datetime , end_datetime  , channels_id  = null , program_id =  null ){

        let pool = await sql.connect(config);

        var is_fake_val = false;
        var startdatetimeCast = dateReturn(start_datetime);
        var enddatetimeCast = dateReturn(end_datetime);
        if(startdatetimeCast != 0){
            is_fake_val = true;
            start_datetime = startdatetimeCast;
        }
        if(enddatetimeCast != 0){
            end_datetime = enddatetimeCast;
        }


        let $query  = get_tvprogram_specificdatetime(start_datetime , end_datetime  , channels_id , program_id);
        let tvprogram = await pool.request()
            .query($query);
        if (tvprogram != null) {
            let tvprogram_obj = tvprogram.recordsets;

            var j = JSON.stringify(tvprogram_obj);
            if(is_fake_val){
                j = j.replace(/2023-08-19/g,"2023-08-26");
            }
            var tvprogram_obj_ = JSON.parse(j);
            
            // console.log(tvprogram_obj_);
            return [{
                "status": true,
                data: tvprogram_obj_[0],
                "result_code": "200",
                "result_desc": "Success"
            }];
        }else {
            return [{
                "status": false,
                "result_code": "400",
                "result_desc": "not found data from server"
            }];
        }

   }
   async function write_excelfile(arr_data , channels  ,tvprogram_recordset_obj , tvprogram_basetime_satalitereport_data_s , tvprogram_basetime_iptvreport_data_s , channels_id , tvprogram_basetime_ytreport_data = null , satalite_gender = null 
    , iptv_gender = null ,province_region_satalite = null  , province_region_iptv = null , datereport = null
    , channeldailyrating_data = null
    , channeldailyrating_data_psitotal = null
    , channeldailyrating_data_othertv = null) {
       
       
        let original_tvprogram_basetime_satalitereport_data = tvprogram_basetime_satalitereport_data_s;
        let original_tvprogram_basetime_iptv_data = tvprogram_basetime_iptvreport_data_s;
        // case : save log file
        let logsatalite    = JSON.stringify(original_tvprogram_basetime_satalitereport_data);
        var logsatalite_fn = 'logsatalite_' + Math.round(+new Date()/1000)+ ".json";
        fs.writeFileSync('./json/daily/'+logsatalite_fn, logsatalite);

        let logiptv    = JSON.stringify(original_tvprogram_basetime_iptv_data);
        var logiptv_fn = 'logiptv_' + Math.round(+new Date()/1000) + ".json";
        fs.writeFileSync('./json/daily/'+logiptv_fn, logiptv);
      
        
        let obj_satalite = require('./json/daily/'+logsatalite_fn);
        let obj_iptv     = require('./json/daily/'+logiptv_fn);
        
        var tvprogram_basetime_satalitereport_data = await find_channeldelete( "satalite" ,obj_satalite ); // list of top twenty digital tv
        var tvprogram_basetime_iptvreport_data     = await find_channeldelete( "iptv" ,obj_iptv );// list of top twenty digital tv
        
        //console.log(tvprogram_basetime_satalitereport_data);
        // let logtop20satalite    = JSON.stringify(tvprogram_basetime_satalitereport_data);
        // var logtop20satalite_ = 'logtop20satalite' + Math.round(+new Date()/1000) + ".json";
        // fs.writeFileSync('./json/daily/'+logtop20satalite_, logtop20satalite);
        let logtop20iiptv    = JSON.stringify(tvprogram_basetime_iptvreport_data);
        var logtop20iiptv_ = 'logtop20iiptv' + Math.round(+new Date()/1000) + ".json";
        fs.writeFileSync('./json/daily/'+logtop20iiptv_, logtop20iiptv);

        let tvprogram_obj          = get_thistvprogram( tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data , channels_id); // case : get only tvprogrm of this channel 
       let merge_data_obj         =  merge_data(tvprogram_basetime_satalitereport_data , tvprogram_basetime_iptvreport_data ); // case : merge top20channel satalite and iptv
       
       let logtop20    = JSON.stringify(merge_data_obj);
       var logtop20_fn = 'logtop20' + Math.round(+new Date()/1000) + ".json";
       fs.writeFileSync('./json/daily/'+logtop20_fn, logtop20);
       
       let merge_data_obj_everychannel  =   merge_data(original_tvprogram_basetime_satalitereport_data , original_tvprogram_basetime_iptv_data ); 
      
      

        let sum_allbaseontime = sum_everychannel_baseontime(merge_data_obj_everychannel); //  sum everychannel base on time slot
        let sum_top20         = sum_everychannel_baseontime(merge_data_obj); // sum  top20  base on timeslot
       
        let merge_data_gender_obj  = merge_data_gender(satalite_gender , iptv_gender);

        let logprovincesatalite    = JSON.stringify(province_region_satalite);
        var logprovincesatalite_fn = 'logprovincesatalite_' + Math.round(+new Date()/1000) + ".json";
        fs.writeFileSync('./json/daily/'+logprovincesatalite_fn, logprovincesatalite);
        
        let logprovinceiptv    = JSON.stringify(province_region_iptv);
        var logprovinceiptv_fn = 'logprovinceiptv_' + Math.round(+new Date()/1000) + ".json";
        fs.writeFileSync('./json/daily/'+logprovinceiptv_fn, logprovinceiptv);
      
        
        let obj_provincesatalite = require('./json/daily/'+logprovincesatalite_fn);
        let obj_provinceiptv     = require('./json/daily/'+logprovinceiptv_fn);

        let merge_data_province_obj= merge_data(obj_provincesatalite , obj_provinceiptv, "element.province_region");
      
    
        var excel = require('excel4node');
        // Create a new instance of a Workbook class
        var workbook = new excel.Workbook();

        // Add Worksheets to the workbook
        var worksheet = workbook.addWorksheet('dailyreport');
        // Create a reusable style
        var style = workbook.createStyle({
            font: {
                color: '#FF0800',
                size: 12
            },
            numberFormat: '$#,##0.00; ($#,##0.00); -'
        });
        var text_style_header = workbook.createStyle({
            font: {
                color: '#000000',
                size: 10
            },alignment: { 
                shrinkToFit: true, 
                wrapText: true
            },
            border: {
                left: {
                    style: 'thin',
                    color: 'black',
                },
                right: {
                    style: 'thin',
                    color: 'black',
                },
                top: {
                    style: 'thin',
                    color: 'black',
                },
                bottom: {
                    style: 'thin',
                    color: 'black',
                },
                outline: false,
            },
        });
        var text_style_header_description_cumalative = workbook.createStyle({
            font: {
                color: '#000000',
                size: 10,
                vertAlign: 'center'
            },alignment: { 
                shrinkToFit: true, 
                wrapText: true,
                horizontal: 'center',
                vertical: 'center'
            },
            fill: {
                type: 'pattern', // the only one implemented so far.
                patternType: 'solid', // most common.
                fgColor: 'FFFF00', // you can add two extra characters to serve as alpha, i.e. '2172d7aa'.
                // bgColor: 'ffffff' // bgColor only applies on patternTypes other than solid.
            },
            border: {
                left: {
                    style: 'thin',
                    color: 'black',
                },
                right: {
                    style: 'thin',
                    color: 'black',
                },
                top: {
                    style: 'thin',
                    color: 'black',
                },
                bottom: {
                    style: 'thin',
                    color: 'black',
                },
                outline: false,
            },
        });
        var text_style = workbook.createStyle({
            font: {
                color: '#000000',
                size: 10
            },alignment: { 
                shrinkToFit: true, 
                wrapText: true
            },
            border: {
                left: {
                    style: 'thin',
                    color: 'black',
                },
                right: {
                    style: 'thin',
                    color: 'black',
                },
                top: {
                    style: 'thin',
                    color: 'black',
                },
                bottom: {
                    style: 'thin',
                    color: 'black',
                },
                outline: false,
            },
        });

        // Set value of cell A1 to 100 as a number type styled with paramaters of style ( แถว , หลัก )
        worksheet.cell(1, 1).string("ข้อมูลรายวัน"+channels[0].channel_name).style(text_style_header);

         worksheet.cell(3, 1).string("วันที่ "+datereport).style(text_style_header);
        worksheet.cell(4, 1).string("MONTH").style(text_style_header);
        worksheet.cell(4, 2).string("WKDAY / WKEND").style(text_style_header);
        worksheet.cell(4, 3).string("DAY NAME").style(text_style_header);
        worksheet.cell(4, 4).string("DAY").style(text_style_header);
        worksheet.cell(4, 5).string("Start").style(text_style_header);
        worksheet.cell(4, 6).string("End").style(text_style_header);
        worksheet.cell(4, 7).string("Description").style(text_style_header);
        worksheet.column(7).setWidth(30);
        worksheet.cell(4, 8).string("ปริมาณการนำเสนอ").style(text_style_header);
        worksheet.cell(4, 9).string("Rank").style(text_style_header);
        worksheet.cell(4, 10).string("เข้าถึงรวม").style(text_style_header);
        worksheet.cell(4, 11).string("PEAK(เวลาที่พีค)").style(text_style_header);
        worksheet.cell(4, 12).string("PEAK(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 13).string("เฉลี่ยต่อเวลานำเสนอ").style(text_style_header);
        worksheet.cell(4, 13).string("เพศชาย(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 15).string("เพศชาย(ต่อเวลานำเสนอ)").style(text_style_header);
        worksheet.cell(4, 14).string("เพศหญิง(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 17).string("เพศหญิง(ต่อเวลานำเสนอ)").style(text_style_header);
        worksheet.cell(4, 15).string("ระบุไม่ได้(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 19).string("ระบุไม่ได้(ต่อเวลานำเสนอ)").style(text_style_header);
     
        // ["กลาง" , 0],
        // ["ตะวันออก" , 0],
        // ["ตะวันออกเฉียงเหนือ" , 0],
        // ["เหนือ" , 0],
        // ["ใต้" , 0],
        // ["ตะวันตก"  , 0],
        // ["ระบุไม่ได้"  , 0],
        // ["กรุงเทพ"  , 0]
        worksheet.cell(4, 16).string("พื้นที่กรุงเทพ(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 21).string("พื้นที่กรุงเทพ(ต่อเวลานำเสนอ)").style(text_style_header);
        worksheet.cell(4, 17).string("พื้นที่ภาคกลาง(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 23).string("พื้นที่ภาคกลาง(ต่อเวลานำเสนอ)").style(text_style_header);
        worksheet.cell(4, 18).string("พื้นที่ภาคตะวันออก(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 25).string("พื้นที่ภาคตะวันออก(ต่อเวลานำเสนอ)").style(text_style_header);
        worksheet.cell(4, 19).string("พื้นที่ภาคตะวันออกเฉียงเหนือ(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 27).string("พื้นที่ภาคตะวันออกเฉียงเหนือ(ต่อเวลานำเสนอ)").style(text_style_header);
        worksheet.cell(4, 20).string("พื้นที่ภาคเหนือ(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 29).string("พื้นที่ภาคเหนือ(ต่อเวลานำเสนอ)").style(text_style_header);
        worksheet.cell(4, 21).string("พื้นที่ภาคใต้(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 31).string("พื้นที่ภาคใต้(ต่อเวลานำเสนอ)").style(text_style_header);
        worksheet.cell(4, 22).string("พื้นที่ภาคตะวันตก(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 33).string("พื้นที่ภาคตะวันตก(ต่อเวลานำเสนอ)").style(text_style_header);
        worksheet.cell(4, 23).string("ระบุไม่ได้(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 35).string("ระบุไม่ได้(ต่อเวลานำเสนอ)").style(text_style_header);

        worksheet.cell(4, 24).string("PSI TOTAL(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 37).string("PSI TOTAL(ต่อเวลานำเสนอ)").style(text_style_header);
        worksheet.cell(4, 25).string("OTHER TV(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 39).string("OTHER TV(ต่อเวลานำเสนอ)").style(text_style_header);
        worksheet.cell(4, 26).string("จำนวนคนที่ดู youtube ทั้งหมดในช่วงเวลานี้(จำนวนเครื่อง)").style(text_style_header);
        // worksheet.cell(4, 41).string("จำนวนคนที่ดู youtube ทั้งหมดในช่วงเวลานี้(ต่อเวลานำเสนอ)").style(text_style_header);
        worksheet.cell(4, 27).string("เรียง 20 ช่องดิจิทัล").style(text_style_header);

        // merge_data_obj
        $column_header_channel_start = 28;
        $channel_array = tvdigitalchannel_ondemand("satalite" , "header");
      
        for (let i = 0; i < $channel_array.length; i++) {
            worksheet.cell(4, $column_header_channel_start).string($channel_array[i]+"(จำนวนเครื่อง)").style(text_style_header);
            ++ $column_header_channel_start;
            // worksheet.cell(4, $column_header_channel_start).string($channel_array[i]+"(ต่อเวลานำเสนอ)").style(text_style_header);
            // ++ $column_header_channel_start;
        }
        /** ======== case : ค่าสะสม 10 นาที ==============================================  */

       // worksheet.cell(3, $column_header_channel_start).string("ค่าสะสม 10นาที").style(text_style_header_description_cumalative);
      
        // const range = workbook.sheet(0).range("AV3:BQ3");
        // range.value("ค่าสะสม 10นาที");
        // range.style({horizontalAlignment: "center", verticalAlignment: "center", })
        // range.merged(true);
        $column_start_cumalative = $column_header_channel_start;
        worksheet.cell(4, $column_header_channel_start).string("PSI TOTAL (จำนวนเครื่อง)").style(text_style_header);
        ++ $column_header_channel_start;
        worksheet.cell(4, $column_header_channel_start).string("OTHER TV (จำนวนเครื่อง)").style(text_style_header);
        ++ $column_header_channel_start;
        for (let j = 0; j < $channel_array.length; j++) {
            worksheet.cell(4, $column_header_channel_start).string($channel_array[j]+"(จำนวนเครื่อง)").style(text_style_header);
            ++ $column_header_channel_start;

        }
        $col_cumultive_end  = $column_header_channel_start - 1;
        worksheet.cell(3, $column_start_cumalative, 3, $col_cumultive_end, true).string("ค่าสะสม 10นาที").style(text_style_header_description_cumalative); // merge cell

        /** ======== (eof ) case : ค่าสะสม 10 นาที ==============================================  */

        // tvprogram_recordset_obj
        $row = 5;
        var arr_quatity_offer = [];
        var shortmonthname = "";
        var wkendwkday = "";
        var dayname    = "";
        var date         = "";
        var summary_starttime = "";
        var summary_endtime   = "";
        var summary_minutediff= 0;
        var arr_summary =  [ [] , ['total_access' ,  0] 
        , ['peak'   , '' ,  0] , ['gender' , 0 , 0 , 0]
        , ['region' ,  0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 ]
        , ['viewer' , 0 , 0 , 0]
        , ['sumtop20' , 0]
        ];
        var dic_summary = {};
        for(arr_sumstart = 28; arr_sumstart < 48; arr_sumstart ++){
            dic_summary[arr_sumstart] = 0;
        }
        // 0 = rank , 1 = เข้าถึงรวม , 2 = peak ในเวลา
        let arr_cumalative      = [];
        let arr_cumalative_psitotal = [];
        let arr_cumalative_othertv =  [];
        let arr_cumalative_minutediff      = [];
        tvprogram_recordset_obj.forEach(element => {
            summary_starttime= get_starttime(element[0]);
            summary_endtime  = get_endtime(element[element.length - 1]);
           
            element.forEach(value => {
            
                 // declare variable : day name
                dayname = get_dayname( value.date );
                 // case :   set month short name
                shortmonthname = get_monthshortname( value.date);
                worksheet.cell($row, 1).string( shortmonthname ).style(text_style);
                // case :   set weekend weekday 
            
                if(dayname == "Sat" || dayname == "Sun"){
                    wkendwkday = "WKEND";
                }else{
                    wkendwkday = "WKDAY";
                }
                worksheet.cell($row, 2).string( wkendwkday ).style(text_style);
                // case :   set day name
                worksheet.cell($row, 3).string( dayname ).style(text_style);
                // case :   set date
                var dt = new Date(value.date);
                date = dt.getDate() +  "/"  + (dt.getMonth() + 1) +  "/"  + dt.getFullYear();
                worksheet.cell($row, 4).string( date ).style(text_style);
             
                // case :   set start
                
                var start =set_timezone(value.start_time, "Asia/Jakarta");
              
                var st_h  = start.getUTCHours();
                st_h =  st_h < 10 ? "0"+st_h : st_h;
                var st_m  = start.getUTCMinutes();
                st_m =  st_m < 10 ? "0"+st_m : st_m;

                start =  st_h + ":" + st_m;
               
                worksheet.cell($row, 5).string( start ).style(text_style);
                // case :   set end
                var end =set_timezone(value.end_time, "Asia/Jakarta");                
                var end_h  = end.getUTCHours();
                end_h =  end_h < 10 ? "0"+end_h : end_h;
                var end_m  = end.getUTCMinutes();
                end_m =  end_m < 10 ? "0"+end_m : end_m;
                end =  end_h + ":" + end_m;

                // channeldailyrating_data
                worksheet.cell($row, 6).string( end ).style(text_style);
                // case :   set description
                var program_name = value.name;
                worksheet.cell($row, 7).string( program_name ).style(text_style);
                // case :   ปริมาณนำเสนอ
       
                var time_diff        = find_timediff(value.end_time , value.start_time);
               
                var value_minutediff = value.minute_diff;
                value_minutediff =  value_minutediff > 0 ?  value_minutediff : 0;
                arr_quatity_offer.push( value_minutediff ); // case : เพิ่มปริมาณนำเสนอ
                worksheet.cell($row, 8).number( value_minutediff ).style(text_style);
                // case :   rank
                var objkey_value =  `tvprogram_obj.field_${value.id}`;
                var no = set_no(tvprogram_obj , value);
                let order_no    =  get_orderno(value , merge_data_obj , channels_id);
                arr_summary[0].push(order_no); // case : push order number to array
                
                worksheet.cell($row, 9 ).number( order_no ).style(text_style);
                // case : เข้าถึงรวม
                var total_access  = eval(objkey_value);
                total_access      = total_access > 0 ? total_access : 0;
                arr_summary[1][1] += total_access;
                worksheet.cell($row, 10 ).number( eval(objkey_value) ).style(text_style);
                
                // case :  peak
                var peak   =  `maxObj.field_${value.id}`;
                var maxObj =   get_peak(value , arr_data);
                var peak_amount =  eval(peak);
                peak_amount     = peak_amount > 0 ? peak_amount : 0;
                if(peak_amount > arr_summary[2][2]){
                    arr_summary[2][1] = maxObj.hour +":"+maxObj.minute;
                    arr_summary[2][2] = peak_amount;
                }
                worksheet.cell($row, 11 ).string( maxObj.hour +":"+maxObj.minute).style(text_style);
                worksheet.cell($row, 12 ).number( peak_amount ).style(text_style);
                
                // case : average
                $field =`field_${value.id}`;
                var avg = get_avg( value , arr_data , $field );
                // worksheet.cell($row, 13 ).number( avg).style(text_style); // เฉลี่ยต่อเวลานำเสนอ
                
                // case :  เพศ
                $total_viewer = eval(objkey_value);
                $gender      = get_totalgender(merge_data_gender_obj , $field);
                $gender_male   = $gender.male > 0 ? $gender.male : 0;
                $gender_female = $gender.female > 0 ? $gender.female : 0;
                $gender_none   = $gender.none > 0 ? $gender.none : 0;

                let _minutediff = value.minute_diff > 0 ?  value.minute_diff  : 0;
                
                $avg_male             = ( 100 * $gender_male ) / $total_viewer;
                $avg_female           = ( 100 * $gender_female ) / $total_viewer;
                $avg_none             = ( 100 * $gender_none ) / $total_viewer;
               
                let $avg_male_comparetime;
                let $avg_female_comparetime;
                let $avg_none_comparetime;

                summary_minutediff += _minutediff; // case : summary minute diff
                if(_minutediff > 0){
                  
                    
                    // $avg_male_comparetime = convertHMS($avg_male_comparetime);
                    $avg_male_comparetime = $gender_male > 0 ? $gender_male / _minutediff : 0;
                    $avg_male_comparetime = round_xdecimalplace($avg_male_comparetime , 2);
                    
                  
                    $avg_female_comparetime = $gender_female > 0 ? $gender_female / _minutediff : 0;
                    $avg_female_comparetime = round_xdecimalplace($avg_female_comparetime , 2);

                   
                    $avg_none_comparetime = $gender_none > 0 ? $gender_none / _minutediff : 0;
                    $avg_none_comparetime = round_xdecimalplace($avg_none_comparetime , 2);
                }else{
                    $avg_male_comparetime =  0;$avg_female_comparetime =  0;$avg_none_comparetime =  0;
                }

                arr_summary[3][1] += $gender_male;
                arr_summary[3][2] += $gender_female;
                arr_summary[3][3] += $gender_none;
                worksheet.cell($row, 13 ).number( $gender_male ).style(text_style);
                // worksheet.cell($row, 15 ).number( $avg_male_comparetime).style(text_style);
                worksheet.cell($row, 14 ).number( $gender_female).style(text_style);
                // worksheet.cell($row, 17 ).number( $avg_female_comparetime).style(text_style);
                worksheet.cell($row, 15 ).number( $gender_none).style(text_style);
                // worksheet.cell($row, 19 ).number( $avg_none_comparetime).style(text_style);
                // case :  ภาค
                $region      = get_totalregion(merge_data_province_obj , $field );
              
                $region_text = "";
                $region_bk = "";
                $region_ct = ""; // กลาง
                $region_e = ""; // ตะวันออก
                $region_nt = ""; // ตะวันออกเฉียงเหนือ
                $region_n = ""; // เหนือ
               
                $region_s = ""; // ใต้
                $region_w = ""; // ตะวันตก
                $region_none= ""; // ระบุไม่ได้
                if($region.length > 0){
                    $region_bk  = $region[7][1];
                    $region_ct  = $region[0][1];
                    $region_e   = $region[1][1];
                    $region_nt  = $region[2][1];
                    $region_n  = $region[3][1];
                    $region_s  = $region[4][1];
                    $region_w  = $region[5][1];
                    $region_none  = $region[6][1];

                    // $avg_bk            = get_avgregion($region_bk , _minutediff , $total_viewer);
                    // $avg_ct            = get_avgregion($region_ct , _minutediff , $total_viewer);
                    // $avg_e             = get_avgregion($region_e , _minutediff , $total_viewer);
                    // $avg_nt            = get_avgregion($region_nt , _minutediff , $total_viewer);
                    // $avg_n             = get_avgregion($region_n , _minutediff , $total_viewer);
                    // $avg_s             = get_avgregion($region_s , _minutediff , $total_viewer);
                    // $avg_w             = get_avgregion($region_w , _minutediff , $total_viewer);
                    // $avg_none          = get_avgregion($region_none , _minutediff , $total_viewer);
                    $avg_bk            = $region_bk > 0 ? $region_bk / _minutediff : 0;
                    $avg_ct            = $region_ct > 0 ? $region_ct / _minutediff : 0;
                    $avg_e             = $region_e > 0 ? $region_e / _minutediff : 0;
                    $avg_nt            = $region_nt > 0 ? $region_nt / _minutediff : 0;
                    $avg_n             = $region_n > 0 ? $region_n / _minutediff : 0;
                    $avg_s             = $region_s > 0 ? $region_s / _minutediff : 0;
                    $avg_w             = $region_w > 0 ? $region_w / _minutediff : 0;
                    $avg_none          = $region_none > 0 ? $region_none / _minutediff : 0;
                    
                    arr_summary[4][1] += $region_bk;
                    arr_summary[4][2] += $region_ct;
                    arr_summary[4][3] += $region_e;
                    arr_summary[4][4] += $region_nt;
                    arr_summary[4][5] += $region_n;
                    arr_summary[4][6] += $region_s;
                    arr_summary[4][7] += $region_w;
                    arr_summary[4][8] += $region_none;


                    worksheet.cell($row, 16 ).number( $region_bk).style(text_style);
                    // worksheet.cell($row, 21 ).number( round_xdecimalplace($avg_bk , 2) ).style(text_style);

                    worksheet.cell($row, 17 ).number( $region_ct).style(text_style);
                    // worksheet.cell($row, 23 ).number( round_xdecimalplace($avg_ct , 2) ).style(text_style);

                    worksheet.cell($row, 18 ).number( $region_e).style(text_style);
                    // worksheet.cell($row, 25 ).number( round_xdecimalplace($avg_e , 2)).style(text_style);

                    worksheet.cell($row, 19 ).number( $region_nt).style(text_style);
                    // worksheet.cell($row, 27 ).number( round_xdecimalplace($avg_nt, 2) ).style(text_style);

                    worksheet.cell($row, 20 ).number( $region_n).style(text_style);
                    // worksheet.cell($row, 29 ).number( round_xdecimalplace($avg_n, 2)).style(text_style);

                    worksheet.cell($row, 21 ).number( $region_s).style(text_style);
                    // worksheet.cell($row, 31 ).number( round_xdecimalplace($avg_s , 2)).style(text_style);

                    worksheet.cell($row, 22 ).number( $region_w).style(text_style);
                    // worksheet.cell($row, 33 ).number( round_xdecimalplace($avg_w , 2)).style(text_style);

                    worksheet.cell($row, 23 ).number( $region_none).style(text_style);
                    // worksheet.cell($row, 35 ).number( round_xdecimalplace($avg_none , 2)).style(text_style);
                }
               
                
               
                
                // case : PSI TOTAL
                let $key_psitotal      =  get_keybyvalue( tvprogram_basetime_satalitereport_data , channels_id);
               
                let $psitotal          = `sum_allbaseontime['${$field}']`;
                $psitotal = eval($psitotal);
                arr_summary[5][1] += $psitotal;
                worksheet.cell($row, 24 ).number( $psitotal  ).style(text_style);
                
                let startdatetime_str = set_starttime(value.start_time );
                let enddatetime_str   = set_endtime(value.end_time);
                
                let arr_push_psitotal = [];
                /**=========================== case : psitotal cumalative ( step 1 ) : prepare data  ==================== */
                if(channeldailyrating_data_psitotal != undefined && channeldailyrating_data_psitotal != null){
                
                    $reach_devices_cumulative_psitotal =`channeldailyrating_data_psitotal[0].${$field}`;
                    $reach_devices_cumulative_psitotal = eval($reach_devices_cumulative_psitotal);
                    arr_push_psitotal.push($reach_devices_cumulative_psitotal);
                  
                }else{
                    arr_push_psitotal.push(0);
                }
                arr_cumalative_psitotal.push(arr_push_psitotal);
                /**=========================== (eof) case : psitotal cumalative ( step 1 ) : prepare data  ==================== */

           
                let arr_push_othertv = [];
                /**=========================== case : othertv cumalative ( step 1 ) : prepare data  ==================== */
                if(channeldailyrating_data_othertv != undefined && channeldailyrating_data_othertv != null){
                                
                    $reach_devices_cumulative_othertv =`channeldailyrating_data_othertv[0].${$field}`;
                    $reach_devices_cumulative_othertv = eval($reach_devices_cumulative_othertv);
                    arr_push_othertv.push($reach_devices_cumulative_othertv);
                
                }else{
                    arr_push_othertv.push(0);
                }
                arr_cumalative_othertv.push(arr_push_othertv);
                /**=========================== (eof) case : psitotal cumalative ( step 1 ) : prepare data  ==================== */

                let avg_psitotal = $psitotal / _minutediff;
            
                // case : OTHER TV 
               // let $key_iptv      =  get_keybyvalue( tvprogram_basetime_iptvreport_data , channels_id);
                let tvchannel_id =  tvprogram_basetime_satalitereport_data[$key_psitotal].merge_s3remote_id;
                var $sumtop_20 = `sum_top20['${$field}']`;
                $sumtop_20     =  eval($sumtop_20);
                let $other_tv  = parseInt($psitotal) - parseInt($sumtop_20);
                arr_summary[5][2] += $other_tv; // other tv
                worksheet.cell($row, 25 ).number( $other_tv  ).style(text_style);
                let avg_other_tv = $other_tv / _minutediff;
                // worksheet.cell($row, 39 ).number( round_xdecimalplace(avg_other_tv , 2)  ).style(text_style);

                // worksheet.cell($row, 38 ).string( ";"  ).style(text_style);
                
                // case : ONLINE (YOUTUBE)
                let yt_total = 0;
                if(tvprogram_basetime_ytreport_data != null){
                    yt_total = set_sum( tvprogram_basetime_ytreport_data ,$field);
                    yt_total = parseInt(yt_total) > 0  ? parseInt(yt_total) : 0;
                    
                }
                arr_summary[5][3] += yt_total; // youtube total
                worksheet.cell($row, 26 ).number( yt_total  ).style(text_style);
                let avg_yt = yt_total / _minutediff;
                // worksheet.cell($row, 41 ).number( round_xdecimalplace(avg_yt , 2)  ).style(text_style);
                // worksheet.cell($row, 40 ).string( ";"  ).style(text_style);
                // case : เรียง 20 ช่องดิจิทัล
                $channel_array_content = tvdigitalchannel_ondemand("satalite" , null); // # find top20 channel from satalite channel id
                $diff_fromothertv      = $psitotal - $other_tv;
                $col_start  = 28;
                var arr_summary_top20 = [];
                let arr_push =  [];
                let arr_push_minutediff =  [];
                 for($x =0;$x < $channel_array_content.length; $x ++){
                    
                     $channel_id_top20 = $channel_array_content[$x];
                   
                    let $key_found     =  get_keybyvalue( merge_data_obj , $channel_id_top20);

                    
                    //console.log("channel_id:"+$channel_id_top20+ " key:" + $key_found);
                    if($key_found >= 0 ){
                       
                        var $v =  `merge_data_obj[${$key_found}].field_${value.id}`;
                        $v     = eval($v);
                        arr_summary_top20.push( eval($v) ); // case : push value in to array for summary data;
                        $v     = $v > 0  ? $v : 0;
                        $get_avgtop20 = $v > 0 ? $v /  _minutediff  : 0;
                        worksheet.cell($row, $col_start ).number( $v).style(text_style);
                        dic_summary[$col_start] += $v; // case : sum dic        
                        ++ $col_start;
                    }else{
                        worksheet.cell($row, $col_start ).number( 0  ).style(text_style);
                        arr_summary_top20.push( 0 ); // case : push value in to array for summary data;
                        dic_summary[$col_start] += 0; // case : sum dic     
                        ++ $col_start;
                    }


                    /** =========================== ค่าสะสม 10 นาที =======================================   */

                     startdatetime_str = set_starttime(value.start_time );
                     enddatetime_str   = set_endtime(value.end_time);
                     arr_push_minutediff.push(_minutediff);
                    let arr_daily_rating = get_keybyvalue_channelsid_byperiodtime(channeldailyrating_data , $channel_id_top20  , startdatetime_str ,enddatetime_str); // find all key that relate this channel
                    // console.log(arr_daily_rating);
                    if((arr_daily_rating == undefined || arr_daily_rating == null) || _minutediff < 10){         
                   
                        let enddatetime_str_add10minute = add_minute(value.start_time , 10); // หาไม่เจอ rating log ให้ + ไป 10 นาที
                        enddatetime_str   = set_endtime(enddatetime_str_add10minute , "fix_59sec");
                        arr_daily_rating = get_keybyvalue_channelsid_byperiodtime(channeldailyrating_data , $channel_id_top20  , startdatetime_str ,enddatetime_str); // find all key that relate this channel
                        
                        $reach_devices_cumulative  = 0;
                        // $reach_devices_cumulative += value_of_ratingtbl > 0 ? value_of_ratingtbl : 0;
                        if(arr_daily_rating == undefined || arr_daily_rating == null){ 
                            $reach_devices_cumulative = 0;
                        }else{
                              
                            // arr_daily_rating.forEach(value_of_ratingtbl => {
                                $reach_devices_cumulative = arr_daily_rating[arr_daily_rating.length - 1].reach_devices > 0 ? arr_daily_rating[arr_daily_rating.length - 1].reach_devices : 0;
                                if($reach_devices_cumulative > 0 ){
                                    $reach_devices_cumulative = ( $reach_devices_cumulative * _minutediff)  / 10;
                                    $reach_devices_cumulative = round_xdecimalplace($reach_devices_cumulative , 0);
                                  
                                }else{
                                    $reach_devices_cumulative = 0;
                                }
                            // });
                        
                        }
                       // console.log($reach_devices_cumulative);
                        arr_push.push($reach_devices_cumulative);
                    }else{
                        if(arr_daily_rating != undefined && arr_daily_rating != null){
                            $reach_devices_cumulative  = 0;
                            // $reach_devices_cumulative += value_of_ratingtbl > 0 ? value_of_ratingtbl : 0;
                            arr_daily_rating.forEach(value_of_ratingtbl => {
                                $reach_devices_cumulative += value_of_ratingtbl.reach_devices > 0 ? value_of_ratingtbl.reach_devices : 0;
                               
                            });
                            arr_push.push($reach_devices_cumulative);
                        }else{
                            arr_push.push(0);
                           //  arr_push_minutediff.push(0);
                        }
                    } // end if arr dailyrating undefined
                  
                      /**==================================================================================== */
                    } // end for loop
                 // console.log(arr_cumalative);
                    arr_cumalative.push(arr_push);
                    arr_cumalative_minutediff.push(arr_push_minutediff);

                 const arr_sum_top20 = arr_summary_top20.reduce(add, 0); 
                 arr_summary[6][1]  += arr_sum_top20;
                 // const sum_top20 = arr_summary_top20.reduce((partialSum, a) => partialSum + a, 0); 
                 worksheet.cell($row, 27 ).number( arr_sum_top20  ).style(text_style);
                ++ $row;
            });
            
           

           
        });
       
      
        let $row_cumulative_start = 5;
        $key_count = 0;
        sum_cumalative = [];
        for(let xx = 0 ; xx < 20; xx ++){
            sum_cumalative[xx] = 0;
        }
        /**====================== วนลูปวานค่าสะสม 10 นาที top20 channel ======================================= */
    
        arr_cumalative.forEach(element_byperiod => {
          
            let $col_cumultive_start =  50;
            $sub_key = 0;
            element_byperiod.forEach(culmalative_value => {
                $cumalative_value_zero = 0;
                if(culmalative_value > 0 ){
                    /** ================== คำนวนจำนวนคนดูสะสมทั้งหมด top 20 ===============  */
                    if(sum_cumalative[$sub_key] != undefined){
                        sum_cumalative[$sub_key]  += parseInt(culmalative_value);
                    }
                        /** ================== (eof )คำนวนจำนวนคนดูสะสมทั้งหมด top 20 ===============  */
                     worksheet.cell($row_cumulative_start, $col_cumultive_start ).number( culmalative_value).style(text_style);
                }else{
                    if($key_count > 0){
                     
                        $cumalative_value_zero =  set_cumalativezero_value(arr_cumalative , $key_count  , $sub_key , arr_cumalative_minutediff , "top20_cumalative" );  // อิงจากระบบหลังบ้าน rating กรณีที่ไม่มีข้อมูลในช่วงเวลานั้นมันจะดึงของช่วงก่อนหน้านั้นมาใช้แทน และ ในอนาคตมาใช้ ( key -1 , key + 1 แล้วค่อยมา / 2)
                            /** ================== คำนวนจำนวนคนดูสะสมทั้งหมด top 20 ===============  */
                        if(sum_cumalative[$sub_key] != undefined){
                            sum_cumalative[$sub_key]  += $cumalative_value_zero;
                        }
                            /** ================== (eof) คำนวนจำนวนคนดูสะสมทั้งหมด top 20 ===============  */
                        worksheet.cell($row_cumulative_start, $col_cumultive_start ).number( $cumalative_value_zero).style(text_style);
                    }
                }
                ++$sub_key;
                ++$col_cumultive_start;
            });

 
            ++ $key_count;
            ++ $row_cumulative_start;
        });
       //  console.log(arr_cumalative_minutediff);
        
    /**====================== (eof) วนลูปวานค่าสะสม 10 นาที top20 channel ======================================= */
    /**====================== วนลูปวาดtotalค่าสะสม 10 นาที psi total  ======================================= */
    sum_cumalative_psitotal = [];
    sum_cumalative_psitotal[0] = 0;
    $key_count = 0;
    let $row_cumulative_start_psitotal = 5;
    arr_cumalative_psitotal.forEach(element_byperiod => {
        $sub_key_psitotal = 0;
        element_byperiod.forEach(culmalative_value => {
            $cumalative_value_zero = 0;
            if(culmalative_value > 0 ){
                /** ================== คำนวนจำนวนคนดูสะสมทั้งหมด top 20 ===============  */
                if(sum_cumalative_psitotal[$sub_key_psitotal] != undefined){
                    sum_cumalative_psitotal[$sub_key_psitotal]  += parseInt(culmalative_value);
                }
                    /** ================== (eof )คำนวนจำนวนคนดูสะสมทั้งหมด top 20 ===============  */
                 worksheet.cell($row_cumulative_start_psitotal, 48 ).number( culmalative_value).style(text_style);
            }else{
                if($key_count > 0){
                 
                    $cumalative_value_zero =  set_cumalativezero_value(arr_cumalative_psitotal , $key_count , $sub_key_psitotal  );  // อิงจากระบบหลังบ้าน rating กรณีที่ไม่มีข้อมูลในช่วงเวลานั้นมันจะดึงของช่วงก่อนหน้านั้นมาใช้แทน และ ในอนาคตมาใช้ ( key -1 , key + 1 แล้วค่อยมา / 2)
                        /** ================== คำนวนจำนวนคนดูสะสมทั้งหมด top 20 ===============  */
                    if(sum_cumalative_psitotal[$sub_key_psitotal] != undefined){
                        sum_cumalative_psitotal[$sub_key_psitotal]  += $cumalative_value_zero;
                    }
                        /** ================== (eof) คำนวนจำนวนคนดูสะสมทั้งหมด top 20 ===============  */
                    worksheet.cell($row_cumulative_start_psitotal, 48 ).number( $cumalative_value_zero).style(text_style);
                }
            }
            ++$sub_key;
    
        });
        ++ $key_count;
        ++ $row_cumulative_start_psitotal;
    });
    /**====================== (eof) วนลูปวานค่าสะสม 10 นาที psi total ======================================= */
       /**====================== วนลูปวาดtotalค่าสะสม 10 นาที othertv total  ======================================= */
       sum_cumalative_othertv = [];
       sum_cumalative_othertv[0] = 0;
       $key_count = 0;
       let $row_cumulative_start_othertv = 5;
       arr_cumalative_othertv.forEach(element_byperiod => {
           $sub_key_othertv = 0;
           element_byperiod.forEach(culmalative_value => {
               $cumalative_value_zero = 0;
               if(culmalative_value > 0 ){
                   /** ================== คำนวนจำนวนคนดูสะสมทั้งหมด othertv===============  */
                   if(sum_cumalative_othertv[$sub_key_othertv] != undefined){
                       sum_cumalative_othertv[$sub_key_othertv]  += parseInt(culmalative_value);
                   }
                       /** ================== (eof )คำนวนจำนวนคนดูสะสมทั้งหมด othertv===============  */
                    worksheet.cell($row_cumulative_start_othertv, 49 ).number( culmalative_value).style(text_style);
               }else{
                   if($key_count > 0){
                    
                       $cumalative_value_zero =  set_cumalativezero_value(arr_cumalative_othertv , $key_count , $sub_key_othertv  );  // อิงจากระบบหลังบ้าน rating กรณีที่ไม่มีข้อมูลในช่วงเวลานั้นมันจะดึงของช่วงก่อนหน้านั้นมาใช้แทน และ ในอนาคตมาใช้ ( key -1 , key + 1 แล้วค่อยมา / 2)
                           /** ================== คำนวนจำนวนคนดูสะสมทั้งหมด othertv===============  */
                       if(sum_cumalative_othertv[$sub_key_othertv] != undefined){
                           sum_cumalative_othertv[$sub_key_othertv]  += $cumalative_value_zero;
                       }
                           /** ================== (eof) คำนวนจำนวนคนดูสะสมทั้งหมด othertv ===============  */
                       worksheet.cell($row_cumulative_start_othertv, 49 ).number( $cumalative_value_zero).style(text_style);
                   }
               }
               ++$sub_key;
       
           });
           ++ $key_count;
           ++ $row_cumulative_start_othertv;
       });
       /**====================== (eof) วนลูปวานค่าสะสม 10 นาที othertv total ======================================= */

        /**==================== วนลูปวาดยอดคนดูสะสม 10 total =======================  */
        $col_cumultive_start =  50;
        // psi total
        sum_cumalative_psitotal.forEach(value_sum_cumalative_psitotal => {
            worksheet.cell($row_cumulative_start, 48 ).number( value_sum_cumalative_psitotal).style(text_style);
        });
        // othertv
        sum_cumalative_othertv.forEach(value_sum_cumalative_othertv => {
            worksheet.cell($row_cumulative_start, 49 ).number( value_sum_cumalative_othertv).style(text_style);
        });
        // top 20
        sum_cumalative.forEach(value_sumtop20cumalative => {
          
            worksheet.cell($row_cumulative_start, $col_cumultive_start ).number( value_sumtop20cumalative).style(text_style);
            ++ $col_cumultive_start;
        });
      
     /**==================== (eof) วนลูปวาดยอดคนดูสะสม total=======================  */


    //    return;
        // case : write last row 
        var  arr_quatityamout_offer = arr_quatity_offer.reduce( add , 0);  // รวมปริมาณนำเสนอ
        worksheet.cell($row, 1 ).string( shortmonthname  ).style(text_style); //  case : สรุปรายงาน
        worksheet.cell($row, 2 ).string( wkendwkday  ).style(text_style); //  case : weekend / weekday
        worksheet.cell($row, 3 ).string( dayname  ).style(text_style); //  case : weekend / weekday
        worksheet.cell($row, 4 ).string( date ).style(text_style); //  case : date
        worksheet.cell($row ,5 ).string(summary_starttime).style(text_style); // case : get start time
        worksheet.cell($row ,6 ).string(summary_endtime).style(text_style); // case : get end time
        worksheet.cell($row, 7 ).string( "SUMMARY" ).style(text_style); //  case : date
        worksheet.cell($row, 8 ).number( arr_quatityamout_offer ).style(text_style); //  case : date
        
        if(arr_summary[0].length > 0){ 
            var sum_orderno = arr_summary[0].reduce((a, b) => a + b, 0);
            var avg_orderno = (sum_orderno / arr_summary[0].length); // || 0
            avg_orderno  = avg_orderno > 0  ? avg_orderno : 0;
            avg_orderno  = round_xdecimalplace(avg_orderno , 0);
            worksheet.cell($row, 9 ).number( avg_orderno).style(text_style); //  case : date
        }
        worksheet.cell($row, 10 ).number( arr_summary[1][1] ).style(text_style); //  case : total access
        worksheet.cell($row, 11 ).string( arr_summary[2][1] ).style(text_style); //  case : peak time
        worksheet.cell($row, 12 ).number( arr_summary[2][2] ).style(text_style); //  case : peak value
        // summary เพศ
        worksheet.cell($row, 13 ).number( arr_summary[3][1] ).style(text_style);
        worksheet.cell($row, 14 ).number( arr_summary[3][2] ).style(text_style);
        worksheet.cell($row, 15 ).number( arr_summary[3][3] ).style(text_style);
         // summary region
         worksheet.cell($row, 16 ).number( arr_summary[4][1] ).style(text_style);
         worksheet.cell($row, 17 ).number( arr_summary[4][2] ).style(text_style);
         worksheet.cell($row, 18 ).number( arr_summary[4][3] ).style(text_style);
         worksheet.cell($row, 19 ).number( arr_summary[4][4] ).style(text_style);
         worksheet.cell($row, 20 ).number( arr_summary[4][5] ).style(text_style);
         worksheet.cell($row, 21 ).number( arr_summary[4][6] ).style(text_style);
         worksheet.cell($row, 22 ).number( arr_summary[4][7] ).style(text_style);
         worksheet.cell($row, 23 ).number( arr_summary[4][8] ).style(text_style);

         // psi total & yt & iptv
         worksheet.cell($row, 24 ).number( arr_summary[5][1] ).style(text_style);
         worksheet.cell($row, 25 ).number( arr_summary[5][2] ).style(text_style);
         worksheet.cell($row, 26 ).number( arr_summary[5][3] ).style(text_style);
        
         // sum all channel
         worksheet.cell($row, 27 ).number( arr_summary[6][1] ).style(text_style);
         // sum top 20 channel
         for (key_column in dic_summary) {
            worksheet.cell($row, key_column ).number( dic_summary[key_column]).style(text_style);
        }

        if(channels_id == 252){
            
        }
        let prefix_channelname = channels_id == 252 ? "TPBS" : "ALTV";
     
        let $prefix_filename  =  prefix_filename(prefix_channelname , datereport);
        $prefix_filename      += "_" + Date.now();
        let $filename  = 'dailyreport_'+$prefix_filename +'.xlsx';
        workbook.write('./excel/daily/' + $filename);
        return $filename;
    }

    
    function get_hour(timeperiods){
        var t =set_timezone(timeperiods, "Asia/Jakarta");
     
        let timeperiod = new Date(t);
        var st_h  = timeperiod.getUTCHours();
        return st_h;
    }
    function get_minute(timeperiods){
        timeperiods =set_timezone(timeperiods, "Asia/Jakarta");
        let timeperiod = new Date(timeperiods);
        var st_m  = timeperiod.getUTCMinutes();


        return st_m;
    }

    function add_minute(date, minutes) {
        var dt = new Date( date );
        return new Date(dt.getTime() + minutes*60000);
    }


    function set_cumalativezero_value(arr_cumalative , $key_count , $sub_key  , arr_cumalative_minutediff  = null, $is_top20channel = null ){
        // อิงจากระบบหลังบ้าน rating กรณีที่ไม่มีข้อมูลในช่วงเวลานั้นมันจะดึงของช่วงก่อนหน้านั้นมาใช้แทน และ ในอนาคตมาใช้
            // if($is_top20channel == null){    // case : กรณีที่ไม่ใช่ 20ช่องดิจิตอล
                    // case : ดึงช่วงก่อนหน้า
                    if(arr_cumalative[$key_count - 1][$sub_key] > 0 ){
                        $cumalative_value_zero += arr_cumalative[$key_count - 1][$sub_key];
                    }else{
                        if(arr_cumalative[$key_count - 2][$sub_key] > 0){
                            $cumalative_value_zero += arr_cumalative[$key_count - 2][$sub_key];
                        }else{
                            $cumalative_value_zero += 0;
                        }
                    }
                    // case : ดึงช่วงหลังจากนี้
                   console.log(arr_cumalative[$key_count + 1]);
                   if(arr_cumalative[$key_count + 1] != undefined && arr_cumalative[$key_count + 2] != undefined){
                    if(arr_cumalative[$key_count + 1][$sub_key] > 0 ){
                        $cumalative_value_zero += arr_cumalative[$key_count + 1][$sub_key];
                    }else{
                        if(arr_cumalative[$key_count + 2][$sub_key] > 0){
                            $cumalative_value_zero += arr_cumalative[$key_count + 2][$sub_key];
                        }else{
                            $cumalative_value_zero += 0;
                        }
                    }
                    $cumalative_value_zero = $cumalative_value_zero > 0 ? $cumalative_value_zero / 2 : 0;
                    if($cumalative_value_zero > 0){
                       $cumalative_value_zero = round_xdecimalplace($cumalative_value_zero , 0);
                    }
                }
            // }else{
              
            //     if(arr_cumalative[$key_count + 1][$sub_key] != undefined){
            //         $cumalative_next10minute = arr_cumalative[$key_count + 1][$sub_key];
            //         $minute_diff  = arr_cumalative_minutediff[$key_count][$sub_key];
                    
            //         // console.log($minute_diff);
            //         if($cumalative_next10minute > 0){
            //             $cumalative_value_zero =    ($cumalative_next10minute * $minute_diff) / 10
            //         }
            //         if($cumalative_value_zero > 0){
            //             $cumalative_value_zero = round_xdecimalplace($cumalative_value_zero , 0);
            //          }
            //     }
            // }
         
            return $cumalative_value_zero;

    }
    function prefix_filename(channel_name , date , type = null ){
       // date = date.split("-").join("_");

        var dt = new Date( date );
        let month = dt.getMonth() + 1;
        month =  month < 10 ? "0"+month : month;
        var d         = dt.getDate();
        d =  d < 10 ? "0"+d : d;

        var date_text = d +  ""  + month +  ""  + dt.getFullYear();

        let filename = channel_name + "_" + date_text;
        return filename;
    }
    function add(accumulator, a) {
        return accumulator + a;
    }

    function convertHMS(value) {
        const sec = parseInt(value, 10); // convert value to number if it's string
        let hours   = Math.floor(sec / 3600); // get hours
        let minutes = Math.floor((sec - (hours * 3600)) / 60); // get minutes
        let seconds = sec - (hours * 3600) - (minutes * 60); //  get seconds
        // add 0 if value < 10; Example: 2 => 02
        if (hours   < 10) {hours   = "0"+hours;}
        if (minutes < 10) {minutes = "0"+minutes;}
        if (seconds < 10) {seconds = "0"+seconds;}
        return hours+':'+minutes+':'+seconds+""; // Return is HH : MM : SS
    }

    function get_avgregion($region ,  _minutediff , $total_viewer){
        let $avg_comparetime;
        $avg             = ( 100 * $region ) / $total_viewer;
        if($avg > 0){
            $avg_comparetime =  ( $avg * _minutediff ) /  100;
            $avg_comparetime =  round_xdecimalplace($avg_comparetime );

            $avg_comparetime = convertHMS( $avg_comparetime );
        }else{
            $avg_comparetime = 0;
        }

        return $avg_comparetime;
    }

    function get_avgtop20($top20 ,  _minutediff , $total_viewer){
        let $avg_top20;
        $avg             = ( 100 * $top20 ) / $total_viewer;
        if($avg > 0){
            $avg_top20 =  ( $avg * _minutediff ) /  100;
            $avg_top20 =  round_xdecimalplace($avg_top20 );

            $avg_top20 = convertHMS( $avg_top20 );
        }else{
            $avg_top20 = 0;
        }

        return $avg_top20;
    }

    function round_xdecimalplace(value, precision) {
  

      var multiplier = Math.pow(10, precision || 0);
      return Math.round(value * multiplier) / multiplier;
        // if(precision == 3){
        //     return Math.round((value + Number.EPSILON) * 1000) / 1000
        // }
        // let val = value > 0 ? parseFloat(value) : 0;
        // return val.toFixed(2);
    }
    function find_timediff(date_future , date_now){
        let date_future_ = new Date(date_future);
        let date_now_    = new Date(date_now);
        
        var seconds = Math.floor((date_future_ - (date_now_))/1000);
        var minutes = Math.floor(seconds/60);
        var hours = Math.floor(minutes/60);
        var days = Math.floor(hours/24);
        
        hours = hours-(days*24);
        minutes = minutes-(days*24*60)-(hours*60);
        seconds = seconds-(days*24*60*60)-(hours*60*60)-(minutes*60);

        hours =  hours < 10 ? "0"+hours : hours;
        minutes =  minutes < 10 ? "0"+minutes : minutes;
        seconds =  seconds < 10 ? "0"+seconds : seconds;

            return hours + ":" + minutes + ":"+seconds;
    }

    function get_totalgender(merge_data_gender_obj = null, $field = null , report_type = null){
       
        let $key_identify_gender_null     =  get_keybyvalue_gender( merge_data_gender_obj , 'none' , report_type );
        let $key_identify_gender_empty    =  get_keybyvalue_gender( merge_data_gender_obj , 'none1' , report_type );
        let $key_identify_gender_dat      =  get_keybyvalue_gender( merge_data_gender_obj , 'none2' , report_type );
        let $key_identify_gender_female   =  get_keybyvalue_gender( merge_data_gender_obj , 'Female' , report_type );
        let $key_identify_gender_femal    =  get_keybyvalue_gender( merge_data_gender_obj , 'Femal' , report_type );
        let $key_identify_gender_male     =  get_keybyvalue_gender( merge_data_gender_obj , 'Male'  , report_type );
        $gender_none  = 0;
        $gender_female= 0;
        $gender_male  = 0;
      
        if($key_identify_gender_null != null){
            let $gender_null          = `merge_data_gender_obj[${$key_identify_gender_null}].${$field}`;
            $gender_none  +=  eval($gender_null);
        }
        if($key_identify_gender_empty != null){
            let $gender_empty         = `merge_data_gender_obj[${$key_identify_gender_empty}].${$field}`;
          
            $gender_none  +=  eval($gender_empty);
        }
        if($key_identify_gender_dat != null){
            let $gender_dat         = `merge_data_gender_obj[${$key_identify_gender_dat}].${$field}`;
            $gender_none  +=  eval($gender_dat);
        }
        if($key_identify_gender_female != null){
            let $gender_female_obj        = `merge_data_gender_obj[${$key_identify_gender_female}].${$field}`;
            $gender_female  +=  eval($gender_female_obj);
        }
        if($key_identify_gender_femal != null){
            let $gender_femal_obj        = `merge_data_gender_obj[${$key_identify_gender_femal}].${$field}`;
            $gender_female  +=  eval($gender_femal_obj);
        }

        if($key_identify_gender_male != null){
            let $gender_male_obj        = `merge_data_gender_obj[${$key_identify_gender_male}].${$field}`;
            $gender_male  +=  eval($gender_male_obj);
        }
        
        return {'male':$gender_male, 'female' : $gender_female ,'none':$gender_none}

    }
    function get_totalregion(merge_data_province_obj , $field ){
       
        let $key_identify_region_null     =  get_keybyvalue_region( merge_data_province_obj , 'none');
        let $key_identify_region_empty    =  get_keybyvalue_region( merge_data_province_obj , 'none1');
        let $key_identify_region_dat      =  get_keybyvalue_region( merge_data_province_obj , 'none2');
        let $key_identify_region_bk       =  get_keybyvalue_region( merge_data_province_obj , 'Bangkok');
        let $key_identify_region_ct       =  get_keybyvalue_region( merge_data_province_obj , 'Central');
        let $key_identify_region_et       =  get_keybyvalue_region( merge_data_province_obj , 'Eastern');
        let $key_identify_region_ne       =  get_keybyvalue_region( merge_data_province_obj , 'NorthEastern');
        let $key_identify_region_nt       =  get_keybyvalue_region( merge_data_province_obj , 'Northern');
        let $key_identify_region_st       =  get_keybyvalue_region( merge_data_province_obj , 'Southern');
        let $key_identify_region_wt       =  get_keybyvalue_region( merge_data_province_obj , 'Western');
        
        $region_none  = 0;
        // $region_arr = [
        //     ["กลาง" , 0],
        //     ["ตะวันออก" , 0],
        //     ["ตะวันออกเฉียงเหนือ" , 0],
        //     ["เหนือ" , 0],
        //     ["ใต้" , 0],
        //     ["ตะวันตก"  , 0],
        //     ["ระบุไม่ได้"  , 0]
        // ];
        $region_arr = [
            ["กลาง" , 0],
            ["ตะวันออก" , 0],
            ["ตะวันออกเฉียงเหนือ" , 0],
            ["เหนือ" , 0],
            ["ใต้" , 0],
            ["ตะวันตก"  , 0],
            ["ระบุไม่ได้"  , 0],
            ["กรุงเทพ"  , 0]
        ];

       
        if($key_identify_region_null != null){
            let $region_null          = `merge_data_province_obj[${$key_identify_region_null}].${$field}`;
            $region_null     =  eval($region_null); 
             $region_arr[6][1] +=  parseInt($region_null)// undefined
            
            //$region_arr[0][1] +=  parseInt($region_null)// undefined
        }
        if($key_identify_region_empty != null){
            let $region_empty         = `merge_data_province_obj[${$key_identify_region_empty}].${$field}`;
          
            $region_empty     =  eval($region_empty); 
            $region_arr[6][1] +=  parseInt($region_empty)// undefined
            //$region_arr[0][1] +=  parseInt($region_empty)// undefined
        }
        if($key_identify_region_dat != null){
            let $region_dat         = `merge_data_province_obj[${$key_identify_region_dat}].${$field}`;
        
            $region_dat     =  eval($region_dat); 
            $region_arr[6][1] +=  parseInt($region_dat)// undefined
            //$region_arr[0][1] +=  parseInt($region_dat)// undefined
        }
        if($key_identify_region_bk != null){
            let $region_bk         = `merge_data_province_obj[${$key_identify_region_bk}].${$field}`;
            //$region_arr[0][1] =  eval($region_bk);  // Central
            $region_bk       =  eval($region_bk); 
            $region_arr[7][1] +=  parseInt($region_bk)// Bangkok
        }
        if($key_identify_region_ct != null){
            let $region_ct         = `merge_data_province_obj[${$key_identify_region_ct}].${$field}`;
            $region_ct       =  eval($region_ct); 
            $region_arr[0][1] +=  parseInt($region_ct)// Central
        }

        if($key_identify_region_et != null){
            let $region_et         = `merge_data_province_obj[${$key_identify_region_et}].${$field}`;
            //$region_arr[1][1] =  eval($region_et);  // eastern

            $region_et       =  eval($region_et); 
            $region_arr[1][1] +=  parseInt($region_et)// eastern
        }
        if($key_identify_region_ne != null){
            let $region_ne         = `merge_data_province_obj[${$key_identify_region_ne}].${$field}`;

            $region_ne       =  eval($region_ne); 
            $region_arr[2][1] +=  parseInt($region_ne)// NorthEastern
        }
        if($key_identify_region_nt != null){
            let $region_nt         = `merge_data_province_obj[${$key_identify_region_nt}].${$field}`;

            $region_nt       =  eval($region_nt); 
            $region_arr[3][1] +=  parseInt($region_nt)// nt
        }
        if($key_identify_region_st != null){
            let $region_st         = `merge_data_province_obj[${$key_identify_region_st}].${$field}`;
            $region_st       =  eval($region_st); 
            $region_arr[4][1] +=  parseInt($region_st)// st
        }
        if($key_identify_region_wt != null){
            let $region_wt         = `merge_data_province_obj[${$key_identify_region_wt}].${$field}`;
            $region_wt       =  eval($region_wt); 
            $region_arr[5][1] +=  parseInt($region_wt)// wt
        }
        
      
        
        return $region_arr;

    }
    function get_iptvtotal(tvchannel_id , tvprogram_basetime_iptvreport_data , $field){
        let $iptv_total;
        
        if(typeof tvchannel_id == 'number'){
        
            let $key_iptv = get_keybyvalue( tvprogram_basetime_iptvreport_data ,tvchannel_id  , "iptv");
            $iptv_total = `tvprogram_basetime_iptvreport_data[${$key_iptv}].${$field}`;
            $iptv_total = eval($iptv_total);
            $iptv_total = parseInt($iptv_total);
         
        }else{
           
            $iptv_total = 0;
        }

        return $iptv_total;
    }
    function get_sum(arr , key){
        var mk = "obj." + key;
      
        var sum = arr.reduce((total, obj) => eval(mk) + total,0);
        return sum;
        //return arr.reduce((accumulator, current) => accumulator + Number(eval(mk)), 0)
    }


    function get_avg(value , arr_data , $field){
        var sum = set_avg( arr_data , $field);
        var avg = sum > 0 ? (sum / value.minute_diff) : 0;
        avg = avg > 0  ? Math.round(avg) : 0;

        return avg;
    }
    function get_peak(value , arr_data){
        var max_field_compare =  `max.field_${value.id}`;
        var obj_field_compare =  `obj.field_${value.id}`;
        let maxObj = arr_data.reduce((max, obj) => (eval(max_field_compare) > eval(obj_field_compare)) ? max : obj);
       

        return maxObj;
    }
    function get_orderno(value , merge_data_obj ,channels_id){

        var var_merge_b = `b.field_${value.id}`;
        var var_merge_a = `a.field_${value.id}`;
        let sort_desc   =  merge_data_obj.sort((a, b) => eval(var_merge_b) - eval(var_merge_a));
        let order_no    =  get_keybyvalue( sort_desc , channels_id);
        order_no = parseInt(order_no) > 0 ? parseInt(order_no) + 1 : 0;

        return order_no;
    }

     function set_avg(arr , key){
        var mk = "obj." + key;
    
        var sum = arr.reduce((total, obj) => eval(mk) + total,0);
        return sum;
        //return arr.reduce((accumulator, current) => accumulator + Number(eval(mk)), 0)
    }

    function set_sum(arr , key){
        var mk = "obj." + key;
    
        var sum = arr.reduce((total, obj) => eval(mk) + total,0);
        return sum;
        //return arr.reduce((accumulator, current) => accumulator + Number(eval(mk)), 0)
    }
    function set_no(tvprogram_obj , value){
        objkey_number = 1;
        for(property in tvprogram_obj){
    
            if(property == "field_"+value.id){
                no = objkey_number;
            }
            ++ objkey_number;
        }

        return no;
    }
    function set_timezone(date , tzString){
        return new Date((typeof date === "string" ? new Date(date) : date).toLocaleString("en-us", {
            timeZone: tzString
        }));
          
          
    }

   
    function get_dayname(dateString){
        var days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
        var d = new Date(dateString);
        var dayName = days[d.getDay()];
        return dayName;
    }

    function get_monthshortname(d){
        const month = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
        var d =  new Date(d);
        let name = month[d.getMonth()];
        return name;
    }
    function get_tvprogram(channels_id , date){
        $query = `select  DATEDIFF(MINUTE, start_time , end_time) as minute_diff,* from channel_programs_baseon_date where channels_id =${channels_id} and date = '${date}'and active = 1 order by start_time asc `;
  
        return $query;
    }

    function init_getdatefromdatetime(date , format = null){
        let __d ;
        if(format == null){
            const _date = new Date(date);
            __d = _date.getDate() +'-' + _date.getMonth() + '-' + _date.getFullYear(); 
        }else if(format == 'yymmdd'){
            const _date = new Date(date);
            let _m = _date.getMonth() + 1;
            _m =  _m < 10 ? "0"+_m : _m;
            var _d         = _date.getDate();
            _d =  _d < 10 ? "0"+_d : _d;
            __d = _date.getFullYear() +'-'+ _m +'-' + _d;
        }
        return __d;
    }
    async function get_s3remotechannelid(channel_id , config){
        let pool = await sql.connect(config);
        $query = ` select top 1 * from channels where id =  ${channel_id} `;
        let execute_query = await pool.request()
        .query($query);
        let $result = execute_query.recordsets;
        $result     = JSON.parse(JSON.stringify($result));
        return $result[0][0].merge_s3remote_id;

    }

    async function get_tvprogram_rating_statistic(date = null,channels_id , obj = null , type = null , config){
        let pool = await sql.connect(config);
        let $query = "";
  
        $query = `
                select *from  tvprogram_baseondate_statistics
                where date =  '${date}'
                 AND channels_id = '${channels_id}'
                 `;

      
           
        let execute_query = await pool.request()
            .query($query);
        let $result = execute_query.recordsets;
        $result     = JSON.parse(JSON.stringify($result));
        if( $result[0].length == undefined){
            return 0;
        }else{
            return $result[0];
        }

    };

     async function get_unique_reachdevice(table = null , obj = null , type = null , config){
        let pool = await sql.connect(config);
        let $query = "";
        let s3remote_channel_id = await get_s3remotechannelid( obj.channels_id  , config);
   
        if(type != null){
           
            $query = `
                    select count(*) from  ${table}
                    where startview_datetime >=  '${obj.start_datetime}'
                    and startview_datetime  <= '${obj.end_datetime}'
                    and tvchannels_id =  ${s3remote_channel_id}
                    group by devices_id `;

           
                    
        }else{
            $query = ` select count(*) from ${table}
            where startview_datetime >= '${obj.start_datetime}'
            and startview_datetime  <= '${obj.end_datetime}'
            and channels_id = ${obj.channels_id}
            group by devices_id `;

           
        }
           
        let execute_query = await pool.request()
            .query($query);
        let $result = execute_query.recordsets;
        $result     = JSON.parse(JSON.stringify($result));
        if( $result[0].length == undefined){
            return 0;
        }else{
            return $result[0].length;
        }
     }
    const get_tvprogram_rating = async (start_datetime , end_datetime , channels_id =  null , program_id = null , date = null , config , service_used = null) => {
   // async function get_tvprogram_rating(start_datetime , end_datetime , channels_id =  null , program_id = null , date = null , pool){
        let pool = await sql.connect(config);
    

        $query = `
         select AVG(channel_daily_rating_logs.rating) as avg_rating , SUM(channel_daily_rating_logs.reach_devices) as reach_devices
         from channel_daily_rating_logs  join channel_daily_rating on  channel_daily_rating_logs.channel_daily_rating_id = channel_daily_rating.id
         where  channel_daily_rating.channels_id  = ${channels_id} 
         and channel_daily_rating.date = '${date}'
         and channel_daily_rating_logs.created >= '${start_datetime}'
         and channel_daily_rating_logs.created <= '${end_datetime}'
        `;
        let obj = {
            'channels_id' : channels_id,
            'start_datetime' : start_datetime,
            'end_datetime' : end_datetime,
            'date' : date
        };
        let execute_query = await pool.request()
            .query($query);
        let $result = execute_query.recordsets;
        $result     = JSON.parse(JSON.stringify($result));
      

        let sum_unqiue =0;
        if(service_used != null){
      
            let islastdate = check_islastdateofmonth(date);
            let rating_data_table      = get_ratingdatatable(date);
            let rating_data_table_iptv = get_ratingdatatable(date , "iptv");
            rating_data_table_iptv     = '[S3Application].dbo.' +rating_data_table_iptv;

            let uniquedevice_satalite = await get_unique_reachdevice( rating_data_table , obj , null , config);
            let uniquedevice_iptv     = await get_unique_reachdevice( rating_data_table_iptv , obj , 'iptv' , config);
              sum_unqiue            = uniquedevice_satalite + uniquedevice_iptv;
        
            if(islastdate == true){
                let rating_data_table_nextmonth      =set_datatable(date); // rating satalite table next month
                let rating_data_table_nextmonth_iptv =set_datatable(date , 'iptv'); // rating satalite table next month
                rating_data_table_nextmonth_iptv = '[S3Application].dbo.' + rating_data_table_nextmonth_iptv;
                let uniquedevice_satalite_nm = await get_unique_reachdevice( rating_data_table_nextmonth , obj , null , config);
                let uniquedevice_iptv_nm     = await get_unique_reachdevice( rating_data_table_nextmonth_iptv , obj , 'iptv' , config);
                sum_unqiue += uniquedevice_satalite_nm;
                sum_unqiue += uniquedevice_iptv_nm;
            }
        }


        // console.log(reach_device_statistic)

         const avg_rating = $result[0][0].avg_rating;
         const reach_devices = $result[0][0].reach_devices;
         let reach_device_unique = sum_unqiue;
        // console.log(reach_device_unique);
         let minute_diff = find_minutediff(start_datetime , end_datetime);
         let sec_diff    = find_seconddiff(start_datetime , end_datetime);
         /** ============ case :  if rating logs not found on this period and then =============================================================  */
         if(avg_rating <= 0  || minute_diff <10){
         
            let enddatetime_str =moment(start_datetime).add(10, 'minutes').format('YYYY-MM-DD HH:mm:');    // หาไม่เจอ rating log ให้ + ไป 10 นาที
            $query = `
            select top 1 channel_daily_rating_logs.rating  avg_rating , channel_daily_rating_logs.reach_devices
            from channel_daily_rating_logs  join channel_daily_rating on  channel_daily_rating_logs.channel_daily_rating_id = channel_daily_rating.id
            where  channel_daily_rating.channels_id  = ${channels_id} 
            and channel_daily_rating.date = '${date}'
            and channel_daily_rating_logs.created >= '${start_datetime}'
            and channel_daily_rating_logs.created <= '${enddatetime_str}59'
            order by channel_daily_rating_logs.created desc
           `;
        
           // console.log($query + " <<--- " + minute_diff + "start_datetime/ end_datetime" + start_datetime +":"+ end_datetime);
           let execute_query = await pool.request()
               .query($query);
           let $result = execute_query.recordsets;
            $result     = JSON.parse(JSON.stringify($result));
            if($result[0][0]){
            let avg_rating = $result[0][0].avg_rating;
            let reach_devices = $result[0][0].reach_devices;
            avg_rating     =  (avg_rating * minute_diff) / 10;
            reach_devices  =  (reach_devices * minute_diff) / 10;
            // console.log(avg_rating);
            avg_rating     = avg_rating > 0 ? avg_rating : 0;
            reach_devices     = reach_devices > 0 ? reach_devices : 0;
            }
            if(service_used != null){
                let reach_device_unique = sum_unqiue > 0 ? sum_unqiue : 0;
                return [{'rating':avg_rating , 'reach_devices':reach_devices , 'reach_device_unique': reach_device_unique , 'minute_diff': minute_diff , 'sec_diff' : sec_diff}];
            }else{
                let reach_device_unique = sum_unqiue > 0 ? sum_unqiue : 0;
                return [{'rating':avg_rating , 'reach_devices':reach_devices   , 'reach_device_unique':reach_device_unique}];
            }
         }
         /** ============ ( eof ) case :  if rating logs not found on this period and then =============================================================  */
       // console.log(avg_rating);
       if(service_used != null){
            
            let reach_device_unique = sum_unqiue > 0 ? sum_unqiue : 0;
            return [{'rating':avg_rating , 'reach_devices':reach_devices , 'reach_device_unique': reach_device_unique ,'minute_diff': minute_diff , 'sec_diff' : sec_diff}];
        }else{
            let reach_device_unique = sum_unqiue > 0 ? sum_unqiue : 0;
            return [{'rating':avg_rating , 'reach_devices':reach_devices , 'reach_device_unique':reach_device_unique }];
        }
      

       // return avg_rating;
    }

    function get_tvprogram_specificdatetime(start_datetime , end_datetime , channels_id =  null , program_id = null , $getallchannel_status = null){
        const _start_date = init_getdatefromdatetime(start_datetime)
        const _end_date = init_getdatefromdatetime(end_datetime);
        
        $where_condition = "";
        if(program_id == null){
            $where_condition = ` where   convert(varchar,date) + ' ' + convert(varchar,start_time) >= '${start_datetime}'
            and   convert(varchar,date) + ' ' + convert(varchar,end_time) <= '${end_datetime}' `;
        }
        $query = `
        select r1.channels_id as channel_id , r1.channel_name , r1.id as program_id , r1.ref_id , r1.name as program_name ,
            r1.start_datetime , r1.end_datetime  from (
            select convert(varchar,date) + ' ' + convert(varchar,start_time) as start_datetime 
            , convert(varchar,date) + ' ' + convert(varchar,end_time) as end_datetime
            , channel_name ,channel_programs_baseon_date.* from 
            channel_programs_baseon_date
            inner join channels on channels.id = channel_programs_baseon_date.channels_id 
                ${$where_condition}
            )r1
        `;
        $sub_where = "";
        if($getallchannel_status == null){ // case : if get all channel status is  true then 
            if(channels_id != null){
                
                $sub_where += " where  r1.channels_id = " + channels_id;
            }else{
                $sub_where += " where  (r1.channels_id = 252 or r1.channels_id = 525) ";
            }
            if(program_id != null){
                if($sub_where != ''){
                    $sub_where += " and r1.id = " + program_id;
                }
                else{
                    $sub_where += " where r1.id = " + program_id;
                }
            }
        }
        $query += $sub_where;
        $query += " order by start_datetime asc ";
       
        return $query;
    }
    function seconds_since_epoch(d){ 
        return Math.floor( d / 1000 ); 
    }
    function get_dateobject(date, type) {
        var data;
        var dateobject = new Date(date);
        if (type == "month") {
            data = dateobject.getUTCMonth() + 1; //months from 1-12
        } else if (type == "day") {
            data = dateobject.getUTCDate();
        } else {
            data = dateobject.getUTCFullYear();
        }

        return data;
    }





    module.exports = {
        get_provinceviewer: get_provinceviewer,
        get_deviceviewerbyprovince: get_deviceviewerbyprovince,
        get_deviceviewerbehaviour: get_deviceviewerbehaviour,
        get_deviceviewer_perminute: get_deviceviewer_perminute,
        get_deviceviewer_report:get_deviceviewer_report,
        get_overview_report:get_overview_report,
        get_overview_report_daily:get_overview_report_daily,
        get_overview_report_perminute:get_overview_report_perminute,
        get_average_views_perminute:get_average_views_perminute,
        tvprogram:tvprogram,
        statistics:statistics,
        tvprogram_ratings:tvprogram_ratings,
        channel_ratings:channel_ratings

    }

           
 
     //let dict_bangkok_value    = set_newarr_province_statisticaapi();
        // for (const [channel_id, value] of Object.entries(send_back_arrprovince)) {
        //     //sum_array_province(path_merge_province , channel_id); //  sum everychannel base on time slot ( top 20)
        //     let dict_bangkok_value_arr  = set_provincebangkokbyregionvalue(path_regionall , path_merge_province , channel_id);
        //     console.log(dict_bangkok_value_arr);
        //     dict_bangkok_value[channel_id].push(dict_bangkok_value_arr);

        // }
        // console.log(dict_bangkok_value);
        // return;