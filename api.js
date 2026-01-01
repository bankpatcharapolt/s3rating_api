var Db = require('./dboperations');
var Customers = require('./customer');

const https = require(`https`);
let ENVIRONMENT = "dev";

const dboperations = require('./dboperations');

/** ( document - 2021/02/18 )
 * Express เป็น web application framework บน Node.js ที่ได้รับความนิยมมากๆตัวหนึ่ง 
 * ซึ่งตัว Express เนี่ยจะมีฟีจเจอร์ต่างๆที่ช่วยให้เราทำเว็บได้สะดวกขึ้น 
 * เช่น การทำ routing, middleware ใช้ในการจัดการ request และ response เป็นต้น 
 * ทำให้เราสามารถพัฒนาเว็บโดยใช้ Node.js ได้สะดวกและรวดเร็วยิ่งขึ้น
 * ติดตั้ง Express ด้วยคำสั่ง  ( npm install express --save )
 */
var express = require('express');
const {
   Worker, workerData,

} = require("worker_threads");



var bodyParser = require('body-parser');
var cors = require('cors');
var app = express();
var router = express.Router();
var path = require('path');
var mime = require('mime');
var fs = require('fs'); // ใช้งาน file system module ของ nodejs
const jwt = require('jsonwebtoken'); // ใช้งาน jwt module
const authorization = require('./config/authorize')
const loginRouter = require('./routes/login')
// const rateLimit = require('express-rate-limit')
if(ENVIRONMENT == "PRODUCTION"){

}

var cluster = require('cluster');




app.use(bodyParser.urlencoded({
   extended: true
})); // bodyParser.urlencoded({ extended: true }) จะทำให้เรา parse application/x-www-form-urlencoded ได้
app.use(bodyParser.json()); // ใช้ middleware ในการ parsing request body ครับ โดย bodyParser.json() จะทำให้เรา parse application/json
// app.use(timeout(240000));
// app.use(haltOnTimedout);

app.use('/login', loginRouter)
/*
Middleware คือ โค้ดที่ทำหน้าที่เป็นตัวกรอง request ก่อนที่จะเข้ามาถึงแอพพลิเคชั่นของเรา
ว่าง่ายๆคือก่อนที่จะเข้ามาถึง app.get หรือ app.post  
มันจะต้องผ่าน middleware ก่อน 
โดยเราสามารถใช้งาน middleware ได้ผ่าน app.use() 
เราเอา middleware ไปใช้ประโยชน์ได้หลายอย่าง เช่น กรอง request 
ว่าต้องมีการถือ token มาก่อน ถึงจะเข้ามาเอา resource ของเราได้ หรือเอาไว้เก็บ log ว่าใครเข้ามาที่แอพพลิเคชั่นของเราบ้าง
*/
app.use(cors());
app.use('/api', router);


router.use((request, response, next) => {
  
   console.log(request);
   next();
})


function haltOnTimedout(req, res, next){
   if (!req.timedout) next();
 }

router.route('/get_provinceviewer').get(authorization, (req, response, next) => { 
   var date = req.body.date;
   var channel_id = req.body.channel_id;
   dboperations.get_provinceviewer(date , channel_id).then(result => {
      response.json(result[0]);
   })

})




router.route('/get_provinceviewer').post(authorization, (req, res, next) => { 
   var date = req.body.date;
   var channel_id = req.body.channel_id;
   dboperations.get_provinceviewer(date , channel_id).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});


//router.post('/get_deviceviewerbyprovince', (req, res) => {
router.route('/get_deviceviewerbyprovince').post(authorization, (req, res, next) => { 
   var date = req.body.date;
   var channel_id = req.body.channel_id;
   var province_id= req.body.province_id;
   dboperations.get_deviceviewerbyprovince(date , channel_id , province_id).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});

router.post('/get_deviceviewer_report', (req, res) => {
//router.route('/get_deviceviewer_report').post(authorization, (req, res, next) => { 
   var date = req.body.date;
   var channel_id = req.body.channel_id;
  
   dboperations.get_deviceviewer_report(date , channel_id ).then(result => {
      if(result != false){
         
         setTimeout(() => {
            res.json(result[0]); // wait until create file and the return
         }, 3000); 
         //res.download('./excel/daily/' + result[0].data );
         // res.sendFile('C:\\xampp\\htdocs\\s3rating_apiservice\\s3rating_nodeapi\\excel\\daily\\dailyreport_1647849079.xlsx');
      }else{
         res.json(false);
      }
   })
  
});
router.get('/excel/:daily', function(req, res) {
   var filename = req.query.filename;
   res.download('./excel/daily/' + filename );

   // res.sendFile('C:\\xampp\\htdocs\\s3rating_apiservice\\s3rating_nodeapi\\excel\\daily\\dailyreport_1647851929.xlsx');
});

router.get('/export_excel/:reporttype', function(req, res) {
   var filename = req.query.filename;
   var foldername = req.query.foldername;
   var report_type = req.query.report_type;
   if(report_type == "overview"){
      report_type = "overview_perminute";
   }
   res.download('./excel/'+report_type+'/tvprogram_baseon_tpbs/'+foldername+"/" + filename );
});

// router.post('/get_overview_report', (req, res) => {
router.route('/get_overview_report').post(authorization, (req, res, next) => {
   
   var date = req.body.date;
   var avg_minute = req.body.avg_minute;
   dboperations.get_overview_report(date , avg_minute ).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});

// router.globalAgent.maxSockets = 100;



router.route('/v1/statistics').post(authorization, (req, res, next) => {
   var date = req.body.date;
   var start_time = req.body.start_time;
   var end_time   = req.body.end_time;
   dboperations.statistics(date , start_time , end_time  ).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});
// cluster.on('exit', function(worker, code, signal) {
//    console.log('worker ' + worker.process.pid + ' died');
//    // kill the other workers.
//    for (var id in cluster.workers) {
//      cluster.workers[id].kill();
//    }
//    // exit the master process
//    process.exit(0);
//  });

const runService = (date , start_time ,end_time , channel_id ,res , type = null) => {
   return new Promise((resolve, reject) => {
  
      if(type =="statistic"){
         const worker = new Worker('./worker_threads.js' , {
               workerData: {
                  date: date,
                  start_time:start_time,
                  end_time:end_time,
                  channel_id:channel_id
               }
            });
         
         worker.on('message',(data) => {
            worker.terminate();
            resolve(data)
         });

       
         //setTimeout( worker.removeAllListeners('message'), 5000);
        //
      }else{
         const worker = new Worker('./worker_threads_channel_rating.js' , {
               workerData: {
                  date: date,
                  start_time:start_time,
                  end_time:end_time,
                  channel_id:channel_id
               }
            });
         
            worker.on('message',(data) => {
               worker.terminate();
               resolve(data)
            });
      }
   })
}

const run = async (date , start_time ,end_time , channel_id ,res) => {
   var type = "statistic";
   const result = await runService(date , start_time ,end_time , channel_id ,res , type);
   res.json(result);
   
}

const run_api_channelrating = async (date , start_time ,end_time , channel_id ,res) => {
   var type = "channel_rating";
   const result = await runService(date , start_time ,end_time , channel_id ,res , type);
   res.json(result);
   
}

router.route('/v1/statistics/channel_id/:channel_id').post(authorization, (req, res, next) => {
   
   var date = req.body.date;
   var start_time = req.body.start_time;
   var end_time   = req.body.end_time;
   let channel_id  = req.params.channel_id;
  
   run(date , start_time ,end_time , channel_id ,res).catch(err => console.error(err));


   // dboperations.statistics(date , start_time , end_time , channel_id  ).then(result => {
      
   //       if(result != false){
   //          res.json(result[0]);
   //       }else{
   //          res.json(false);
   //       }
   // }).catch(function (error) {
      
   // });

    
  
});


// resource  -> get rating all channel
router.route('/v1/tvprogram_ratings/').post(authorization, (req, res, next) => {
   var date = req.body.date;
   var start_time = req.body.start_time;
   var end_time   = req.body.end_time;
   dboperations.tvprogram_ratings(date , start_time , end_time  , null ).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});

// resource  -> get rating from channel id
router.route('/v1/tvprogram_ratings/channel_id/:channel_id').post(authorization, (req, res, next) => {
   var date = req.body.date;
   let channel_id  = req.params.channel_id;
   var start_time = req.body.start_time;
   var end_time   = req.body.end_time;
   dboperations.tvprogram_ratings(date , start_time , end_time  , channel_id ).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});
// resource  -> get rating from tvprogram id
router.route('/v1/tvprogram_ratings/program_id/:program_id').post(authorization, (req, res, next) => {
   var date = req.body.date;
   let program_id  = req.params.program_id;
   var start_time = req.body.start_time;
   var end_time   = req.body.end_time;
   var service_used  = null;
   if(req.body.service_used != null){
      service_used = req.body.service_used;
   }
   dboperations.tvprogram_ratings(date , start_time , end_time  , null , program_id , service_used ).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});

// resource get channel rating
router.route('/v1/channel_ratings').post(authorization, (req, res, next) => {
   
   // var date = req.body.date;
   // var start_time = req.body.start_time;
   // var end_time   = req.body.end_time;
   // dboperations.channel_ratings(date,start_time , end_time ,  null  , null ).then(result => {
   //    if(result != false){
   //       res.json(result[0]);
   //    }else{
   //       res.json(false);
   //    }
   // })

   
   res.json( { status : false , result_code : 405 ,  result_desc : "not authorized to access this resource/api" } );
  
});

// resource  -> get rating from channel id
router.route('/v1/channel_ratings/channel_id/:channel_id').post(authorization, (req, res, next) => {
   var date = req.body.date;
   let channel_id  = req.params.channel_id;
   var start_time = req.body.start_time;
   var end_time   = req.body.end_time;

  
   run_api_channelrating(date , start_time ,end_time , channel_id ,res).catch(err => console.error(err));

 
  
});
// resource  -> get rating from tvprogram id
router.route('/v1/channel_ratings/program_id/:program_id').post(authorization, (req, res, next) => {
   var date = req.body.date;
   let program_id  = req.params.program_id;
   var start_time = req.body.start_time;
   var end_time   = req.body.end_time;
   dboperations.channel_ratings(date , start_time , end_time  , null , program_id ).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});


router.route('/v1/tvprogram').post(authorization, (req, res, next) => {
   
   var start_datetime = req.body.start_datetime;
   var end_datetime   = req.body.end_datetime;
   var program_id     = req.body.program_id;
   dboperations.tvprogram(start_datetime , end_datetime ,  null  , program_id ).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});

router.route('/v1/tvprogram/:channel_id').post(authorization, (req, res, next) => {
   
   var start_datetime = req.body.start_datetime;
   var end_datetime   = req.body.end_datetime;
   let channel_id  = req.params.channel_id;
   var program_id     = req.body.program_id;
   dboperations.tvprogram(start_datetime , end_datetime ,  channel_id   , program_id).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});



router.post('/get_overview_report_daily', (req, res) => {
// router.route('/get_overview_report_daily').post(authorization, (req, res, next) => {
   
   var date = req.body.date;
   dboperations.get_overview_report_daily(date  ).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});


router.post('/get_average_views_perminute', (req, res) => {
// router.route('/get_average_views_perminute').post(authorization, (req, res, next) => {
   
   var date = req.body.date;
   var avg_minute = req.body.avg_minute;
   dboperations.get_average_views_perminute(date , avg_minute ).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});

router.post('/get_overview_report_perminute', (req, res) => {
// router.route('/get_overview_report_perminute').post(authorization, (req, res, next) => {
   
   var date = req.body.date;
   var avg_minute = req.body.avg_minute;
   dboperations.get_overview_report_perminute(date , avg_minute ).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});



router.post('/get_deviceviewer', (req, res) => {
//router.route('/get_deviceviewer').post(authorization, (req, res, next) => {
   var date = req.body.date;
   var channel_id = req.body.channel_id;
  
   dboperations.get_deviceviewer_perminute(date , channel_id ).then(result => {
      if(result != false){
         res.json(result[0]);
      }else{
         res.json(false);
      }
   })
  
});

// router.post('/get_deviceviewerbehaviour', (req, res) => {
router.route('/get_deviceviewerbehaviour').post(authorization, (req, res, next) => {
   var devices_id = req.body.devices_id;
   var datereport = req.body.datereport;
 
   dboperations.get_deviceviewerbehaviour(devices_id , datereport).then(result => {
      res.json(result);
   })
  
});





/**
 * จากโค้ดก็คือเราจะสร้าง server อยู่ที่ port 8090 จะทำให้เราเข้าถึง server 
 * ได้ที่ http://localhost:8090 ซึ่งเราสามารถสั่งรันได้ด้วยคำสั่ง
 */

if(ENVIRONMENT == "PRODUCTION"){
const options = {
   key: fs.readFileSync(`/etc/apache2/ssl/tpbsrating/private.key`),
   cert: fs.readFileSync(`/etc/apache2/ssl/tpbsrating/tpbsrating.softtechnw.com.chained.crt`)
   };

 https.createServer(options, (req, res) => {
   res.writeHead(200);
 }).listen(8090);
}else{
   var port = process.env.PORT || 8090;
   //app.listen(port);
   // app.enable('trust proxy');
   var server = app.listen(port, function() {
   // debug('Express server listening on port ' + server.address().port);
   console.log('S3Rating API is runnning at ' + port);
   });
   server.timeout = 1960000;
}

//console.log('S3Rating API is runnning at ' + port);