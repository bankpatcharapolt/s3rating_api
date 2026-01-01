// const { WorkerData, parentPort } = require('worker_threads')
// parentPort.postMessage({ welcome: WorkerData })
const dboperations = require('./dboperations');
const {workerData ,parentPort} = require('worker_threads');
const res = require('express/lib/response');

dboperations.channel_ratings(workerData.date , workerData.start_time , workerData.end_time , workerData.channel_id ).then(result => {
   if(result != false){
    parentPort.postMessage(result[0]);
   }else{
      
   }
})

