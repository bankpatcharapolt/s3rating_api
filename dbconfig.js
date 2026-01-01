
const config = {
    user :'sa',
    password :'computer',
    server:'192.168.100.21',
    database:'S3Rating',
    options:{
        trustedconnection: true,
        enableArithAbort : true, 
        instancename :'SQLEXPRESS',
        encrypt:false,
    },
    requestTimeout: 300000,
    pool: {
        max: 300,
        min: 1,
        idleTimeoutMillis: 30000
      },
    port : 1433
    ,   timezone: 'Asia/Jakarta' 
}

module.exports = config; 