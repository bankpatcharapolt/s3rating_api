
const config_s3app = {
    user :'sa',
    password :'computer',
    server:'192.168.100.21',
    database:'S3Application',
    options:{
        trustedconnection: true,
        enableArithAbort : true, 
        instancename :'SQLEXPRESS',
        encrypt:false,
    },
    port : 1433,
       timezone: 'Asia/Jakarta' 
}

module.exports = config_s3app; 