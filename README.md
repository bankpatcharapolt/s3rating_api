# Web api S3rating  

## Node.js

### branch

1. master = branch production 
    1. database config:
      1. database name:  S3Rating
      2. database host  : 100.21

1. dev    = branch สำหรับตัวเทส ( http://testweb.softtechnw.com/s3rating_apiservice/ ) 
    1. database config:
      1. database name:  S3Rating
      2. database host  : 100.21



1. how to use
    1. npm install ( ไม่ได้อัพโหลด node_modules ไป )
    2. npm install -g pm2@3.2.2  ( install lib ชื่อ pm2 ไว้ให้สั่งทำงานใน background ได้)
    3. pm2 start api.js ( สั่ง run ใน background )
    4. ถ้า npm install  lib excel error ให้พิมพ์  npm install excel4node --save
    