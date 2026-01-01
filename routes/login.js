const express = require('express');
const router = express.Router()
const jwt = require('jsonwebtoken')  // ใช้งาน jwt module
const fs = require('fs') // ใช้งาน file system module ของ nodejs
 
router.post('/', function(req, res, next) {
    // ใช้ค่า privateKey เป็น buffer ค่าที่อ่านได้จากไฟล์ private.key ในโฟลเดอร์ config
    const privateKey = fs.readFileSync(__dirname+'/../config/private.key')
    // สมมติข้อมูลใน payload เช่น id , name , role ค่าเหล่านี้ เราจะเอาจากฐานข้อมูล กรณีทำการล็อกอินจริง
    // ไม่ authentication ใน database เจ้านี้ใช้ของเขาคนเดียว ฝังไว้ในcode
    const payload = {
        id:20134,
        username:req.body.username,
        password:req.body.password,
        role:'admin'
    }
    
    if((payload.username != 'tpbsrating' && payload.username != 'superadmin') || payload.password != 'example123E$'){
        return res.status(401).json({ // หาก error ไม่ผ่าน
            "status": 401,
            "message": "unauthorized : username or password is incorrect"
        })   
    }
    // ทำการลงชื่อขอรับ token โดยใช้ค่า payload กับ privateKey
    const token = jwt.sign(payload, privateKey, {
        expiresIn: "24h" // it will be expired after 24 hours
    });
    // เมื่อเราได้ค่า token มา ในที่นี้ เราจะแสดงค่าใน textarea เพื่อให้เอาไปทดสอบการทำงานผ่าน postman
    // ในการใช้งานจริง ค่า token เราจะส่งไปกับ heaer ในขั้นตอนการเรียกใช้งาน API  เราอาจจะบันทึก
    // ไว้ใน localStorage ไว้ใช้งานก็ได้

    res.send({username: payload.username, access_token: token})
})
 
module.exports = router