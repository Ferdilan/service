const mysql = require('mysql2/promise');

require('dotenv').config();

const pool = mysql.createPool({
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_DATABASE || 'ambulans',
    port: process.env.DB_PORT || 3306,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    enableKeepAlive: true,
    keepAliveInitialDelay: 0
});

// pool.getConnection()
//     .then(connection => {
//         console.log('Koneksi Database (db.js) berhasil.');
//         connection.release();
//     })
//     .catch(err => {
//        console.error('Koneksi Database (db.js) GAGAL: ' + err.code);
//         if (err.code === 'ETIMEDOUT') {
//             console.error('Pastikan Firewall Anda mengizinkan Port ' + (process.env.DB_PORT || 3306));
//         }
//         if (err.code === 'ECONNREFUSED') {
//             console.error('Pastikan server MySQL (misal: XAMPP) sudah berjalan di Port ' + (process.env.DB_PORT || 3306));
//         }
//         if (err.code === 'ER_ACCESS_DENIED_ERROR') {
//             console.error('Periksa DB_USER dan DB_PASSWORD di file .env Anda.');
//         }
//     });

// Mengekspor 'pool' agar bisa di-import oleh file lain
module.exports = pool;