const mysql = require('mysql2/promise');

const pool = mysql.createPool({
    host: 'localhost',         // Ganti jika perlu
    user: 'root',              // Ganti dengan username database Anda
    password: '', // Ganti dengan password database Anda
    database: 'ambulans',   // Ganti dengan nama database Anda
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Mengekspor 'pool' agar bisa di-import oleh file lain
module.exports = pool;