// const mysql = require('mysql2/promise');
const { Pool } = require('pg');
require('dotenv').config();

// ============ MYSQL ============
// const pool = mysql.createPool({
//     host: process.env.DB_HOST || 'localhost',
//     user: process.env.DB_USER || 'root',
//     password: process.env.DB_PASSWORD || '',
//     database: process.env.DB_DATABASE || 'ambulans',
//     port: process.env.DB_PORT || 3306,
//     waitForConnections: true,
//     connectionLimit: 10,
//     queueLimit: 0,
//     enableKeepAlive: true,
//     keepAliveInitialDelay: 0
// });


// ============ Neon ============
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false,
        sslmode: 'verify-full'
    },
    keepAlive: true,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
});

pool.on('error', (err, client) => {
    console.error('⚠️ [Database] Koneksi terputus dari Neon (Auto-Sleep / Network Drop).');
    console.error('Detail:', err.message);
    // Jangan lakukan process.exit(1). 
    // Biarkan Pool secara otomatis membuat koneksi baru saat ada T2/T5 masuk.
});

module.exports = {
    // Membungkus pool.query agar bisa dipanggil dengan nama 'execute' atau 'query'
    execute: async (sql, params) => {
        try {
            const result = await pool.query(sql, params);
            // Mengembalikan struktur data yang menyerupai format destructuring MySQL
            // result.rows adalah array data, result.rowCount adalah jumlah baris terdampak
            return [result.rows, result.fields, result];
        } catch (error) {
            throw error;
        }
    },
    query: async (sql, params) => {
        return await pool.query(sql, params);
    },
    getPool: () => pool
};