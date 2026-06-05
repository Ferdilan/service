// db.js — Neon PostgreSQL dengan Automatic Failover (Primary → Backup)
const { Pool } = require('pg');
require('dotenv').config();

// ============================================================
// KONFIGURASI POOL
// ============================================================
const SSL_CONFIG = { rejectUnauthorized: false };

const primaryPool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: SSL_CONFIG,
    keepAlive: true,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
});

const backupPool = new Pool({
    connectionString: process.env.DATABASE_URL_BACKUP,
    ssl: SSL_CONFIG,
    keepAlive: true,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
});

// ============================================================
// STATE FAILOVER
// ============================================================
let usingBackup = false;           // Flag: sedang pakai backup?
let probeTimer = null;             // Timer untuk coba balik ke primary
const PROBE_INTERVAL_MS = 10 * 60 * 1000; // Coba balik ke primary setiap 10 menit

// ============================================================
// DETEKSI ERROR KUOTA / KONEKSI NEON
// ============================================================
function isNeonUnavailableError(err) {
    if (!err || !err.message) return false;
    const msg = err.message.toLowerCase();
    return (
        msg.includes('compute time quota') ||
        msg.includes('project is disabled') ||
        msg.includes('connection refused') ||
        msg.includes('econnrefused') ||
        msg.includes('endpoint is disabled') ||
        msg.includes('too many connections') ||
        msg.includes('connection terminated') ||
        err.code === 'ENOTFOUND' ||
        err.code === 'ETIMEDOUT' ||
        err.code === '57P01' // admin shutdown
    );
}

// ============================================================
// SWITCH KE BACKUP
// ============================================================
function switchToBackup(reason) {
    if (usingBackup) return; // Sudah di backup, tidak perlu switch lagi
    usingBackup = true;
    console.warn(`⚠️  [DB-FAILOVER] Beralih ke database BACKUP. Alasan: ${reason}`);

    // Jadwalkan probe: coba balik ke primary setiap 10 menit
    if (!probeTimer) {
        probeTimer = setInterval(async () => {
            console.log('🔍 [DB-FAILOVER] Mencoba koneksi ke Primary kembali...');
            try {
                const client = await primaryPool.connect();
                await client.query('SELECT 1');
                client.release();
                // Berhasil → balik ke primary
                usingBackup = false;
                clearInterval(probeTimer);
                probeTimer = null;
                console.log('✅ [DB-FAILOVER] Primary kembali online! Beralih kembali ke Primary.');
            } catch (e) {
                console.log(`⏳ [DB-FAILOVER] Primary masih tidak tersedia: ${e.message}`);
            }
        }, PROBE_INTERVAL_MS);
    }
}

// ============================================================
// EVENT HANDLER ERROR POOL
// ============================================================
primaryPool.on('error', (err) => {
    console.error('⚠️  [DB-PRIMARY] Pool error:', err.message);
    if (isNeonUnavailableError(err)) {
        switchToBackup(err.message);
    }
});

backupPool.on('error', (err) => {
    console.error('⚠️  [DB-BACKUP] Pool error:', err.message);
});

// ============================================================
// FUNGSI QUERY UTAMA (dengan fallback otomatis)
// ============================================================
async function runQuery(sql, params) {
    const activePool = usingBackup ? backupPool : primaryPool;

    try {
        return await activePool.query(sql, params);
    } catch (err) {
        // Jika primary gagal karena kuota/koneksi → switch ke backup dan coba ulang
        if (!usingBackup && isNeonUnavailableError(err)) {
            switchToBackup(err.message);
            console.warn(`🔄 [DB-FAILOVER] Retry query ke Backup...`);
            return await backupPool.query(sql, params);
        }
        throw err; // Error lain (SQL syntax, constraint) → lempar ke pemanggil
    }
}

// ============================================================
// EXPORT
// ============================================================
module.exports = {
    /**
     * Menjalankan query dan mengembalikan format [rows, fields, result]
     * (kompatibel dengan format MySQL/Sequelize-style yang dipakai service.js)
     */
    execute: async (sql, params) => {
        const result = await runQuery(sql, params);
        return [result.rows, result.fields, result];
    },

    /**
     * Menjalankan query dan mengembalikan objek result pg standar
     */
    query: async (sql, params) => {
        return await runQuery(sql, params);
    },

    /**
     * Mengembalikan pool yang sedang aktif (primary atau backup)
     */
    getPool: () => (usingBackup ? backupPool : primaryPool),

    /**
     * Mengembalikan status failover saat ini
     */
    getStatus: () => ({
        usingBackup,
        active: usingBackup ? 'BACKUP' : 'PRIMARY',
    }),

    /**
     * Memaksa switch ke backup (untuk kebutuhan testing/manual)
     */
    forceBackup: () => switchToBackup('Manual switch'),

    // Expose pools untuk digunakan service.js (LISTEN connection)
    primaryPool,
    backupPool,
};