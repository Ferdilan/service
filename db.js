// db.js — Neon PostgreSQL with Automatic Failover (Primary → Backup)
const { Pool } = require('pg');
require('dotenv').config();

// ============================================================
// POOL CONFIGURATION
// ============================================================
const SSL_CONFIG = { rejectUnauthorized: false };

const primaryPool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: SSL_CONFIG,
    min: 0,                        // Do not maintain minimum connections
    idleTimeoutMillis: 10000,      // Release idle connections after 10 seconds
    connectionTimeoutMillis: 10000,
});

const backupPool = new Pool({
    connectionString: process.env.DATABASE_URL_BACKUP,
    ssl: SSL_CONFIG,
    min: 0,                        // Do not maintain minimum connections
    idleTimeoutMillis: 10000,      // Release idle connections after 10 seconds
    connectionTimeoutMillis: 10000,
});

// ============================================================
// FAILOVER STATE
// ============================================================
let usingBackup = false;           // Flag: currently using backup?
let probeTimer = null;             // Timer to attempt switching back to primary
const PROBE_INTERVAL_MS = 30 * 60 * 1000; // Attempt switching back to primary every 30 minutes

// ============================================================
// NEON QUOTA / CONNECTION ERROR DETECTION
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
// SWITCH TO BACKUP
// ============================================================
function switchToBackup(reason) {
    if (usingBackup) return; // Already on backup, no need to switch again
    usingBackup = true;
    console.warn(`⚠️  [DB-FAILOVER] Switching to BACKUP database. Reason: ${reason}`);

    // Schedule probe: try reconnecting to primary every 30 minutes
    if (!probeTimer) {
        probeTimer = setInterval(async () => {
            console.log('🔍 [DB-FAILOVER] Attempting reconnection to Primary...');
            try {
                const client = await primaryPool.connect();
                await client.query('SELECT 1');
                client.release();
                // Success → switch back to primary
                usingBackup = false;
                clearInterval(probeTimer);
                probeTimer = null;
                console.log('✅ [DB-FAILOVER] Primary is back online! Switching back to Primary.');
            } catch (e) {
                console.log(`⏳ [DB-FAILOVER] Primary still unavailable: ${e.message}`);
            }
        }, PROBE_INTERVAL_MS);
    }
}

// ============================================================
// POOL ERROR EVENT HANDLERS
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
// MAIN QUERY FUNCTION (with automatic fallback)
// ============================================================
async function runQuery(sql, params) {
    const activePool = usingBackup ? backupPool : primaryPool;

    try {
        return await activePool.query(sql, params);
    } catch (err) {
        // If primary fails due to quota/connection → switch to backup and retry
        if (!usingBackup && isNeonUnavailableError(err)) {
            switchToBackup(err.message);
            console.warn(`🔄 [DB-FAILOVER] Retrying query against Backup...`);
            return await backupPool.query(sql, params);
        }
        throw err; // Other errors (SQL syntax, constraint) → rethrow to caller
    }
}

// ============================================================
// EXPORTS
// ============================================================
module.exports = {
    /**
     * Executes query and returns [rows, fields, result] format
     * (compatible with MySQL/Sequelize-style format used by service.js)
     */
    execute: async (sql, params) => {
        const result = await runQuery(sql, params);
        return [result.rows, result.fields, result];
    },

    /**
     * Executes query and returns standard pg result object
     */
    query: async (sql, params) => {
        return await runQuery(sql, params);
    },

    /**
     * Returns currently active pool (primary or backup)
     */
    getPool: () => (usingBackup ? backupPool : primaryPool),

    /**
     * Returns current failover status
     */
    getStatus: () => ({
        usingBackup,
        active: usingBackup ? 'BACKUP' : 'PRIMARY',
    }),

    /**
     * Forces switch to backup (for testing/manual requirements)
     */
    forceBackup: () => switchToBackup('Manual switch'),

    // Expose pools for service.js (LISTEN connection)
    primaryPool,
    backupPool,
};