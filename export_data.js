// export_data.js
// Script untuk mengekspor data real dari Neon Primary ke SQL INSERT statements
// Jalankan: node export_data.js

require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

function escapeStr(val) {
    if (val === null || val === undefined) return 'NULL';
    // Escape single quotes
    return "'" + String(val).replace(/'/g, "''") + "'";
}

function formatVal(val, colName) {
    if (val === null || val === undefined) return 'NULL';
    if (typeof val === 'number') return val;
    if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE';
    // Dates
    if (val instanceof Date) return escapeStr(val.toISOString());
    // Normalisasi status_operasional ke UPPERCASE agar cocok dengan service.js
    if (colName === 'status_operasional') return escapeStr(String(val).toUpperCase());
    return escapeStr(val);
}

async function exportData() {
    const client = await pool.connect();
    try {
        console.log('✅ Terhubung ke Neon Primary. Mengambil data...\n');

        let output = [];

        output.push(`-- ============================================================`);
        output.push(`--  EXPORTED REAL DATA FROM NEON PRIMARY`);
        output.push(`--  Diekspor pada: ${new Date().toISOString()}`);
        output.push(`--  Tempel bagian ini ke BAGIAN 6 di backup_schema.sql`);
        output.push(`-- ============================================================\n`);

        // ============ AMBULANS ============
        const ambulansRes = await client.query('SELECT * FROM ambulans ORDER BY id_ambulans');
        const ambulansRows = ambulansRes.rows;
        console.log(`📦 Ditemukan ${ambulansRows.length} baris di tabel ambulans`);

        if (ambulansRows.length > 0) {
            output.push(`-- ---- Tabel: ambulans (${ambulansRows.length} baris) ----`);
            output.push(`TRUNCATE TABLE transaksi_panggilan, ambulans RESTART IDENTITY CASCADE;`);
            output.push(``);

            for (const row of ambulansRows) {
                const cols = Object.keys(row).map(c => `"${c}"`).join(', ');
                const vals = Object.entries(row).map(([k, v]) => formatVal(v, k)).join(', ');
                output.push(`INSERT INTO ambulans (${cols}) VALUES (${vals}) ON CONFLICT (id_ambulans) DO UPDATE SET`);
                const updates = Object.keys(row)
                    .filter(c => c !== 'id_ambulans')
                    .map(c => `"${c}" = EXCLUDED."${c}"`)
                    .join(', ');
                output.push(`    ${updates};`);
            }
            output.push(``);
            output.push(`-- Sync sequence ambulans`);
            output.push(`SELECT setval('ambulans_id_ambulans_seq', (SELECT MAX(id_ambulans) FROM ambulans));`);
            output.push(``);
        }

        // ============ PASIEN ============
        const pasienRes = await client.query('SELECT * FROM pasien ORDER BY id_pasien');
        const pasienRows = pasienRes.rows;
        console.log(`📦 Ditemukan ${pasienRows.length} baris di tabel pasien`);

        if (pasienRows.length > 0) {
            output.push(`-- ---- Tabel: pasien (${pasienRows.length} baris) ----`);
            output.push(`TRUNCATE TABLE pasien RESTART IDENTITY CASCADE;`);
            output.push(``);

            for (const row of pasienRows) {
                const cols = Object.keys(row).map(c => `"${c}"`).join(', ');
                const vals = Object.entries(row).map(([k, v]) => formatVal(v, k)).join(', ');
                output.push(`INSERT INTO pasien (${cols}) VALUES (${vals}) ON CONFLICT (id_pasien) DO UPDATE SET`);
                const updates = Object.keys(row)
                    .filter(c => c !== 'id_pasien')
                    .map(c => `"${c}" = EXCLUDED."${c}"`)
                    .join(', ');
                output.push(`    ${updates};`);
            }
            output.push(``);
            output.push(`-- Sync sequence pasien`);
            output.push(`SELECT setval('pasien_id_pasien_seq', (SELECT MAX(id_pasien) FROM pasien));`);
            output.push(``);
        }

        // ============ TRANSAKSI PANGGILAN ============
        const transaksiRes = await client.query('SELECT * FROM transaksi_panggilan ORDER BY id_panggilan');
        const transaksiRows = transaksiRes.rows;
        console.log(`📦 Ditemukan ${transaksiRows.length} baris di tabel transaksi_panggilan`);

        if (transaksiRows.length > 0) {
            output.push(`-- ---- Tabel: transaksi_panggilan (${transaksiRows.length} baris) ----`);
            output.push(``);

            for (const row of transaksiRows) {
                const cols = Object.keys(row).map(c => `"${c}"`).join(', ');
                const vals = Object.entries(row).map(([k, v]) => formatVal(v, k)).join(', ');
                output.push(`INSERT INTO transaksi_panggilan (${cols}) VALUES (${vals}) ON CONFLICT (id_panggilan) DO UPDATE SET`);
                const updates = Object.keys(row)
                    .filter(c => c !== 'id_panggilan')
                    .map(c => `"${c}" = EXCLUDED."${c}"`)
                    .join(', ');
                output.push(`    ${updates};`);
            }
            output.push(``);
            output.push(`-- Sync sequence transaksi`);
            output.push(`SELECT setval('transaksi_panggilan_id_panggilan_seq', (SELECT MAX(id_panggilan) FROM transaksi_panggilan));`);
            output.push(``);
        } else {
            output.push(`-- ---- Tabel: transaksi_panggilan (kosong) ----`);
            output.push(`-- (Tidak ada data transaksi untuk diekspor)`);
            output.push(``);
        }

        // Ringkasan
        output.push(`-- ============================================================`);
        output.push(`--  SELESAI. Ringkasan:`);
        output.push(`--  - ambulans        : ${ambulansRows.length} baris`);
        output.push(`--  - pasien          : ${pasienRows.length} baris`);
        output.push(`--  - transaksi       : ${transaksiRows.length} baris`);
        output.push(`-- ============================================================`);

        const outFile = path.join(__dirname, 'exported_data.sql');
        fs.writeFileSync(outFile, output.join('\n'), 'utf8');

        console.log(`\n✅ SUKSES! File SQL berhasil dibuat: exported_data.sql`);
        console.log(`📄 Total baris diekspor: ambulans=${ambulansRows.length}, pasien=${pasienRows.length}, transaksi=${transaksiRows.length}`);
        console.log(`\n📋 Langkah selanjutnya:`);
        console.log(`   1. Buka file exported_data.sql`);
        console.log(`   2. Salin isinya ke SQL Editor di Neon BACKUP dashboard`);
        console.log(`   3. Jalankan SETELAH backup_schema.sql berhasil dijalankan`);

    } catch (err) {
        console.error('❌ Gagal mengekspor data:', err.message);
        console.error(err);
    } finally {
        client.release();
        await pool.end();
    }
}

exportData();
