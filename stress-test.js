/**
 *  STRESS TEST - Pengujian Titik Jenuh Server Dispatch Ambulans
 * ================================================================
 * 
 *  Mengirim panggilan darurat secara bersamaan ke server melalui
 *  MQTT untuk mengukur kapasitas pemrosesan server.
 * 
 *  Cara pakai: node stress-test.js
 * ================================================================
 */

const mqtt = require('mqtt');
const { Pool } = require('pg');
const fs = require('fs');
require('dotenv').config();

// ===================== KONFIGURASI =====================
const STAGES = [10, 25, 50, 75, 100];        // Jumlah panggilan per tahap secara berurutan
const ID_PASIEN = 45;                        // ID pasien valid di database (ganti jika perlu)
const WAIT_PROCESSING_SEC = 35;              // Waktu tunggu server memproses (detik)
const WAIT_BETWEEN_STAGES_SEC = 180;         // Jeda 3 MENIT antar tahap (detik)
const BASE_LAT = -7.945;                     // Titik tengah koordinat pengujian
const BASE_LON = 112.615;                    // Titik tengah koordinat pengujian
const COORD_SPREAD = 0.08;                   // Sebaran koordinat (radius ~4km)
// =======================================================

function randomCoord(base, spread) {
    return parseFloat((base + (Math.random() - 0.5) * spread).toFixed(7));
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function timestamp() {
    return new Date().toISOString().replace('T', ' ').substring(0, 19);
}

// Countdown timer yang ditampilkan di terminal
async function countdown(seconds, label) {
    for (let i = seconds; i > 0; i--) {
        process.stdout.write(`\r  ${label}: ${i} detik tersisa...   `);
        await sleep(1000);
    }
    process.stdout.write(`\r  ${label}: Selesai.                    \n`);
}

async function main() {
    console.log(`\n================================================================`);
    console.log(` STRESS TEST - PENGUJIAN TITIK JENUH SERVER`);
    console.log(` Waktu Mulai : ${timestamp()}`);
    console.log(` Tahapan     : ${STAGES.join(', ')} panggilan`);
    console.log(` ID Pasien   : ${ID_PASIEN}`);
    console.log(`================================================================\n`);

    // --- 1. Koneksi MQTT ---
    console.log(`[SETUP] Menghubungkan ke MQTT Broker...`);
    const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL);

    await new Promise((resolve, reject) => {
        mqttClient.on('connect', () => {
            console.log(`[SETUP] MQTT Terhubung.`);
            resolve();
        });
        mqttClient.on('error', (err) => {
            console.error(`[SETUP] MQTT Gagal:`, err.message);
            reject(err);
        });
    });

    // --- 2. Koneksi Database ---
    console.log(`[SETUP] Menghubungkan ke Database Neon...`);
    const dbPool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false },
        max: 5
    });
    await dbPool.query('SELECT 1'); // Test connection
    console.log(`[SETUP] Database Terhubung.`);

    // --- 3. Subscribe untuk mendeteksi penugasan driver ---
    let assignmentCount = 0;
    const assignmentTimes = [];
    let stageStartTime = 0;

    mqttClient.subscribe('ambulans/tugas/+', { qos: 1 });
    mqttClient.on('message', (topic) => {
        if (topic.startsWith('ambulans/tugas/')) {
            assignmentCount++;
            assignmentTimes.push(Date.now() - stageStartTime);
        }
    });

    // --- 4. Jalankan pengujian per tahap ---
    const allResults = [];

    for (let stage = 0; stage < STAGES.length; stage++) {
        const count = STAGES[stage];
        assignmentCount = 0;
        assignmentTimes.length = 0;

        console.log(`\n================================================================`);
        console.log(` TAHAP ${stage + 1}/${STAGES.length}: ${count} PANGGILAN BERSAMAAN`);
        console.log(`================================================================`);

        // Reset kondisi server
        console.log(`  [RESET] Mengembalikan semua driver ke AVAILABLE...`);
        await dbPool.query(`
            UPDATE ambulans SET status_operasional = 'AVAILABLE' 
            WHERE status_operasional IN ('BUSY', 'WAITING_FOR_DRIVER')
        `);

        console.log(`  [RESET] Membatalkan panggilan aktif...`);
        await dbPool.query(`
            UPDATE transaksi_panggilan SET status_panggilan = 'CANCELLED' 
            WHERE status_panggilan IN ('PENDING', 'WAITING_FOR_DRIVER')
        `);

        await sleep(3000); // Beri waktu server stabil

        // Catat jumlah panggilan SEBELUM test (lebih akurat dari timestamp)
        const countBefore = (await dbPool.query(`SELECT COUNT(*)::int as total FROM transaksi_panggilan`)).rows[0].total;
        stageStartTime = Date.now();

        // Publish N pesan secara bersamaan
        console.log(`  [PUBLISH] Mengirim ${count} panggilan secara bersamaan...`);
        const publishPromises = [];
        for (let i = 0; i < count; i++) {
            const payload = {
                id_pasien: ID_PASIEN,
                lokasi_pasien_lat: randomCoord(BASE_LAT, COORD_SPREAD),
                lokasi_pasien_lon: randomCoord(BASE_LON, COORD_SPREAD),
                jenis_layanan: "DARURAT",
                kondisi: `Stress Test T${stage + 1} #${i + 1}`
            };
            publishPromises.push(new Promise((resolve) => {
                mqttClient.publish('panggilan/masuk', JSON.stringify(payload), { qos: 1 }, resolve);
            }));
        }
        await Promise.all(publishPromises);

        const publishDuration = Date.now() - stageStartTime;
        console.log(`  [PUBLISH] Selesai dalam ${publishDuration} ms`);

        // Tunggu server memproses
        await countdown(WAIT_PROCESSING_SEC, 'Menunggu server');

        const totalDuration = Date.now() - stageStartTime;

        // Query jumlah panggilan SESUDAH test
        const countAfter = (await dbPool.query(`SELECT COUNT(*)::int as total FROM transaksi_panggilan`)).rows[0].total;
        const totalMasukDB = countAfter - countBefore;

        // Query status breakdown untuk panggilan terbaru
        const dbResult = await dbPool.query(`
            SELECT status_panggilan, COUNT(*)::int as jumlah 
            FROM (
                SELECT status_panggilan FROM transaksi_panggilan 
                ORDER BY id_panggilan DESC 
                LIMIT $1
            ) sub
            GROUP BY status_panggilan
            ORDER BY jumlah DESC
        `, [totalMasukDB > 0 ? totalMasukDB : count]);

        const statusCounts = {};
        dbResult.rows.forEach(row => {
            statusCounts[row.status_panggilan] = row.jumlah;
        });

        const assigned = (statusCounts['WAITING_FOR_DRIVER'] || 0) + (statusCounts['ON_THE_WAY'] || 0);
        const noDrivers = (statusCounts['NO_DRIVERS'] || 0) + (statusCounts['NO_DRIVERS_AVAILABLE'] || 0);
        const apiFailed = statusCounts['API_FAILED'] || 0;
        const failed = statusCounts['FAILED'] || 0;
        const pending = statusCounts['PENDING'] || 0;
        const cancelled = statusCounts['CANCELLED'] || 0;

        const totalTerproses = totalMasukDB - pending;
        const tingkatProses = totalMasukDB > 0 ? ((totalTerproses / count) * 100).toFixed(1) : '0.0';
        const avgResponseMs = assignmentTimes.length > 0
            ? Math.round(assignmentTimes.reduce((a, b) => a + b, 0) / assignmentTimes.length)
            : '-';
        const minResponseMs = assignmentTimes.length > 0 ? Math.min(...assignmentTimes) : '-';
        const maxResponseMs = assignmentTimes.length > 0 ? Math.max(...assignmentTimes) : '-';

        const result = {
            tahap: stage + 1,
            jumlahDikirim: count,
            waktuPublishMs: publishDuration,
            waktuTotalMs: totalDuration,
            masukDB: totalMasukDB,
            terproses: totalTerproses,
            driverDitugaskan: assignmentCount,
            noDrivers: noDrivers,
            apiFailed: apiFailed,
            gagal: failed,
            pending: pending,
            tingkatProses: tingkatProses,
            avgResponseMs: avgResponseMs,
            minResponseMs: minResponseMs,
            maxResponseMs: maxResponseMs,
            statusDetail: statusCounts
        };
        allResults.push(result);

        // Tampilkan hasil tahap ini
        console.log(`\n  ---- HASIL TAHAP ${stage + 1} ----`);
        console.log(`  Panggilan dikirim      : ${count}`);
        console.log(`  Masuk ke Database      : ${totalMasukDB}`);
        console.log(`  Terproses              : ${totalTerproses}`);
        console.log(`  Driver Ditugaskan      : ${assignmentCount}`);
        console.log(`  No Drivers Available   : ${noDrivers}`);
        console.log(`  API Failed             : ${apiFailed}`);
        console.log(`  Gagal / Error          : ${failed}`);
        console.log(`  Masih Pending          : ${pending}`);
        console.log(`  Tingkat Pemrosesan     : ${tingkatProses}%`);
        console.log(`  Waktu Publish          : ${publishDuration} ms`);
        if (assignmentTimes.length > 0) {
            console.log(`  Respons Tercepat       : ${minResponseMs} ms`);
            console.log(`  Respons Terlambat      : ${maxResponseMs} ms`);
            console.log(`  Rata-rata Respons      : ${avgResponseMs} ms`);
        }
        console.log(`  Status Detail          : ${JSON.stringify(statusCounts)}`);

        // Cleanup: reset untuk tahap berikutnya
        await dbPool.query(`
            UPDATE ambulans SET status_operasional = 'AVAILABLE' 
            WHERE status_operasional IN ('BUSY', 'WAITING_FOR_DRIVER')
        `);

        if (stage < STAGES.length - 1) {
            console.log(``);
            await countdown(WAIT_BETWEEN_STAGES_SEC, 'Jeda antar tahap (Screenshot Time)');
        }
    }

    // --- 5. Laporan Akhir ---
    console.log(`\n\n================================================================`);
    console.log(` LAPORAN AKHIR - PENGUJIAN TITIK JENUH SERVER`);
    console.log(` Waktu Selesai: ${timestamp()}`);
    console.log(`================================================================\n`);

    // Header tabel
    console.log(`${''.padEnd(75, '-')}`);
    console.log(` ${'Tahap'.padEnd(6)}| ${'Kirim'.padStart(6)} | ${'Pub(ms)'.padStart(8)} | ${'Min(ms)'.padStart(8)} | ${'Max(ms)'.padStart(8)} | ${'Avg(ms)'.padStart(8)}`);
    console.log(`${''.padEnd(75, '-')}`);

    allResults.forEach(r => {
        const row = [
            `T${r.tahap}`.padEnd(6),
            String(r.jumlahDikirim).padStart(6),
            String(r.waktuPublishMs).padStart(8),
            String(r.minResponseMs).padStart(8),
            String(r.maxResponseMs).padStart(8),
            String(r.avgResponseMs).padStart(8),
        ];
        console.log(` ${row.join(' | ')}`);
    });
    console.log(`${''.padEnd(75, '-')}`);

    // Temuan penting dari response time
    console.log(`\n  Catatan Respons Penugasan Driver:`);
    allResults.forEach(r => {
        if (r.avgResponseMs !== '-') {
            console.log(`  - T${r.tahap} (${r.jumlahDikirim} panggilan): Avg ${r.avgResponseMs}ms, Min ${r.minResponseMs}ms, Max ${r.maxResponseMs}ms`);
        }
    });

    // --- 6. Export CSV ---
    const csvHeader = 'Tahap,Jumlah Dikirim,Waktu Publish (ms),Respons Min (ms),Respons Max (ms),Respons Avg (ms)\n';
    const csvRows = allResults.map(r =>
        `${r.tahap},${r.jumlahDikirim},${r.waktuPublishMs},${r.minResponseMs},${r.maxResponseMs},${r.avgResponseMs}`
    ).join('\n');

    const csvFilename = `stress-test-result_${new Date().toISOString().slice(0, 10)}.csv`;
    fs.writeFileSync(csvFilename, '\uFEFF' + csvHeader + csvRows, 'utf8');
    console.log(`\n  Hasil disimpan ke: ${csvFilename}`);

    // --- 7. Analisis Otomatis ---
    console.log(`\n================================================================`);
    console.log(` ANALISIS`);
    console.log(`================================================================`);

    const lastFullProcess = allResults.filter(r => parseFloat(r.tingkatProses) >= 95);
    const firstDrop = allResults.find(r => parseFloat(r.tingkatProses) < 95);

    if (lastFullProcess.length > 0) {
        const best = lastFullProcess[lastFullProcess.length - 1];
        console.log(`  Server mampu memproses ${best.jumlahDikirim} panggilan bersamaan`);
        console.log(`  dengan tingkat keberhasilan ${best.tingkatProses}%.`);
    }
    if (firstDrop) {
        console.log(`  Penurunan performa mulai terdeteksi pada ${firstDrop.jumlahDikirim} panggilan`);
        console.log(`  (tingkat pemrosesan turun ke ${firstDrop.tingkatProses}%).`);
    }
    if (!firstDrop) {
        console.log(`  Server berhasil memproses semua tahap tanpa penurunan signifikan.`);
    }

    console.log(`\n================================================================\n`);

    // Cleanup
    mqttClient.end();
    await dbPool.end();
    process.exit(0);
}

main().catch(err => {
    console.error('\n[FATAL]', err.message);
    process.exit(1);
});
