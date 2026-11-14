/**
 *  STRESS TEST - Pengujian Kapasitas dan Titik Jenuh Server Dispatch Ambulans
 * ==============================================================================
 * 
 *  Mengirim panggilan darurat secara bersamaan ke server melalui MQTT
 *  untuk mengukur kapasitas pemrosesan, response time, dan stabilitas server.
 * 
 *  Cara pakai: node stress-test.js
 * ==============================================================================
 */

const mqtt = require('mqtt');
const { Pool } = require('pg');
const fs = require('fs');
require('dotenv').config();

// ===================== KONFIGURASI =====================
// Tahapan jumlah panggilan bersamaan per batch
const STAGES = process.env.TEST_STAGES
    ? process.env.TEST_STAGES.split(',').map(s => parseInt(s.trim()))
    : [10, 25, 50, 75, 100];

// Daftar ID pasien bervariasi untuk pengujian (menghindari ID tunggal seragam)
const ID_PASIEN_LIST = process.env.TEST_PATIENT_IDS
    ? process.env.TEST_PATIENT_IDS.split(',').map(s => parseInt(s.trim()))
    : [45, 46, 47, 48, 49, 50, 51, 52, 53, 54];

// Durasi jendela observasi pemrosesan (detik)
const WAIT_PROCESSING_SEC = parseInt(process.env.WAIT_PROCESSING_SEC || '35', 10);

// Durasi jeda antar tahap untuk stabilisasi sistem (detik)
const WAIT_BETWEEN_STAGES_SEC = parseInt(process.env.WAIT_BETWEEN_STAGES_SEC || '180', 10);
const WAIT_STAGE_LABEL = process.env.WAIT_STAGE_LABEL || 'Jeda stabilisasi antar tahap';

// Koordinat pusat sebaran uji (Kota Malang)
const BASE_LAT = parseFloat(process.env.BASE_LAT || '-7.945');
const BASE_LON = parseFloat(process.env.BASE_LON || '112.615');
const COORD_SPREAD = parseFloat(process.env.COORD_SPREAD || '0.08'); // ~4km radius

// Flag simulasi konfirmasi driver (opsional, untuk menguji batas kehabisan armada)
const SIMULATE_DRIVER_ACCEPT = process.env.SIMULATE_DRIVER_ACCEPT === 'true';
// =======================================================

function randomCoord(base, spread) {
    return parseFloat((base + (Math.random() - 0.5) * spread).toFixed(7));
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function timestampIso() {
    return new Date().toISOString();
}

function timestampDisplay() {
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
    console.log(` Waktu Mulai        : ${timestampDisplay()}`);
    console.log(` Tahapan            : ${STAGES.join(', ')} panggilan`);
    console.log(` Daftar ID Pasien   : ${ID_PASIEN_LIST.join(', ')} (${ID_PASIEN_LIST.length} IDs)`);
    console.log(` Jendela Observasi  : ${WAIT_PROCESSING_SEC} detik`);
    console.log(` Jeda Antar Tahap   : ${WAIT_BETWEEN_STAGES_SEC} detik`);
    console.log(` Simulasi Terima    : ${SIMULATE_DRIVER_ACCEPT ? 'AKTIF (Driver mengirim DITERIMA)' : 'NON-AKTIF (Offer-only)'}`);
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
    console.log(`[SETUP] Menghubungkan ke Database...`);
    const dbPool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false },
        max: 5
    });
    await dbPool.query('SELECT 1'); // Test connection
    console.log(`[SETUP] Database Terhubung.`);

    // --- 3. Subscribe & State Tracking Penugasan Driver ---
    // Tracking per call ID untuk membedakan penugasan pertama dari re-offer
    const assignedCallIds = new Set();
    let uniqueAssignmentCount = 0;
    let reofferCount = 0;
    const assignmentTimes = [];
    let stageStartTime = 0;

    mqttClient.subscribe('ambulans/tugas/+', { qos: 1 });
    mqttClient.on('message', async (topic, message) => {
        if (!topic.startsWith('ambulans/tugas/')) return;

        let callId = null;
        let driverId = topic.split('/')[2];
        try {
            const payload = JSON.parse(message.toString());
            callId = payload.id_panggilan;
        } catch (e) {
            // Fallback jika payload bukan JSON
        }

        const elapsed = Date.now() - stageStartTime;

        if (callId) {
            if (!assignedCallIds.has(callId)) {
                // Penugasan pertama untuk ID panggilan ini
                assignedCallIds.add(callId);
                uniqueAssignmentCount++;
                assignmentTimes.push(elapsed);
            } else {
                // Penugasan kedua atau lebih untuk ID yang sama = re-offer
                reofferCount++;
            }
        } else {
            // Jika ID panggilan tidak dapat diparsing
            uniqueAssignmentCount++;
            assignmentTimes.push(elapsed);
        }

        // Jika simulasi terima driver aktif, kirim balik konfirmasi diterima
        if (SIMULATE_DRIVER_ACCEPT && callId && driverId) {
            const confirmationPayload = {
                id_panggilan: callId,
                id_ambulans: parseInt(driverId, 10),
                status: 'diterima'
            };
            mqttClient.publish(
                'ambulans/respons/konfirmasi',
                JSON.stringify(confirmationPayload),
                { qos: 1 }
            );
        }
    });

    // --- 4. Jalankan pengujian per tahap ---
    const allResults = [];

    for (let stage = 0; stage < STAGES.length; stage++) {
        const count = STAGES[stage];
        assignedCallIds.clear();
        uniqueAssignmentCount = 0;
        reofferCount = 0;
        assignmentTimes.length = 0;

        const stageStartIso = timestampIso();

        console.log(`\n================================================================`);
        console.log(` TAHAP ${stage + 1}/${STAGES.length}: ${count} PANGGILAN BERSAMAAN`);
        console.log(` Waktu Mulai Tahap: ${stageStartIso}`);
        console.log(`================================================================`);

        // Reset kondisi server
        console.log(`  [RESET] Mengembalikan status driver ke AVAILABLE...`);
        await dbPool.query(`
            UPDATE ambulans SET status_operasional = 'AVAILABLE' 
            WHERE status_operasional IN ('BUSY', 'WAITING_FOR_DRIVER')
        `);

        console.log(`  [RESET] Membatalkan panggilan aktif sebelumnya...`);
        await dbPool.query(`
            UPDATE transaksi_panggilan SET status_panggilan = 'CANCELLED' 
            WHERE status_panggilan IN ('PENDING', 'WAITING_FOR_DRIVER')
        `);

        await sleep(3000); // Beri waktu server stabil

        // Catat jumlah panggilan SEBELUM test
        const countBefore = (await dbPool.query(`SELECT COUNT(*)::int as total FROM transaksi_panggilan`)).rows[0].total;
        stageStartTime = Date.now();

        // Publish N pesan secara bersamaan dengan variasi ID Pasien
        console.log(`  [PUBLISH] Mengirim ${count} panggilan secara bersamaan...`);
        const publishPromises = [];
        for (let i = 0; i < count; i++) {
            const currentPatientId = ID_PASIEN_LIST[i % ID_PASIEN_LIST.length];
            const payload = {
                id_pasien: currentPatientId,
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

        // Tunggu server memproses dalam jendela observasi
        await countdown(WAIT_PROCESSING_SEC, 'Menunggu jendela observasi');

        const totalDuration = Date.now() - stageStartTime;
        const stageEndIso = timestampIso();

        // Query jumlah panggilan SESUDAH test
        const countAfter = (await dbPool.query(`SELECT COUNT(*)::int as total FROM transaksi_panggilan`)).rows[0].total;
        const totalMasukDB = countAfter - countBefore;

        // Query status breakdown untuk panggilan terbaru pada tahap ini
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

        const noDrivers = (statusCounts['NO_DRIVERS'] || 0) + (statusCounts['NO_DRIVERS_AVAILABLE'] || 0);
        const apiFailed = statusCounts['API_FAILED'] || 0;
        const failed = statusCounts['FAILED'] || 0;
        const pending = statusCounts['PENDING'] || 0;
        const outsideWindow = pending; // Panggilan yang belum sempat diselesaikan dalam jendela observasi

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
            waktuMulaiIso: stageStartIso,
            waktuSelesaiIso: stageEndIso,
            waktuPublishMs: publishDuration,
            waktuTotalMs: totalDuration,
            masukDB: totalMasukDB,
            terproses: totalTerproses,
            driverDitugaskanUnik: uniqueAssignmentCount,
            jumlahReoffer: reofferCount,
            noDrivers: noDrivers,
            apiFailed: apiFailed,
            gagal: failed,
            pending: pending,
            diLuarJendela: outsideWindow,
            tingkatProses: tingkatProses,
            avgResponseMs: avgResponseMs,
            minResponseMs: minResponseMs,
            maxResponseMs: maxResponseMs,
            statusDetail: JSON.stringify(statusCounts)
        };
        allResults.push(result);

        // Tampilkan hasil tahap ini
        console.log(`\n  ---- HASIL TAHAP ${stage + 1} ----`);
        console.log(`  Waktu Mulai            : ${stageStartIso}`);
        console.log(`  Waktu Selesai          : ${stageEndIso}`);
        console.log(`  Panggilan Dikirim      : ${count}`);
        console.log(`  Masuk ke Database      : ${totalMasukDB}`);
        console.log(`  Terproses              : ${totalTerproses}`);
        console.log(`  Driver Ditugaskan (Unik): ${uniqueAssignmentCount}`);
        console.log(`  Re-offer Terdeteksi    : ${reofferCount}`);
        console.log(`  No Drivers Available   : ${noDrivers}`);
        console.log(`  API Failed             : ${apiFailed}`);
        console.log(`  Gagal / Error          : ${failed}`);
        console.log(`  Masih Pending          : ${pending}`);
        console.log(`  Di Luar Jendela Waktu  : ${outsideWindow}`);
        console.log(`  Tingkat Pemrosesan     : ${tingkatProses}%`);
        console.log(`  Waktu Publish          : ${publishDuration} ms`);
        if (assignmentTimes.length > 0) {
            console.log(`  Respons Tercepat       : ${minResponseMs} ms`);
            console.log(`  Respons Terlambat      : ${maxResponseMs} ms`);
            console.log(`  Rata-rata Respons      : ${avgResponseMs} ms`);
        }
        console.log(`  Status Detail          : ${result.statusDetail}`);

        // Cleanup: reset status sebelum jeda tahap
        await dbPool.query(`
            UPDATE ambulans SET status_operasional = 'AVAILABLE' 
            WHERE status_operasional IN ('BUSY', 'WAITING_FOR_DRIVER')
        `);

        if (stage < STAGES.length - 1) {
            console.log(``);
            await countdown(WAIT_BETWEEN_STAGES_SEC, WAIT_STAGE_LABEL);
        }
    }

    // --- 5. Laporan Akhir ---
    console.log(`\n\n================================================================`);
    console.log(` LAPORAN AKHIR - PENGUJIAN TITIK JENUH SERVER`);
    console.log(` Waktu Selesai: ${timestampDisplay()}`);
    console.log(`================================================================\n`);

    // Header tabel
    console.log(`${''.padEnd(100, '-')}`);
    console.log(` ${'Tahap'.padEnd(6)}| ${'Kirim'.padStart(6)} | ${'MasukDB'.padStart(8)} | ${'Assigned'.padStart(9)} | ${'Reoffer'.padStart(8)} | ${'Pub(ms)'.padStart(8)} | ${'Min(ms)'.padStart(8)} | ${'Max(ms)'.padStart(8)} | ${'Avg(ms)'.padStart(8)}`);
    console.log(`${''.padEnd(100, '-')}`);

    allResults.forEach(r => {
        const row = [
            `T${r.tahap}`.padEnd(6),
            String(r.jumlahDikirim).padStart(6),
            String(r.masukDB).padStart(8),
            String(r.driverDitugaskanUnik).padStart(9),
            String(r.jumlahReoffer).padStart(8),
            String(r.waktuPublishMs).padStart(8),
            String(r.minResponseMs).padStart(8),
            String(r.maxResponseMs).padStart(8),
            String(r.avgResponseMs).padStart(8),
        ];
        console.log(` ${row.join(' | ')}`);
    });
    console.log(`${''.padEnd(100, '-')}`);

    // --- 6. Export CSV Lengkap ---
    const csvHeaders = [
        'Tahap',
        'Jumlah Dikirim',
        'Waktu Mulai (ISO 8601)',
        'Waktu Selesai (ISO 8601)',
        'Waktu Publish (ms)',
        'Waktu Total (ms)',
        'Masuk DB',
        'Terproses',
        'Driver Ditugaskan Unik',
        'Jumlah Reoffer',
        'No Drivers Available',
        'API Failed',
        'Gagal Error',
        'Masih Pending',
        'Di Luar Jendela',
        'Tingkat Proses (%)',
        'Respons Min (ms)',
        'Respons Max (ms)',
        'Respons Avg (ms)',
        'Status Detail JSON'
    ];

    const csvRows = allResults.map(r => [
        r.tahap,
        r.jumlahDikirim,
        `"${r.waktuMulaiIso}"`,
        `"${r.waktuSelesaiIso}"`,
        r.waktuPublishMs,
        r.waktuTotalMs,
        r.masukDB,
        r.terproses,
        r.driverDitugaskanUnik,
        r.jumlahReoffer,
        r.noDrivers,
        r.apiFailed,
        r.gagal,
        r.pending,
        r.diLuarJendela,
        r.tingkatProses,
        r.minResponseMs,
        r.maxResponseMs,
        r.avgResponseMs,
        `"${r.statusDetail.replace(/"/g, '""')}"`
    ].join(','));

    const csvContent = '\uFEFF' + csvHeaders.join(',') + '\n' + csvRows.join('\n') + '\n';
    const csvFilename = `stress-test-result_${new Date().toISOString().slice(0, 10)}.csv`;
    fs.writeFileSync(csvFilename, csvContent, 'utf8');
    console.log(`\n  Hasil lengkap seluruh metrik disimpan ke file: ${csvFilename}`);

    // Cleanup koneksi
    mqttClient.end();
    await dbPool.end();
    process.exit(0);
}

// Hanya jalankan jika dieksekusi langsung
if (require.main === module) {
    main().catch(err => {
        console.error('\n[FATAL]', err.message);
        process.exit(1);
    });
}

module.exports = {
    randomCoord,
    timestampIso,
    main
};
