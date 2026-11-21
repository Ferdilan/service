/**
 *  STRESS TEST - Capacity and Saturation Testing of Ambulance Dispatch Server
 * ==============================================================================
 * 
 *  Concurrently sends emergency calls to server via MQTT
 *  to measure processing capacity, response time, and server stability.
 * 
 *  Usage: node stress-test.js
 * ==============================================================================
 */

const mqtt = require('mqtt');
const { Pool } = require('pg');
const fs = require('fs');
require('dotenv').config();

// ===================== CONFIGURATION =====================
// Stages of concurrent calls per batch
const STAGES = process.env.TEST_STAGES
    ? process.env.TEST_STAGES.split(',').map(s => parseInt(s.trim()))
    : [10, 25, 50, 75, 100];

// Varied patient ID list for testing (avoiding uniform single ID)
const ID_PASIEN_LIST = process.env.TEST_PATIENT_IDS
    ? process.env.TEST_PATIENT_IDS.split(',').map(s => parseInt(s.trim()))
    : [45, 46, 47, 48, 49, 50, 51, 52, 53, 54];

// Observation processing window duration (seconds)
const WAIT_PROCESSING_SEC = parseInt(process.env.WAIT_PROCESSING_SEC || '35', 10);

// Inter-stage cooldown duration for system stabilization (seconds)
const WAIT_BETWEEN_STAGES_SEC = parseInt(process.env.WAIT_BETWEEN_STAGES_SEC || '180', 10);
const WAIT_STAGE_LABEL = process.env.WAIT_STAGE_LABEL || 'Inter-stage stabilization cooldown';

// Test distribution center coordinates (Malang City)
const BASE_LAT = parseFloat(process.env.BASE_LAT || '-7.945');
const BASE_LON = parseFloat(process.env.BASE_LON || '112.615');
const COORD_SPREAD = parseFloat(process.env.COORD_SPREAD || '0.08'); // ~4km radius

// Driver acceptance simulation flag (optional, to test fleet exhaustion limits)
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

// Terminal countdown timer display
async function countdown(seconds, label) {
    for (let i = seconds; i > 0; i--) {
        process.stdout.write(`\r  ${label}: ${i} seconds remaining...   `);
        await sleep(1000);
    }
    process.stdout.write(`\r  ${label}: Completed.                    \n`);
}

async function main() {
    console.log(`\n================================================================`);
    console.log(` STRESS TEST - SERVER CAPACITY & SATURATION TESTING`);
    console.log(` Start Time         : ${timestampDisplay()}`);
    console.log(` Stages             : ${STAGES.join(', ')} calls`);
    console.log(` Patient ID List    : ${ID_PASIEN_LIST.join(', ')} (${ID_PASIEN_LIST.length} IDs)`);
    console.log(` Observation Window : ${WAIT_PROCESSING_SEC} seconds`);
    console.log(` Inter-stage Gap    : ${WAIT_BETWEEN_STAGES_SEC} seconds`);
    console.log(` Accept Simulation  : ${SIMULATE_DRIVER_ACCEPT ? 'ACTIVE (Driver sends ACCEPTED)' : 'INACTIVE (Offer-only)'}`);
    console.log(`================================================================\n`);

    // --- 1. MQTT Connection ---
    console.log(`[SETUP] Connecting to MQTT Broker...`);
    const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL);

    await new Promise((resolve, reject) => {
        mqttClient.on('connect', () => {
            console.log(`[SETUP] MQTT Connected.`);
            resolve();
        });
        mqttClient.on('error', (err) => {
            console.error(`[SETUP] MQTT Failed:`, err.message);
            reject(err);
        });
    });

    // --- 2. Database Connection ---
    console.log(`[SETUP] Connecting to Database...`);
    const dbPool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false },
        max: 5
    });
    await dbPool.query('SELECT 1'); // Test connection
    console.log(`[SETUP] Database Connected.`);

    // --- 3. Subscribe & State Tracking for Driver Assignments ---
    // Track per call ID to distinguish initial assignments from re-offers
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
            // Fallback if payload is not JSON
        }

        const elapsed = Date.now() - stageStartTime;

        if (callId) {
            if (!assignedCallIds.has(callId)) {
                // First assignment for this call ID
                assignedCallIds.add(callId);
                uniqueAssignmentCount++;
                assignmentTimes.push(elapsed);
            } else {
                // Second or subsequent assignment for the same ID = re-offer
                reofferCount++;
            }
        } else {
            // If call ID could not be parsed
            uniqueAssignmentCount++;
            assignmentTimes.push(elapsed);
        }

        // If driver acceptance simulation is active, publish acceptance confirmation back
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

    // --- 4. Execute test per stage ---
    const allResults = [];

    for (let stage = 0; stage < STAGES.length; stage++) {
        const count = STAGES[stage];
        assignedCallIds.clear();
        uniqueAssignmentCount = 0;
        reofferCount = 0;
        assignmentTimes.length = 0;

        const stageStartIso = timestampIso();

        console.log(`\n================================================================`);
        console.log(` STAGE ${stage + 1}/${STAGES.length}: ${count} CONCURRENT CALLS`);
        console.log(` Stage Start Time: ${stageStartIso}`);
        console.log(`================================================================`);

        // Reset server state
        console.log(`  [RESET] Reverting driver status to AVAILABLE...`);
        await dbPool.query(`
            UPDATE ambulans SET status_operasional = 'AVAILABLE' 
            WHERE status_operasional IN ('BUSY', 'WAITING_FOR_DRIVER')
        `);

        console.log(`  [RESET] Cancelling previous active calls...`);
        await dbPool.query(`
            UPDATE transaksi_panggilan SET status_panggilan = 'CANCELLED' 
            WHERE status_panggilan IN ('PENDING', 'WAITING_FOR_DRIVER')
        `);

        await sleep(3000); // Allow server to stabilize

        // Record call count BEFORE test
        const countBefore = (await dbPool.query(`SELECT COUNT(*)::int as total FROM transaksi_panggilan`)).rows[0].total;
        stageStartTime = Date.now();

        // Publish N messages concurrently with varying Patient IDs
        console.log(`  [PUBLISH] Sending ${count} calls concurrently...`);
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
        console.log(`  [PUBLISH] Completed in ${publishDuration} ms`);

        // Wait for server processing within observation window
        await countdown(WAIT_PROCESSING_SEC, 'Waiting for observation window');

        const totalDuration = Date.now() - stageStartTime;
        const stageEndIso = timestampIso();

        // Query call count AFTER test
        const countAfter = (await dbPool.query(`SELECT COUNT(*)::int as total FROM transaksi_panggilan`)).rows[0].total;
        const totalMasukDB = countAfter - countBefore;

        // Query status breakdown for latest calls in this stage
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
        const outsideWindow = pending; // Calls not completed within the observation window

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

        // Display results for this stage
        console.log(`\n  ---- STAGE ${stage + 1} RESULTS ----`);
        console.log(`  Start Time             : ${stageStartIso}`);
        console.log(`  End Time               : ${stageEndIso}`);
        console.log(`  Calls Sent             : ${count}`);
        console.log(`  Recorded in DB         : ${totalMasukDB}`);
        console.log(`  Processed              : ${totalTerproses}`);
        console.log(`  Assigned Drivers (Unique): ${uniqueAssignmentCount}`);
        console.log(`  Re-offers Detected     : ${reofferCount}`);
        console.log(`  No Drivers Available   : ${noDrivers}`);
        console.log(`  API Failed             : ${apiFailed}`);
        console.log(`  Failed / Error         : ${failed}`);
        console.log(`  Still Pending          : ${pending}`);
        console.log(`  Outside Time Window    : ${outsideWindow}`);
        console.log(`  Processing Rate        : ${tingkatProses}%`);
        console.log(`  Publish Duration       : ${publishDuration} ms`);
        if (assignmentTimes.length > 0) {
            console.log(`  Fastest Response       : ${minResponseMs} ms`);
            console.log(`  Slowest Response       : ${maxResponseMs} ms`);
            console.log(`  Average Response       : ${avgResponseMs} ms`);
        }
        console.log(`  Status Detail          : ${result.statusDetail}`);

        // Cleanup: reset status before inter-stage gap
        await dbPool.query(`
            UPDATE ambulans SET status_operasional = 'AVAILABLE' 
            WHERE status_operasional IN ('BUSY', 'WAITING_FOR_DRIVER')
        `);

        if (stage < STAGES.length - 1) {
            console.log(``);
            await countdown(WAIT_BETWEEN_STAGES_SEC, WAIT_STAGE_LABEL);
        }
    }

    // --- 5. Final Report ---
    console.log(`\n\n================================================================`);
    console.log(` FINAL REPORT - SERVER CAPACITY & SATURATION TESTING`);
    console.log(` End Time: ${timestampDisplay()}`);
    console.log(`================================================================\n`);

    // Table header
    console.log(`${''.padEnd(100, '-')}`);
    console.log(` ${'Stage'.padEnd(6)}| ${'Sent'.padStart(6)} | ${'InDB'.padStart(8)} | ${'Assigned'.padStart(9)} | ${'Reoffer'.padStart(8)} | ${'Pub(ms)'.padStart(8)} | ${'Min(ms)'.padStart(8)} | ${'Max(ms)'.padStart(8)} | ${'Avg(ms)'.padStart(8)}`);
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

    // --- 6. Full CSV Export ---
    const csvHeaders = [
        'Stage',
        'Calls Sent',
        'Start Time (ISO 8601)',
        'End Time (ISO 8601)',
        'Publish Duration (ms)',
        'Total Duration (ms)',
        'Recorded in DB',
        'Processed',
        'Unique Drivers Assigned',
        'Reoffer Count',
        'No Drivers Available',
        'API Failed',
        'Failed Error',
        'Still Pending',
        'Outside Window',
        'Processing Rate (%)',
        'Min Response (ms)',
        'Max Response (ms)',
        'Avg Response (ms)',
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
    console.log(`\n  Complete metric results saved to file: ${csvFilename}`);

    // Connection cleanup
    mqttClient.end();
    await dbPool.end();
    process.exit(0);
}

// Only execute when run directly
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
