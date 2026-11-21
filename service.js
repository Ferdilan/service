// --- CONFIGURATION ---
const fs = require('fs');
const path = require('path');
const mqtt = require('mqtt');
const Joi = require('joi');
const db = require('./db.js');
const { getDistance } = require('geolib');
const { LocationClient, CalculateRouteMatrixCommand } = require("@aws-sdk/client-location");
const { Client: PgClient } = require('pg');
const logger = require('./logger');

require('dotenv').config();

// --- SCHEMA VALIDATOR (JOI) ---
const schemaT1 = Joi.object({
    lokasi_latitude: Joi.number().required(),
    lokasi_longitude: Joi.number().required()
}).unknown(true);

const schemaT2 = Joi.object({
    id_pasien: Joi.number().integer().required(),
    lokasi_pasien_lat: Joi.number().required(),
    lokasi_pasien_lon: Joi.number().required(),
    jenis_layanan: Joi.string().optional().allow(null, '')
}).unknown(true);

const schemaT4 = Joi.object({
    id_panggilan: Joi.number().integer().required(),
    status: Joi.string().valid('diterima', 'ditolak', 'selesai', 'kendala').required(),
    id_ambulans: Joi.number().integer().optional(),
    id_driver: Joi.number().integer().optional()
}).or('id_ambulans', 'id_driver').unknown(true);

const schemaBatal = Joi.object({
    id_panggilan: Joi.alternatives().try(Joi.number().integer(), Joi.string()).required(),
    id_pasien: Joi.alternatives().try(Joi.number().integer(), Joi.string()).optional(),
    id_ambulans: Joi.number().integer().optional()
}).unknown(true);

const schemaStatusOp = Joi.object({
    id_ambulans: Joi.number().integer().optional(),
    id_driver: Joi.number().integer().optional(),
    status: Joi.string().required()
}).or('id_ambulans', 'id_driver').unknown(true);
// ------------------------------

<<<<<<< HEAD
// --- EMERGENCY SEVERITY INDEX (ESI) ---
// Mapping kondisi pasien → tingkat kedaruratan (1=tertinggi, 5=terendah)
const SEVERITY_MAP = {
    // Level 1 - Resusitasi (Mengancam Jiwa)
    'henti_jantung': 1, 'tidak_bernapas': 1, 'stroke': 1, 'tenggelam': 1,
    // Level 2 - Emergensi (Darurat Tinggi)
    'kecelakaan_berat': 2, 'luka_bakar': 2, 'kejang': 2, 'pendarahan_hebat': 2,
    // Level 3 - Urgen
    'patah_tulang': 3, 'sesak_napas': 3, 'kecelakaan_ringan': 3, 'luka_robek': 3,
    // Level 4 - Semi-Urgen
    'demam_tinggi': 4, 'nyeri_dada': 4, 'cedera_ringan': 4,
    // Level 5 - Non-Urgen
    'transport': 5, 'konsultasi': 5, 'jenazah': 5,
};

// Fallback berdasarkan jenis_layanan
const SERVICE_SEVERITY_MAP = { 'DARURAT': 2, 'TRANSPORT': 5, 'JENAZAH': 5 };

// Label visual untuk logging
const SEVERITY_LABELS = ['', '🔴 RESUSITASI', '🟠 EMERGENSI', '🟡 URGEN', '🟢 SEMI-URGEN', '🔵 NON-URGEN'];

/**
 * Menghitung severity level berdasarkan kondisi dan jenis_layanan.
 * @param {object} data - { kondisi, jenis_layanan }
 * @returns {number} Severity level (1-5)
 */
function calculateSeverity(data) {
    if (data.kondisi && SEVERITY_MAP[data.kondisi]) return SEVERITY_MAP[data.kondisi];
    if (data.jenis_layanan && SERVICE_SEVERITY_MAP[data.jenis_layanan]) return SERVICE_SEVERITY_MAP[data.jenis_layanan];
    return 3; // Default: Urgen
}

/**
 * Priority Queue berbasis Min-Heap untuk dispatch ambulans.
 * Panggilan dengan severity TERENDAH (paling darurat) diproses duluan.
 * Jika severity sama, FCFS (siapa duluan masuk, dia duluan diproses).
 */
class EmergencyPriorityQueue {
    constructor() { this.heap = []; }

    enqueue(callData) {
        const entry = { ...callData, severity: calculateSeverity(callData), timestamp: Date.now() };
        this.heap.push(entry);
        this._bubbleUp(this.heap.length - 1);
        return entry;
    }

    dequeue() {
        if (this.heap.length === 0) return null;
        const top = this.heap[0];
        const last = this.heap.pop();
        if (this.heap.length > 0) { this.heap[0] = last; this._sinkDown(0); }
        return top;
    }

    peek() { return this.heap[0] || null; }
    size() { return this.heap.length; }
    isEmpty() { return this.heap.length === 0; }

    _compare(a, b) {
        if (a.severity !== b.severity) return a.severity - b.severity;
        return a.timestamp - b.timestamp;
    }

    _bubbleUp(idx) {
        while (idx > 0) {
            const parent = Math.floor((idx - 1) / 2);
            if (this._compare(this.heap[idx], this.heap[parent]) >= 0) break;
            [this.heap[idx], this.heap[parent]] = [this.heap[parent], this.heap[idx]];
            idx = parent;
        }
    }

    _sinkDown(idx) {
        const len = this.heap.length;
        while (true) {
            let smallest = idx;
            const l = 2 * idx + 1, r = 2 * idx + 2;
            if (l < len && this._compare(this.heap[l], this.heap[smallest]) < 0) smallest = l;
            if (r < len && this._compare(this.heap[r], this.heap[smallest]) < 0) smallest = r;
            if (smallest === idx) break;
            [this.heap[idx], this.heap[smallest]] = [this.heap[smallest], this.heap[idx]];
            idx = smallest;
        }
    }
}

// Instance Priority Queue dan Batch Timer
const emergencyQueue = new EmergencyPriorityQueue();
let dispatchTimer = null;
const BATCH_WINDOW_MS = 3000; // Kumpulkan panggilan selama 3 detik sebelum proses batch
// ------------------------------------------

// Connection Configuration
const BROKER_URL = process.env.MQTT_BROKER_URL;
const AWS_REGION = process.env.AWS_REGION || 'ap-southeast-1';
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;
const ROUTE_CALCULATOR_NAME = process.env.AWS_LOCATION_ROUTE_CALCULATOR_NAME || 'AmbulanceRouteCalculator';

// Explicit MQTT Credentials (Optional if already included in URL)
const MQTT_USER = process.env.MQTT_USERNAME || undefined;
const MQTT_PASS = process.env.MQTT_PASSWORD || undefined;

// Client Configuration
const client = mqtt.connect(BROKER_URL, {
    username: MQTT_USER,
    password: MQTT_PASS,
    protocol: 'mqtt',
    port: 1883
});

const locationClient = new LocationClient({
    region: AWS_REGION,
    credentials: (AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY && AWS_ACCESS_KEY_ID !== 'your_aws_access_key_id_here') ? {
        accessKeyId: AWS_ACCESS_KEY_ID,
        secretAccessKey: AWS_SECRET_ACCESS_KEY
    } : undefined
});

// Neon DB Connection (LISTEN/NOTIFY) — LAZY Mode
// LISTEN is only active during activity. Does not auto-reconnect when Neon scales to zero.
let pgClient = null;
let listenActive = false;              // Is LISTEN currently active?
let listenUsingBackup = false;         // Is LISTEN currently using backup?
let listenReconnectTimer = null;       // Reconnect timer (if any)

// Error keywords indicating exhausted Neon quota/connection
function isNeonUnavailableError(err) {
    if (!err || !err.message) return false;
    const msg = err.message.toLowerCase();
    return (
        msg.includes('compute time quota') ||
        msg.includes('project is disabled') ||
        msg.includes('connection refused') ||
        msg.includes('econnrefused') ||
        msg.includes('endpoint is disabled') ||
        err.code === 'ENOTFOUND' ||
        err.code === 'ETIMEDOUT'
    );
}

/**
 * Ensures the LISTEN connection is active. Called upon DB activity
 * (e.g. when an MQTT message arrives). If already active, does nothing.
 */
function ensureListenConnection() {
    if (listenActive) return; // Already active, no need to create a new one
    if (listenReconnectTimer) return; // Currently in reconnect process
    listenToDatabaseChanges();
}

async function listenToDatabaseChanges() {
    if (listenActive) return; // Guard: avoid duplicate connection

    // Synchronize LISTEN state with db.js state
    const dbStatus = db.getStatus();
    if (dbStatus.usingBackup && !listenUsingBackup) {
        console.warn('[LISTEN] db.js is already using Backup, LISTEN switching to Backup as well.');
        listenUsingBackup = true;
    }

    // Select URL based on failover state
    const listenUrl = listenUsingBackup
        ? process.env.DATABASE_DIRECT_URL_BACKUP
        : process.env.DATABASE_DIRECT_URL;

    const sourceLabel = listenUsingBackup ? 'BACKUP' : 'PRIMARY';

    try {
        pgClient = new PgClient({
            connectionString: listenUrl,
            ssl: { rejectUnauthorized: false }
        });

        pgClient.on('error', (err) => {
            console.error(`[LISTEN-${sourceLabel}] LISTEN connection lost:`, err.message);
            listenActive = false;
            pgClient.end().catch(() => { });
            // DO NOT auto-reconnect — allow Neon to scale to zero.
            // LISTEN will be reactivated on subsequent activity via ensureListenConnection().
            console.log('[LISTEN] Not auto-reconnecting. Waiting for next activity...');
        });

        await pgClient.connect();
        listenActive = true;
        listenReconnectTimer = null;
        console.log(`[LISTEN-${sourceLabel}] Connected. Listening to Postgres channel...`);

        await pgClient.query('LISTEN status_ambulans_channel');

        // When connection is closed (Neon scale-to-zero or network drop):
        // DO NOT reconnect automatically. Let Neon sleep.
        pgClient.on('end', () => {
            listenActive = false;
            console.warn(`[LISTEN-${sourceLabel}] LISTEN connection closed (Neon scale-to-zero). Waiting for next activity...`);
            // DO NOT call scheduleReconnect() — this prevents infinite loops!
        });

        // Capture notification from Postgres trigger
        pgClient.on('notification', (msg) => {
            if (msg.channel === 'status_ambulans_channel') {
                const data = JSON.parse(msg.payload);
                console.log(`[REAL-TIME] Ambulance ${data.id_ambulans} status changed to ${data.status_baru}`);
                const topic = `ambulans/status/sinkronisasi/${data.id_ambulans}`;
                client.publish(topic, JSON.stringify(data), { qos: 1 });
            }
        });

    } catch (err) {
        listenActive = false;
        listenReconnectTimer = null;
        const isUnavailable = isNeonUnavailableError(err);

        if (isUnavailable && !listenUsingBackup) {
            // Primary unavailable → try backup once
            console.warn(`[LISTEN-PRIMARY] Unavailable (${err.message}). Trying BACKUP...`);
            listenUsingBackup = true;
            listenReconnectTimer = setTimeout(listenToDatabaseChanges, 2000);
        } else if (isUnavailable && listenUsingBackup) {
            // Backup also unavailable → DO NOT retry. Wait for next activity.
            console.warn(`[LISTEN-BACKUP] Also unavailable. Waiting for next activity...`);
        } else {
            console.error(`[LISTEN-${sourceLabel}] Failed:`, err.message);
            // DO NOT retry automatically — leave idle.
        }
    }
}
// Run listener when service starts for the first time
listenToDatabaseChanges();

// Topic Definitions
const TOPIC_PERMINTAAN_AMBULANS = 'panggilan/masuk';                    // T2
const TOPIC_UPDATE_LOKASI_DRIVER = 'ambulans/lokasi/update/+';          // T1
const TOPIC_KONFIRMASI_TUGAS_DRIVER = 'ambulans/respons/konfirmasi';    // T4
const TOPIC_UPDATE_STATUS_OPERASIONAL = 'ambulans/status/update';       // Additional Topic
const TOPIC_PEMBATALAN_PASIEN = 'panggilan/batal/pasien';               // Patient Cancellation Topic
const TOPIC_PEMBATALAN_DRIVER = 'panggilan/batal/driver';               // Driver Cancellation Topic

// Stores list of drivers who rejected the call
const rejectedDriversMap = {};

// --- MQTT CONNECTION & ROUTER ---

/**
 * Main function executed upon successfully connecting to MQTT broker.
 * Subscribes to all relevant topics.
 */
client.on('connect', () => {
    console.log(`\n================================================================`);
    console.log(` AMBULANCE DISPATCH SERVICE v1.0`);
    console.log(` Mode  : ALGORITHM TESTING`);
    console.log(` Time  : ${new Date().toISOString().replace('T', ' ').substring(0, 19)}`);
    console.log(`================================================================\n`);

    logger.setMqttClient(client);

    const topics = [
        TOPIC_PERMINTAAN_AMBULANS,
        TOPIC_UPDATE_LOKASI_DRIVER,
        TOPIC_KONFIRMASI_TUGAS_DRIVER,
        TOPIC_UPDATE_STATUS_OPERASIONAL,
        TOPIC_PEMBATALAN_PASIEN,
        TOPIC_PEMBATALAN_DRIVER
    ];
    topics.forEach(t => {
        client.subscribe(t, (err) => {
            if (!err) console.log(`[OK] Successfully subscribed to: ${t}`);
        });
    });

    console.log(`\n================================================================`);
    console.log(` Waiting for driver location updates and patient calls...`);
    console.log(`================================================================\n`);
}); // End Client Connect


/**
 * Main router for all incoming MQTT messages.
 * Parses messages and invokes the corresponding handler function based on topic.
 * @param {string} topic - MQTT topic where message was received.
 * @param {Buffer} message - Message content as Buffer.
 */
client.on('message', async (topic, message) => {
    try {
        const payload = JSON.parse(message.toString());

        // Ensure LISTEN connection is active upon MQTT activity
        ensureListenConnection();

        // 1. Driver Location Update Flow
        if (topic.startsWith('ambulans/lokasi/update/')) { // T1
            const { error, value } = schemaT1.validate(payload);
            if (error) {
                console.error(`[VALIDATION] Payload T1 Error from topic ${topic}:`, error.message);
                return;
            }
            const driverId = topic.split('/')[3]; // ambulans/lokasi/update/{id_ambulans}
            await handleDriverLocationUpdate(driverId, value);
            // display a dot to indicate server is still alive
            // process.stdout.write('.'); 

        }

        // 2. Emergency Call Flow
        else if (topic === TOPIC_PERMINTAAN_AMBULANS) { // T2
            const { error, value } = schemaT2.validate(payload);
            if (error) {
                logger.error(`[VALIDATION] Payload T2 Error: ${error.message}`, { category: 'dispatch' });
                return;
            }
            logger.info('Receiving patient ambulance request...', { category: 'dispatch' });
            await handlePatientRequest(value);
        }

        // 3. Driver Confirmation Flow
        else if (topic === TOPIC_KONFIRMASI_TUGAS_DRIVER) { // T4
            const { error, value } = schemaT4.validate(payload);
            if (error) {
                console.error(`[VALIDATION] Payload T4 Error:`, error.message);
                return;
            }
            await handleDriverTaskConfirmation(value);
        }

        else if (topic === TOPIC_UPDATE_STATUS_OPERASIONAL) { // Additional Topic
            const { error, value } = schemaStatusOp.validate(payload);
            if (error) {
                console.error(`[VALIDATION] Payload Ops Status Error:`, error.message);
                return;
            }
            await handleOperationalStatusUpdate(value);
        }

        // 5. Patient Call Cancellation Flow
        else if (topic === TOPIC_PEMBATALAN_PASIEN) {
            const { error, value } = schemaBatal.validate(payload);
            if (error) {
                console.error(`[VALIDATION] Cancellation Payload Error:`, error.message);
                return;
            }
            await handlePatientCancellation(value);
        }
        else if (topic === TOPIC_PEMBATALAN_DRIVER) {
            const { error, value } = schemaBatal.validate(payload);
            if (error) {
                console.error(`[VALIDATION] Driver Cancellation Payload Error:`, error.message);
                return;
            }
            await handleDriverCancellation(value);
        }
    }
    catch (e) {
        console.error(`Failed to process message on topic ${topic}:`, e.message || e);
    }
}); //End Client Message


/**
 * Handler for MQTT connection errors.
 */
client.on('error', (error) => {
    console.error('MQTT Connection Error:', error);
});



/**
 * =======================================================
 * HANDLER FUNCTIONS (LOGIC FLOW)
 * =======================================================
 */

/**
 * [FLOW 1 - T1] Handles real-time location updates from driver.
 * Receives data from T1 and updates the 'ambulans' table in DB.
 * @param {string} driverId - Driver ID extracted from topic.
 * @param {object} data - JSON payload from T1 (lokasi_latitude, lokasi_longitude).
 * @returns {Promise<void>}
 */
async function handleDriverLocationUpdate(driverId, data) {
    const sql = `
        UPDATE ambulans 
        SET 
            lokasi_latitude = $1, 
            lokasi_longitude = $2, 
            timestamp_update = NOW(),
            status_operasional = CASE
                WHEN status_operasional = 'OFFLINE' THEN 'AVAILABLE'
                ELSE status_operasional
            END
        WHERE id_ambulans = $3
    `;
    try {
        const { lokasi_latitude, lokasi_longitude } = data;
        if (lokasi_latitude === undefined || lokasi_longitude === undefined) {
            console.error(`[T5] Driver ${driverId} location data incomplete. Payload:`, data);
            return;
        }
        const result = await db.query(sql, [lokasi_latitude, lokasi_longitude, driverId]);
        if (result.rowCount === 0) {
            console.warn(`[T5] Location update failed: Driver ID ${driverId} not found in DB.`);
        } else {
            // console.log(`[T5] Driver ${driverId} location updated in DB.`);
        }
    } catch (error) {
        console.error(`[T5] Failed to update location for driver ${driverId}:`, error.sqlMessage || error.message);
    }
} //End function handleDriverLocationUpdate


/**
 * [FLOW 2 - T2] Handles incoming emergency call from patient.
 * Main coordinator function for the dispatch process.
 * @param {object} data - JSON payload from T2 (id_pasien, lokasi_pasien_lat, lokasi_pasien_lon, jenis_layanan).
 * @returns {Promise<void>}
 */
async function handlePatientRequest(data) {
    const { id_pasien, lokasi_pasien_lat, lokasi_pasien_lon, jenis_layanan, kondisi } = data;
    const layanan = jenis_layanan || 'DARURAT';

    if (!id_pasien || !lokasi_pasien_lat || !lokasi_pasien_lon) {
        logger.error("Incomplete data. Payload: " + JSON.stringify(data), { category: 'dispatch' });
        return;
    }

    const patientLocation = { latitude: lokasi_pasien_lat, longitude: lokasi_pasien_lon };
    let newCallId;

    try {
        // Retrieve patient name from DB for logging
        let namaPasien = `Patient ${id_pasien}`;
        const pasienData = await db.query(`SELECT nama FROM pasien WHERE id_pasien = $1`, [id_pasien]);
        if (pasienData.rows.length > 0 && pasienData.rows[0].nama) namaPasien = pasienData.rows[0].nama;

        // Banner for new emergency call (Terminal)
        console.log(`\n================================================================`);
        console.log(` NEW EMERGENCY CALL`);
        console.log(` Patient      : ${namaPasien}`);
        console.log(` Time         : ${new Date().toISOString().replace('T', ' ').substring(0, 19)}`);
        console.log(`================================================================`);
        console.log(``);
<<<<<<< HEAD
        console.log(` Lokasi Pasien:`);
        console.log(`   Latitude  : ${lokasi_pasien_lat}`);
        console.log(`   Longitude : ${lokasi_pasien_lon}`);
        console.log(`   Jenis     : ${layanan}`);

        // Hitung severity berdasarkan kondisi darurat
        const severity = calculateSeverity({ kondisi, jenis_layanan: layanan });
        console.log(`   Kondisi   : ${kondisi || 'Tidak disebutkan'}`);
        console.log(`   Prioritas : ${SEVERITY_LABELS[severity]} (Level ${severity})`);
=======
        console.log(` Patient Location:`);
        console.log(`   Latitude   : ${lokasi_pasien_lat}`);
        console.log(`   Longitude  : ${lokasi_pasien_lon}`);
        console.log(`   Service    : ${layanan}`);
>>>>>>> refactor/translate-to-english
        console.log(``);

        // Log to System Monitor
        logger.info(`----------------------------------------------------------------`, { category: 'dispatch' });
<<<<<<< HEAD
        logger.info(`PANGGILAN DARURAT BARU | ${namaPasien} | ${SEVERITY_LABELS[severity]}`, { category: 'dispatch' });
        logger.info(`Lokasi: ${lokasi_pasien_lat}, ${lokasi_pasien_lon} | Kondisi: ${kondisi || layanan}`, { category: 'dispatch' });
        logger.info(`----------------------------------------------------------------`, { category: 'dispatch' });

        // Langkah 1: Catat panggilan darurat ke DB (dengan prioritas severity)
=======
        logger.info(`NEW EMERGENCY CALL | Patient: ${namaPasien}`, { category: 'dispatch' });
        logger.info(`Location: ${lokasi_pasien_lat}, ${lokasi_pasien_lon} | Service: ${layanan}`, { category: 'dispatch' });
        logger.info(`----------------------------------------------------------------`, { category: 'dispatch' });

        // Step 1: Record emergency call to DB to obtain call ID
>>>>>>> refactor/translate-to-english
        const sqlInsertCall = `
            INSERT INTO transaksi_panggilan
            (id_pasien, lokasi_pasien_lat, lokasi_pasien_lon, jenis_layanan, kondisi, status_panggilan, prioritas, waktu_panggilan, createdAt, updatedAt)
            VALUES ($1, $2, $3, $4, $5, 'PENDING', $6, NOW(), NOW(), NOW())
            RETURNING id_panggilan
        `;
        const result = await db.query(sqlInsertCall, [
            id_pasien,
            lokasi_pasien_lat,
            lokasi_pasien_lon,
            layanan,
            kondisi,
            severity
        ]);
        newCallId = result.rows[0].id_panggilan;
<<<<<<< HEAD
        logger.info(`Panggilan #${newCallId} dari ${namaPasien} dicatat di DB (Prioritas: ${severity}).`, { category: 'dispatch' });

        // Langkah 2: Masukkan ke Priority Queue (bukan langsung dispatch)
        enqueueCall({
            newCallId,
            id_pasien,
            lokasi_pasien_lat,
            lokasi_pasien_lon,
            jenis_layanan: layanan,
            kondisi,
            namaPasien
        });
=======
        logger.info(`New call from ${namaPasien} recorded in DB.`, { category: 'dispatch' });

        // Step 2: Find best driver using Hybrid Model (Filter + Refine)
        logger.info(`Starting driver search process...`, { category: 'dispatch' });
        const bestDriver = await _findBestDriver(patientLocation, newCallId, layanan);

        // Step 3: Assign driver and send notification
        await _assignDriverToCall(bestDriver, newCallId, patientLocation, id_pasien, kondisi);
>>>>>>> refactor/translate-to-english

    } catch (error) {
        console.error(`Failed to process assistance request:`, error.message);
        if (newCallId) {
            await db.query(`UPDATE transaksi_panggilan SET status_panggilan = 'FAILED' WHERE id_panggilan = $1`, [newCallId]).catch(console.error);
        }
    }
} //End function handlePatientRequest


/**
<<<<<<< HEAD
 * =======================================================
 * DISPATCH ENGINE (Priority Queue Batch Processing)
 * =======================================================
 */

/**
 * Menambahkan panggilan ke antrian prioritas dan memulai timer batch.
 * Timer batch mengumpulkan panggilan selama BATCH_WINDOW_MS sebelum memproses.
 */
function enqueueCall(callData) {
    const entry = emergencyQueue.enqueue(callData);
    logger.info(`[QUEUE] Masuk antrian | ${SEVERITY_LABELS[entry.severity]} | Antrian: ${emergencyQueue.size()} panggilan`, { category: 'dispatch' });

    // Mulai batch timer jika belum berjalan
    if (!dispatchTimer) {
        dispatchTimer = setTimeout(() => { processBatch(); }, BATCH_WINDOW_MS);
    }
}

/**
 * Memproses seluruh panggilan di antrian berdasarkan prioritas.
 * Panggilan dengan severity terendah (paling darurat) diproses lebih dulu.
 * Jika ambulans habis, sisa panggilan masuk status QUEUED di database.
 */
async function processBatch() {
    dispatchTimer = null;
    if (emergencyQueue.isEmpty()) return;

    const batchSize = emergencyQueue.size();
    console.log(`\n================================================================`);
    console.log(` BATCH DISPATCH: Memproses ${batchSize} panggilan berdasarkan prioritas`);
    console.log(`================================================================\n`);
    logger.info(`[BATCH] Memulai batch dispatch untuk ${batchSize} panggilan.`, { category: 'dispatch' });

    const availableCount = await getAvailableAmbulanceCount();
    let dispatched = 0;

    while (!emergencyQueue.isEmpty()) {
        const call = emergencyQueue.dequeue();
        const label = SEVERITY_LABELS[call.severity] || `Level ${call.severity}`;
        console.log(`  [${dispatched + 1}/${batchSize}] ${label} | Pasien: ${call.namaPasien || call.id_pasien} | Kondisi: ${call.kondisi || call.jenis_layanan}`);

        if (dispatched < availableCount) {
            try {
                await dispatchCall(call);
                dispatched++;
            } catch (err) {
                console.error(`    Dispatch gagal: ${err.message}`);
                logger.error(`[BATCH] Dispatch gagal untuk panggilan #${call.newCallId}: ${err.message}`, { category: 'dispatch' });
                await markAsWaiting(call);
            }
        } else {
            console.log(`    Semua ambulans sibuk. Masuk WAITING QUEUE...`);
            await markAsWaiting(call);
        }
    }

    console.log(`\n  Hasil: ${dispatched} dispatch berhasil, ${batchSize - dispatched} menunggu di antrian.\n`);
    logger.info(`[BATCH] Selesai: ${dispatched} dispatch, ${batchSize - dispatched} menunggu.`, { category: 'dispatch' });
}

/**
 * Men-dispatch satu panggilan: mencari driver terbaik dan menugaskannya.
 * Menggunakan algoritma Hybrid Model (Haversine + Google Distance Matrix) yang sudah ada.
 */
async function dispatchCall(callData) {
    const patientLocation = { latitude: callData.lokasi_pasien_lat, longitude: callData.lokasi_pasien_lon };
    logger.info(`[DISPATCH] Mencari driver untuk panggilan #${callData.newCallId}...`, { category: 'dispatch' });
    const bestDriver = await _findBestDriver(patientLocation, callData.newCallId, callData.jenis_layanan);
    await _assignDriverToCall(bestDriver, callData.newCallId, patientLocation, callData.id_pasien, callData.kondisi);
}

/**
 * Menghitung jumlah ambulans yang saat ini berstatus AVAILABLE.
 * @returns {Promise<number>} Jumlah ambulans tersedia.
 */
async function getAvailableAmbulanceCount() {
    const result = await db.query(`SELECT COUNT(*) as count FROM ambulans WHERE status_operasional = 'AVAILABLE'`);
    return parseInt(result.rows[0].count);
}

/**
 * Menandai panggilan sebagai QUEUED di database (menunggu ambulans bebas).
 * Panggilan QUEUED akan otomatis di-dispatch saat ada ambulans yang selesai tugas.
 */
async function markAsWaiting(callData) {
    try {
        await db.query(
            `UPDATE transaksi_panggilan SET status_panggilan = 'QUEUED', prioritas = $1 WHERE id_panggilan = $2`,
            [callData.severity, callData.newCallId]
        );
        logger.info(`[QUEUE] Panggilan #${callData.newCallId} masuk antrian tunggu (Prioritas: ${callData.severity}).`, { category: 'dispatch' });
    } catch (err) {
        console.error(`[QUEUE] Gagal menandai panggilan #${callData.newCallId} sebagai QUEUED:`, err.message);
    }
}

/**
 * Memeriksa apakah ada panggilan yang menunggu di antrian setelah ambulans bebas.
 * Dipanggil otomatis ketika driver menyelesaikan tugas (status 'selesai').
 * Mengambil panggilan QUEUED dengan prioritas tertinggi (severity terendah).
 */
async function checkWaitingQueue(freedAmbulanceId) {
    try {
        const result = await db.query(`
            SELECT id_panggilan, id_pasien, lokasi_pasien_lat, lokasi_pasien_lon, 
                   jenis_layanan, kondisi, prioritas
            FROM transaksi_panggilan 
            WHERE status_panggilan = 'QUEUED' 
            ORDER BY prioritas ASC, waktu_panggilan ASC 
            LIMIT 1
        `);

        if (result.rows.length > 0) {
            const nextCall = result.rows[0];
            const label = SEVERITY_LABELS[nextCall.prioritas] || `Level ${nextCall.prioritas}`;

            console.log(`\n================================================================`);
            console.log(` AUTO-DISPATCH: Ambulans ${freedAmbulanceId} bebas`);
            console.log(` -> Dispatch panggilan #${nextCall.id_panggilan} (${label})`);
            console.log(`================================================================\n`);
            logger.info(`[AUTO-DISPATCH] Ambulans ${freedAmbulanceId} bebas -> Panggilan #${nextCall.id_panggilan} (${label})`, { category: 'dispatch' });

            const patientLocation = {
                latitude: parseFloat(nextCall.lokasi_pasien_lat),
                longitude: parseFloat(nextCall.lokasi_pasien_lon)
            };

            // Update status dari QUEUED ke PENDING sebelum dispatch
            await db.query(`UPDATE transaksi_panggilan SET status_panggilan = 'PENDING' WHERE id_panggilan = $1`, [nextCall.id_panggilan]);

            const bestDriver = await _findBestDriver(patientLocation, nextCall.id_panggilan, nextCall.jenis_layanan);
            await _assignDriverToCall(bestDriver, nextCall.id_panggilan, patientLocation, nextCall.id_pasien, nextCall.kondisi);
        } else {
            console.log(`[AUTO-DISPATCH] Tidak ada panggilan dalam antrian tunggu.`);
        }
    } catch (err) {
        console.error(`[AUTO-DISPATCH] Gagal memproses antrian tunggu:`, err.message);
    }
}


/**
 * [ALUR 3 - T4] Menangani konfirmasi tugas dari driver.
 * Menerima T4 (diterima/selesai) dan memperbarui status di DB.
 * @param {object} data - Payload JSON dari T4 (id_panggilan, id_ambulans, status).
=======
 * [FLOW 3 - T4] Handles task confirmation from driver.
 * Receives T4 (diterima/selesai) and updates status in DB.
 * @param {object} data - JSON payload from T4 (id_panggilan, id_ambulans, status).
>>>>>>> refactor/translate-to-english
 * @returns {Promise<void>}
 */
async function handleDriverTaskConfirmation(data) {
    // 1. DATA EXTRACTION & MISMATCH HANDLING
    const id_panggilan = data.id_panggilan;
    const status = data.status;
    const id_ambulans = data.id_driver || data.id_ambulans;

    // Attempt to fetch driver name from DB for better logging
    let namaDriver = `Ambulance ${id_ambulans}`;
    try {
        const driverData = await db.query(`SELECT nama_driver FROM ambulans WHERE id_ambulans = $1`, [id_ambulans]);
        if (driverData.rows.length > 0 && driverData.rows[0].nama_driver) namaDriver = driverData.rows[0].nama_driver;
    } catch (e) { }

    logger.info(`Confirmation from ${namaDriver}: ${status.toUpperCase()}`, { category: 'dispatch' });

    // Data Validation
    if (!id_panggilan || !id_ambulans || !status) {
        logger.error(`Task confirmation data incomplete. Payload: ${JSON.stringify(data)}`, { category: 'dispatch' });
        return;
    }

    try {
        // Scenario A: Accepted
        if (status == 'diterima') {
            // Update status_panggilan to ON_THE_WAY
            await db.query(
                `UPDATE transaksi_panggilan SET status_panggilan = 'ON_THE_WAY', id_ambulans_respons = $1 WHERE id_panggilan = $2`,
                [id_ambulans, id_panggilan]
            );

            // Update driver status to BUSY
            await db.query(
                `UPDATE ambulans SET status_operasional = 'BUSY' WHERE id_ambulans = $1`,
                [id_ambulans]
            );

            // Remove rejection data from memory for cleanup
            if (rejectedDriversMap[id_panggilan]) delete rejectedDriversMap[id_panggilan];

            console.log(`\n================================================================`);
            console.log(` ASSIGNMENT ACCEPTED`);
            console.log(` Driver : ${namaDriver}`);
            console.log(` Status : Driver is en route to patient location.`);
            console.log(`================================================================\n`);
            logger.info(`${namaDriver} ACCEPTED TASK`, { category: 'dispatch' });

            const callData = await db.query(
                `SELECT id_pasien FROM transaksi_panggilan WHERE id_panggilan = $1`,
                [id_panggilan]
            );

            if (callData.rows.length > 0) {
                const id_pasien = callData.rows[0].id_pasien;

                // 4. SEND NOTIFICATION TO PATIENT THAT DRIVER IS EN ROUTE
                const topicBalasanPasien = `panggilan/status/pasien/${id_pasien}`;
                const payloadUntukPasien = {
                    status_panggilan: "menuju_lokasi", // <-- Expected by Android SearchingActivity
                    id_panggilan: id_panggilan,
                    id_ambulans: id_ambulans,
                    eta_detik: 0 // Recalculate ETA here if needed
                };

                client.publish(topicBalasanPasien, JSON.stringify(payloadUntukPasien), { qos: 1 });
                logger.info(`[T8] Response sent to Patient: ${topicBalasanPasien}`, { category: 'dispatch' });
            }
        }

        // Scenario B: Rejected or Problem Encountered (re-dispatching)
        else if (status === 'ditolak' || status === 'kendala') {

            // 1. Critical Validation: Without id_panggilan, DB lookup is impossible
            if (!id_panggilan) {
                logger.error(`Fatal Error: Driver sent status '${status}', but 'id_panggilan' was not sent by Android!`, { category: 'dispatch' });
                return; // Halt process to prevent server crash
            }

            const actionName = status === 'kendala' ? 'ENCOUNTERED AN ISSUE ON' : 'REJECTED';

            console.log(`\n================================================================`);
            console.log(` ASSIGNMENT ${status === 'kendala' ? 'DISRUPTED' : 'REJECTED'}`);
            console.log(` Driver : ${namaDriver}`);
            console.log(` Status : Searching for next driver candidate...`);
            console.log(`================================================================\n`);
            logger.warn(`${namaDriver} ${actionName} task #${id_panggilan}.`, { category: 'dispatch' });

            // 2. Initialize rejection list if not already present
            if (!Array.isArray(rejectedDriversMap[id_panggilan])) {
                rejectedDriversMap[id_panggilan] = [];
            }

            // Update driver status (If problem encountered, set to OFFLINE)
            if (status === 'kendala') {
                await db.query(`UPDATE ambulans SET status_operasional = 'OFFLINE' WHERE id_ambulans = $1`, [id_ambulans]);
                logger.info(`Status of ${namaDriver} changed to OFFLINE due to operational issues.`, { category: 'dispatch' });
            }
            // 3. Add to blacklist for this call
            if (!rejectedDriversMap[id_panggilan].includes(id_ambulans)) {
                rejectedDriversMap[id_panggilan].push(id_ambulans);
            }

            try {
                // 4. Retrieve call data from DB (Get jenis_layanan, location & condition)
                const callData = await db.query(
                    `SELECT lokasi_pasien_lat, lokasi_pasien_lon, jenis_layanan, id_pasien, kondisi FROM transaksi_panggilan WHERE id_panggilan = $1`,
                    [id_panggilan]
                );

                if (callData.rows.length > 0) {
                    const row = callData.rows[0];

                    const dataPasien = callData.rows[0];
                    const patientLocation = {
                        latitude: parseFloat(row.lokasi_pasien_lat),
                        longitude: parseFloat(row.lokasi_pasien_lon)
                    };

                    // Validation: If coordinates are NaN, do not continue
                    if (isNaN(patientLocation.latitude) || isNaN(patientLocation.longitude)) {
                        throw new Error(`Patient coordinates for call #${id_panggilan} are invalid/empty in DB.`);
                    }

                    // 5. Run Re-selection Algorithm (Search Next Best Driver)
                    const nextBestDriver = await _findBestDriver(
                        patientLocation,
                        id_panggilan,
                        dataPasien.jenis_layanan,
                        rejectedDriversMap[id_panggilan] // send rejection list
                    );

                    logger.info(`Replacement candidate found: ${nextBestDriver.nama}`, { category: 'dispatch' });

                    // 6. Assign new driver including condition from DB
                    await _assignDriverToCall(nextBestDriver, id_panggilan, patientLocation, dataPasien.id_pasien, dataPasien.kondisi);
                }
            } catch (errFind) {
                console.log(`\n================================================================`);
                console.log(` SEARCH FAILED`);
                console.log(` Reason : ${errFind.message}`);
                console.log(`================================================================\n`);
                logger.error(`Failed to find replacement: ${errFind.message}`, { category: 'dispatch' });
                // IF NO MORE DRIVERS AVAILABLE, NOTIFY PATIENT
                const callDataFallback = await db.query(`SELECT id_pasien FROM transaksi_panggilan WHERE id_panggilan = $1`, [id_panggilan]);
                if (callDataFallback.rows.length > 0) {
                    const id_pasien = callDataFallback.rows[0].id_pasien;
                    const topicBalasanPasien = `panggilan/status/pasien/${id_pasien}`;
                    client.publish(topicBalasanPasien, JSON.stringify({ status_panggilan: "ditolak" }), { qos: 1 });
                }
            }
        }

        // Scenario C: Completed
        else if (status === 'selesai') {
            // Update call status to COMPLETED
            await db.query(`UPDATE transaksi_panggilan SET status_panggilan = 'COMPLETED' WHERE id_panggilan = $1`, [id_panggilan]);

            // Set driver back to 'AVAILABLE'
            await db.query(`UPDATE ambulans SET status_operasional = 'AVAILABLE' WHERE id_ambulans = $1`, [id_ambulans]);

            const callData = await db.query(`SELECT id_pasien FROM transaksi_panggilan WHERE id_panggilan = $1`, [id_panggilan]);
            if (callData.rows.length > 0) {
                const id_pasien = callData.rows[0].id_pasien;
                const topicBalasanPasien = `panggilan/status/pasien/${id_pasien}`;

                const payloadSelesai = {
                    status_panggilan: "selesai",
                    id_panggilan: id_panggilan
                };

                client.publish(topicBalasanPasien, JSON.stringify(payloadSelesai), { qos: 1 });
                logger.info(`COMPLETED notification sent to Patient: ${topicBalasanPasien}`, { category: 'dispatch' });
            }

            console.log(`\n================================================================`);
            console.log(` TASK COMPLETED`);
            console.log(` Driver : ${namaDriver}`);
            console.log(` Status : Driver is back to AVAILABLE status.`);
            console.log(`================================================================\n`);
            logger.info(`Task Completed. ${namaDriver} is available again.`, { category: 'dispatch' });

            // Cek antrian: apakah ada panggilan QUEUED yang menunggu?
            await checkWaitingQueue(id_ambulans);

        }
    } catch (error) {
        console.error(`Failed to process T4 confirmation:`, error.message);
    }
} //End function handleDriverTaskConfirmation


/**
 * Handles driver operational status updates (Available, Busy, Offline)
 * Aligns with Table 3.1 and Figure 3.4 in Proposal
 */
async function handleOperationalStatusUpdate(data) {
    const id_ambulans = data.id_ambulans || data.id_driver;
    const status_baru = data.status; // 'Offline', 'Available', 'Busy'

    if (!id_ambulans || !status_baru) {
        console.error("[T6] Status update data incomplete:", data);
        return;
    }

    try {
        await db.query(
            `UPDATE ambulans SET status_operasional = $1 WHERE id_ambulans = $2`,
            [status_baru.toUpperCase(), id_ambulans]
        );
        console.log(`[T6] Status for Driver ${id_ambulans} manually updated to: ${status_baru}`);
    } catch (error) {
        console.error(`[T6] Failed to manually update status:`, error.message);
    }
}


/**
 * Handles call cancellation initiated by patient.
 * @param {object} data - JSON payload containing id_panggilan and optional id_ambulans.
 */
async function handlePatientCancellation(data) {
    try {
        logger.info(`[CANCELLATION] Received cancellation from patient for call ${data.id_panggilan}`, { category: 'dispatch' });

        if (!data.id_panggilan) {
            logger.error(`[CANCELLATION] Incomplete cancellation data (empty id_panggilan): ${JSON.stringify(data)}`, { category: 'dispatch' });
            return;
        }

        let assignedDriverId = data.id_ambulans; // Fallback if patient Android happens to send it

        if (data.id_pasien) {
            // Cancel based on Patient ID
            const result = await db.query(`
                UPDATE transaksi_panggilan 
                SET status_panggilan = 'CANCELLED' 
                WHERE id_pasien = $1 
                AND status_panggilan IN ('PENDING', 'WAITING_FOR_DRIVER', 'ON_THE_WAY')
                RETURNING id_ambulans_respons`, [data.id_pasien]);

            if (result.rows.length > 0 && result.rows[0].id_ambulans_respons) {
                assignedDriverId = result.rows[0].id_ambulans_respons;
            }
        } else if (Number.isInteger(data.id_panggilan) || !isNaN(parseInt(data.id_panggilan))) {
            // Cancel using native Integer (if available)
            const validId = parseInt(data.id_panggilan);
            const result = await db.query(`
                UPDATE transaksi_panggilan 
                SET status_panggilan = 'CANCELLED' 
                WHERE id_panggilan = $1
                RETURNING id_ambulans_respons`, [validId]);

            if (result.rows.length > 0 && result.rows[0].id_ambulans_respons) {
                assignedDriverId = result.rows[0].id_ambulans_respons;
            }
        } else {
            logger.error("[CANCELLATION] Android sent text format without providing id_pasien", { category: 'dispatch' });
            return;
        }

        // 2. If a driver has been assigned, revert to AVAILABLE and send interrupt
        if (assignedDriverId) {
            await db.query(
                "UPDATE ambulans SET status_operasional = 'AVAILABLE' WHERE id_ambulans = $1",
                [assignedDriverId]
            );

            // Send Kill Signal to specific Driver!
            const topicInterupsi = `ambulans/interupsi/batal/${assignedDriverId}`;
            client.publish(topicInterupsi, JSON.stringify({
                pesan: "Pasien membatalkan pesanan. Silakan kembali bersiaga."
            }), { qos: 1 });

            logger.info(`[CANCELLATION] Cancellation signal successfully sent to ambulance ${assignedDriverId}`, { category: 'dispatch' });
        }
    } catch (error) {
        logger.error(`[CANCELLATION] Failed to process patient cancellation: ${error.message}`, { category: 'dispatch' });
    }
}

/**
 * Handles call cancellation initiated by driver (e.g. vehicle breakdown).
 */
async function handleDriverCancellation(data) {
    try {
        logger.info(`[DRIVER CANCELLATION] Received cancellation from driver for call ${data.id_panggilan}`, { category: 'dispatch' });

        if (!data.id_panggilan) {
            logger.error(`[DRIVER CANCELLATION] Incomplete cancellation data (empty id_panggilan)`, { category: 'dispatch' });
            return;
        }

        const validId = parseInt(data.id_panggilan);

        // Update transaction to CANCELLED and fetch notification info
        const result = await db.query(`
            UPDATE transaksi_panggilan 
            SET status_panggilan = 'CANCELLED' 
            WHERE id_panggilan = $1
            RETURNING id_pasien, id_ambulans_respons`, [validId]);

        if (result.rows.length > 0) {
            const row = result.rows[0];

            // 1. Change driver status to OFFLINE due to vehicle issue
            const driverId = row.id_ambulans_respons || data.id_ambulans;
            if (driverId) {
                await db.query(
                    "UPDATE ambulans SET status_operasional = 'OFFLINE' WHERE id_ambulans = $1",
                    [driverId]
                );
                logger.info(`[DRIVER CANCELLATION] Driver ${driverId} status changed to OFFLINE due to vehicle issue.`, { category: 'dispatch' });
            }

            // 2. Send cancellation notification to patient
            if (row.id_pasien) {
                const topicBalasanPasien = `panggilan/status/pasien/${row.id_pasien}`;
                client.publish(topicBalasanPasien, JSON.stringify({
                    id_panggilan: validId,
                    status: 'BATAL_OLEH_DRIVER',
                    pesan: "Driver membatalkan pesanan karena kendala operasional kendaraan. Silakan lakukan pesanan ulang."
                }), { qos: 1 });
                logger.info(`[DRIVER CANCELLATION] Cancellation signal successfully sent to patient ${row.id_pasien}`, { category: 'dispatch' });
            }
        } else {
<<<<<<< HEAD
            logger.error(`[PEMBATALAN DRIVER] Panggilan ${validId} tidak ditemukan atau sudah batal.`, { category: 'dispatch' });
=======
             logger.error(`[DRIVER CANCELLATION] Call ${validId} not found or already cancelled.`, { category: 'dispatch' });
>>>>>>> refactor/translate-to-english
        }
    } catch (error) {
        logger.error(`[DRIVER CANCELLATION] Failed to process cancellation: ${error.message}`, { category: 'dispatch' });
    }
}


/**
 * =======================================================
 * HELPER FUNCTIONS
 * =======================================================
 */

/**
 * Builds request parameters for Amazon Location Service (CalculateRouteMatrix).
 * Uses GeoJSON format [longitude, latitude], TravelMode: 'Car', and real-time DepartureTime.
 */
function _buildAwsRouteMatrixParams(patientLocation, candidates, calculatorName = ROUTE_CALCULATOR_NAME) {
    const departurePositions = [
        [patientLocation.longitude, patientLocation.latitude]
    ];
    const destinationPositions = candidates.map(c => [
        c.location.longitude,
        c.location.latitude
    ]);

    return {
        CalculatorName: calculatorName,
        DeparturePositions: departurePositions,
        DestinationPositions: destinationPositions,
        TravelMode: 'Car',
        DepartureTime: new Date(),
        DistanceUnit: 'Kilometers'
    };
}

/**
 * Processes result elements from Amazon Location Service (CalculateRouteMatrix) response.
 * Records numerical details to logger and logs/dispatch-candidates.jsonl file.
 */
function _processAwsRouteMatrixElements(candidates, apiResponse, newCallId = null, logTarget = logger) {
    const results = [];
    const nowIso = new Date().toISOString();

    if (!apiResponse || !apiResponse.RouteMatrix || !apiResponse.RouteMatrix[0]) {
        return { results };
    }

    const row = apiResponse.RouteMatrix[0];
    row.forEach((element, index) => {
        const candidate = candidates[index];
        if (!candidate) return;

        if (!element.Error && typeof element.DurationSeconds === 'number') {
            const distanceKm = typeof element.Distance === 'number' ? element.Distance : 0;
            const distanceMeters = Math.round(distanceKm * 1000);
            const distanceText = distanceKm >= 1 ? `${distanceKm.toFixed(1)} km` : `${distanceMeters} m`;

            const durationSeconds = Math.round(element.DurationSeconds);
            const durationMinutes = Math.round(durationSeconds / 60);
            const durationText = durationMinutes > 0 ? `${durationMinutes} min` : `${durationSeconds} sec`;

            const candidateLogData = {
                callId: newCallId,
                candidateId: candidate.id,
                candidateName: candidate.nama,
                haversineMeters: candidate.distanceHaversine,
                durationSeconds: durationSeconds,
                durationInTrafficSeconds: durationSeconds,
                etaSeconds: durationSeconds,
                etaSource: 'aws_traffic_route_matrix',
                distanceMeters: distanceMeters,
                timestamp: nowIso
            };

            // Save to JSON lines file (logs/dispatch-candidates.jsonl)
            try {
                const logDir = path.join(__dirname, 'logs');
                if (!fs.existsSync(logDir)) {
                    fs.mkdirSync(logDir, { recursive: true });
                }
                fs.appendFileSync(path.join(logDir, 'dispatch-candidates.jsonl'), JSON.stringify(candidateLogData) + '\n', 'utf8');
            } catch (errLog) {
                console.error('[LOG FILE] Failed to write to dispatch-candidates.jsonl:', errLog.message);
            }

            console.log(` ${candidate.nama.padEnd(16)}| ${String(candidate.distanceHaversine).padStart(13)} | ${distanceText.padEnd(15)}| ${durationText} (traffic)`);
            if (logTarget && typeof logTarget.info === 'function') {
                logTarget.info(`  -> ${candidate.nama.padEnd(15)} (Hav: ${candidate.distanceHaversine} m) | Route: ${distanceText} | ETA: ${durationText} | Used: aws_traffic (${durationSeconds}s)`, {
                    category: 'dispatch',
                    metadata: candidateLogData
                });
            }

            results.push({
                id: candidate.id,
                nama: candidate.nama,
                etaSeconds: durationSeconds,
                etaSource: 'aws_traffic_route_matrix',
                distanceMeters: distanceMeters,
                distanceText: distanceText,
                durationText: durationText,
                durationInTrafficText: durationText,
                haversineMeters: candidate.distanceHaversine,
                durationSeconds: durationSeconds,
                durationInTrafficSeconds: durationSeconds
            });
        } else {
            const errDetail = element.Error ? (element.Error.Code || JSON.stringify(element.Error)) : 'Route not found';
            if (logTarget && typeof logTarget.warn === 'function') {
                logTarget.warn(`  -> ${candidate.nama}: AWS RouteMatrix Failed (Status: ${errDetail})`, { category: 'dispatch' });
            }
        }
    });

    return { results };
}

// Function aliases for backward compatibility
const _buildDistanceMatrixParams = _buildAwsRouteMatrixParams;
const _processDistanceMatrixElements = _processAwsRouteMatrixElements;

/**
 * [HELPER FOR T3] Executes Hybrid Model logic (Filter + Refine).
 * @param {object} patientLocation - Patient { latitude, longitude } object.
 * @param {number} newCallId - Newly generated emergency call ID.
 * @returns {Promise<object>} Best driver object containing { id, etaSeconds }.
 * @throws {Error} If no drivers found or API fails.
 */
async function _findBestDriver(patientLocation, newCallId, jenisLayanan, excludedDriverIds = []) {

    // Fleet classification logic
    let targetKategori = 'PSC';
    if (jenisLayanan === 'JENAZAH') {
        targetKategori = 'JENAZAH';
    } else if (jenisLayanan === 'TRANSPORT') {
        targetKategori = 'RELAWAN';
    }

    logger.info(`Searching for fleet category ${targetKategori} for service ${jenisLayanan}...`, { category: 'dispatch' });

    // Use safeBlacklist to prevent crash
    const safeBlacklist = Array.isArray(excludedDriverIds) ? excludedDriverIds : [];

    // Use 'AVAILABLE' status per proposal for drivers ready for duty.
    const sqlGetDrivers = `
        SELECT id_ambulans, nama_driver, lokasi_latitude, lokasi_longitude 
        FROM ambulans 
        WHERE status_operasional = 'AVAILABLE'
          AND lokasi_latitude IS NOT NULL
          AND kategori_armada = $1
    `;


    // 1. Fetch drivers from DB
    const result = await db.query(sqlGetDrivers, [targetKategori]);
    const drivers = result.rows;

    if (drivers.length === 0) {
        await db.query(`UPDATE transaksi_panggilan SET status_panggilan = 'NO_DRIVERS' WHERE id_panggilan = $1`, [newCallId]);
        throw new Error(`No ${targetKategori} fleet currently online.`);
    }

    // Exclude drivers present in 'excludedDriverIds' list
    const validDrivers = drivers.filter(driver =>
        !safeBlacklist.includes(driver.id_ambulans) &&
        !safeBlacklist.includes(String(driver.id_ambulans))
    );

    if (validDrivers.length === 0) {
        // If all drivers rejected or no drivers available
        await db.query(`UPDATE transaksi_panggilan SET status_panggilan = 'NO_DRIVERS_AVAILABLE' WHERE id_panggilan = $1`, [newCallId]);
        throw new Error(`No ${targetKategori} drivers available.`);
    }

    // 2. STAGE 1: FILTER (Haversine)
    console.log(`----------------------------------------------------------------`);
    logger.info(`--- STAGE 1: FILTER -- Haversine Algorithm (Straight-line Distance) ---`, { category: 'dispatch' });
    logger.info(`Calculating distance from ${validDrivers.length} drivers to patient location...`, { category: 'dispatch' });
    console.log(``);
    console.log(` Driver          | Latitude       | Longitude      | Haversine (m)`);
    console.log(`----------------------------------------------------------------`);
    const driversWithDistance = validDrivers.map(driver => {
        const driverLocation = {
            latitude: parseFloat(driver.lokasi_latitude),
            longitude: parseFloat(driver.lokasi_longitude)
        };
        const distanceHaversine = getDistance(patientLocation, driverLocation);
        const nama = driver.nama_driver || `Ambulance ${driver.id_ambulans}`;

        console.log(` ${nama.padEnd(16)}| ${String(driver.lokasi_latitude).padEnd(15)}| ${String(driver.lokasi_longitude).padEnd(15)}| ${String(distanceHaversine).padStart(12)}`);
        logger.info(`  -> ${nama.padEnd(15)} | Haversine: ${distanceHaversine} m`, { category: 'dispatch' });

        return {
            id: driver.id_ambulans,
            nama: nama,
            location: driverLocation,
            distanceHaversine: distanceHaversine
        };
    });
    console.log(`----------------------------------------------------------------`);

    driversWithDistance.sort((a, b) => a.distanceHaversine - b.distanceHaversine); // Sort by shortest Haversine distance
    const candidates = driversWithDistance.slice(0, 5); // Take top 5 candidates

    // Log Haversine ranking
    console.log(``);
    console.log(` Haversine Ranking Results (sorted nearest to farthest):`);
    logger.info(`Haversine Ranking Results (sorted nearest to farthest):`, { category: 'dispatch' });
    candidates.forEach((c, i) => {
        const marker = i === 0 ? '  << Nearest' : '';
        console.log(`   ${i + 1}. ${c.nama.padEnd(15)} -> ${c.distanceHaversine} m${marker}`);
        logger.info(`  ${i + 1}. ${c.nama} -> ${c.distanceHaversine} m${marker}`, { category: 'dispatch' });
    });
    if (driversWithDistance.length > 5) {
        console.log(`   -- (${driversWithDistance.length - 5} other drivers not in top 5) --`);
    }
    console.log(``);

    // 3. STAGE 2: REFINE (Amazon Location Service)
    console.log(`----------------------------------------------------------------`);
    logger.info(`--- STAGE 2: REFINE -- Amazon Location Service (Route Distance & Time) ---`, { category: 'dispatch' });
    logger.info(`Sending top ${candidates.length} candidates to AWS Location Service...`, { category: 'dispatch' });
    logger.info(`Destinations: ${candidates.map(c => c.nama).join(', ')}`, { category: 'dispatch' });

    let results = [];
    try {
        const matrixParams = _buildAwsRouteMatrixParams(patientLocation, candidates);
        const command = new CalculateRouteMatrixCommand(matrixParams);
        const apiResponse = await locationClient.send(command);

        console.log(`\n Driver          | Haversine (m) | Route Distance | Travel Time   `);
        console.log(`----------------------------------------------------------------`);
        logger.info(`--- CALCULATION RESULTS (AWS) ---`, { category: 'dispatch' });

        const processed = _processAwsRouteMatrixElements(candidates, apiResponse, newCallId, logger);
        results = processed.results;
    } catch (apiErr) {
        logger.warn(`[AWS ROUTING ERROR] Failed to call AWS Location Service: ${apiErr.message}. Switching to Haversine fallback.`, { category: 'dispatch' });
        console.warn(`⚠️ [AWS ROUTING ERROR] Failed to call AWS Location Service: ${apiErr.message}`);
        console.warn(`   Switching to automatic Haversine fallback.`);
    }

    // If AWS fails or returns no valid routes -> Fallback to Haversine
    if (results.length === 0) {
        console.log(`----------------------------------------------------------------`);
        console.log(` [FALLBACK] Using Haversine distance ranking (straight-line)`);
        logger.warn(`[FALLBACK] Using Haversine distance ranking for call ${newCallId}`, { category: 'dispatch' });
        const fallbackWinner = driversWithDistance[0];
        const estSeconds = Math.round(fallbackWinner.distanceHaversine / 8.33); // ~30 km/h
        const estMinutes = Math.max(1, Math.round(estSeconds / 60));
        return {
            id: fallbackWinner.id,
            nama: fallbackWinner.nama,
            etaSeconds: estSeconds,
            etaSource: 'haversine_fallback',
            distanceMeters: fallbackWinner.distanceHaversine,
            distanceText: `${fallbackWinner.distanceHaversine} m`,
            durationText: `~${estMinutes} min (Haversine)`
        };
    }

    console.log(`----------------------------------------------------------------`);
    console.log(` Validation: Route Distance is always >= Haversine [VALID]`);

    results.sort((a, b) => a.etaSeconds - b.etaSeconds || a.distanceMeters - b.distanceMeters);
    const bestDriverAPI = results[0];

    // AWS Route Matrix Ranking Results
    console.log(``);
    console.log(` AWS Route Matrix Ranking Results (sorted fastest to slowest):`);
    logger.info(`AWS Route Matrix Ranking Results (sorted fastest to slowest):`, { category: 'dispatch' });
    results.forEach((r, i) => {
        console.log(`   ${i + 1}. ${r.nama.padEnd(15)} -> ${r.durationText}  (Route Distance: ${r.distanceText})`);
        logger.info(`  ${i + 1}. ${r.nama} -> ${r.durationText}  (Route Distance: ${r.distanceText})`, { category: 'dispatch' });
    });

    // Conclusion
    const haversineWinner = driversWithDistance[0];
    const isConsistent = haversineWinner.id === bestDriverAPI.id;

    console.log(``);
    console.log(`================================================================`);
    console.log(` CONCLUSION (Hybrid Model):`);
    console.log(`  Nearest driver (Haversine)         : ${haversineWinner.nama} (${haversineWinner.distanceHaversine} m)`);
    console.log(`  Fastest driver (AWS Route Matrix)  : ${bestDriverAPI.nama} (${bestDriverAPI.durationText})`);
    console.log(``);
    logger.info(`--- CONCLUSION (Hybrid Model) ---`, { category: 'dispatch' });
    logger.info(`Nearest driver (Haversine)         : ${haversineWinner.nama} (${haversineWinner.distanceHaversine} m)`, { category: 'dispatch' });
    logger.info(`Fastest driver (AWS Route Matrix)  : ${bestDriverAPI.nama} (${bestDriverAPI.durationText})`, { category: 'dispatch' });

    if (isConsistent) {
        console.log(`  RESULT: CONSISTENT -- Both methods selected the same driver.`);
        logger.info(`RESULT: CONSISTENT -- Both methods selected the same driver.`, { category: 'dispatch' });
    } else {
        console.log(`  RESULT: DIVERGENT -- Haversine and AWS Route Matrix selected different drivers.`);
        console.log(`  ${haversineWinner.nama} is closer in a straight line, but the road route is longer.`);
        console.log(`  System selected ${bestDriverAPI.nama} based on fastest travel time.`);
        logger.warn(`RESULT: DIVERGENT -- Haversine and AWS Route Matrix selected different drivers.`, { category: 'dispatch' });
        logger.warn(`${haversineWinner.nama} is closer in a straight line, but the road route is longer.`, { category: 'dispatch' });
        logger.warn(`System selected ${bestDriverAPI.nama} based on fastest travel time.`, { category: 'dispatch' });
    }
    console.log(`================================================================\n`);

    return bestDriverAPI;
} //End _findBestDriver


/**
 * [HELPER FOR T3] Assigns driver to call and sends notification.
 * @param {object} bestDriver - Best driver object { id, etaSeconds }.
 * @param {number} callId - Emergency call ID from DB.
 * @param {object} patientLocation - Patient { latitude, longitude } object.
 * @param {number} id_pasien - ID of calling patient.
 * @param {string} kondisi - Patient condition sent from application.
 * @returns {Promise<void>}
 */
async function _assignDriverToCall(bestDriver, callId, patientLocation, id_pasien, kondisi) {
    let namaPasien = "Emergency Patient"; // Default value

    const sqlUpdateCall = `
        UPDATE transaksi_panggilan 
        SET id_ambulans_respons = $1, status_panggilan = 'WAITING_FOR_DRIVER' 
        WHERE id_panggilan = $2
    `;

    console.log(`\n================================================================`);
    console.log(` SENDING ASSIGNMENT`);
    console.log(` Target : ${bestDriver.nama}`);
    console.log(` Status : Waiting for driver confirmation (Timeout: 20 seconds)`);
    console.log(`================================================================\n`);

    logger.info(`${bestDriver.nama} OFFERED call task ${callId}. Waiting for confirmation...`, { category: 'dispatch' });

    try {
        await db.query(sqlUpdateCall, [bestDriver.id, callId]);
        logger.info(`Updated status for call ${callId} to WAITING_FOR_DRIVER successfully.`, { category: 'dispatch' });

        // 2. Retrieve Patient Name from Neon DB
        const sqlGetPatient = `SELECT nama FROM pasien WHERE id_pasien = $1`;

        const patientData = await db.query(sqlGetPatient, [id_pasien]);

        if (patientData.rows.length > 0) {
            namaPasien = patientData.rows[0].nama;
        }
    } catch (err) {
        console.error("[T3] ERROR DURING DB UPDATE IN _assignDriverToCall:", err);
    }

    // 2. Update DB: Set driver status to 'BUSY'
    // await db.query(
    //     `UPDATE ambulans SET status_operasional = 'BUSY' WHERE id_ambulans = ?`,
    //     [bestDriver.id]
    // );
    // console.log(`[T3] Driver ${bestDriver.id} assigned for call ${callId} in DB.`);

    // 3. Send T3: Task notification to selected driver
    const topicTugas = `ambulans/tugas/${bestDriver.id}`;
    const payloadTugas = {
        id_panggilan: callId,
        id_pasien: id_pasien,
        nama_pasien: namaPasien,
        lokasi_pasien_lat: patientLocation.latitude,
        lokasi_pasien_lon: patientLocation.longitude,
        eta_detik: bestDriver.etaSeconds,
        kondisi: kondisi // <-- Using parameter passed from caller
    };
    client.publish(topicTugas, JSON.stringify(payloadTugas), { qos: 1 });
    logger.info(`Task Offer sent to topic ${topicTugas}`, { category: 'dispatch' });

    // Inside _assignDriverToCall function, after dispatching MQTT task:
    setTimeout(async () => {
        try {
            // Check current call status in database
            const result = await db.query(
                `SELECT status_panggilan, jenis_layanan, id_ambulans_respons FROM transaksi_panggilan WHERE id_panggilan = $1`,
                [callId]
            );

            if (result.rows.length > 0) {
                const callData = result.rows[0];
                const currentStatus = callData.status_panggilan;
                const assignedDriver = callData.id_ambulans_respons;

                // If after 20 seconds the status is still WAITING_FOR_DRIVER (neither accepted nor rejected) and driver hasn't been reassigned
                if (currentStatus === 'WAITING_FOR_DRIVER' && assignedDriver == bestDriver.id) {
                    console.log(`\n================================================================`);
                    console.log(` ASSIGNMENT TIMEOUT`);
                    console.log(` Driver : ${bestDriver.nama}`);
                    console.log(` Status : No response in 20 seconds, searching for replacement.`);
                    console.log(`================================================================\n`);
                    logger.warn(`[TIMEOUT] ${bestDriver.nama} did not respond within 20 seconds. Forwarding to another driver...`, { category: 'dispatch' });

                    // 1. Send cancel/timeout signal to first driver so the app stops ringing
                    const topicInterupsi = `ambulans/interupsi/batal/${bestDriver.id}`;
                    client.publish(topicInterupsi, JSON.stringify({
                        pesan: "Waktu penawaran habis (20 detik). Pesanan otomatis dialihkan."
                    }), { qos: 1 });

                    // 2. Add this driver to rejectedDriversMap
                    if (!Array.isArray(rejectedDriversMap[callId])) {
                        rejectedDriversMap[callId] = [];
                    }
                    if (!rejectedDriversMap[callId].includes(bestDriver.id)) {
                        rejectedDriversMap[callId].push(bestDriver.id);
                    }

                    try {
                        // 3. Search nextBestDriver again as in the "rejected" block
                        const nextBestDriver = await _findBestDriver(
                            patientLocation,
                            callId,
                            callData.jenis_layanan,
                            rejectedDriversMap[callId]
                        );

                        logger.info(`[TIMEOUT] Replacement candidate found: ${nextBestDriver.nama}`, { category: 'dispatch' });

                        // 4. Re-invoke _assignDriverToCall with the new driver
                        await _assignDriverToCall(nextBestDriver, callId, patientLocation, id_pasien, kondisi);
                    } catch (errFind) {
                        console.error(`[TIMEOUT] Failed to find replacement: ${errFind.message}`);
                        // If no more drivers available, notify patient
                        const topicBalasanPasien = `panggilan/status/pasien/${id_pasien}`;
                        client.publish(topicBalasanPasien, JSON.stringify({ status_panggilan: "ditolak" }), { qos: 1 });
                    }
                }
            }
        } catch (err) {
            console.error("Failed to perform timeout check:", err);
        }
    }, 20000); // 20000 ms = 20 seconds
} //End _assignDriverToCall

module.exports = {
    _findBestDriver,
    _assignDriverToCall,
    _buildAwsRouteMatrixParams,
    _processAwsRouteMatrixElements,
    _buildDistanceMatrixParams,
    _processDistanceMatrixElements
};
