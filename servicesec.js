// --- KONFIGURASI ---
// Mode: Haversine-Only (tanpa Google Distance Matrix API)
const mqtt = require('mqtt');
const Joi = require('joi');
const db = require('./db.js');
const { getDistance } = require('geolib');
// Google Maps API dihapus karena limit kuota — menggunakan Haversine saja
// const { Client } = require("@googlemaps/google-maps-services-js");
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
    status: Joi.string().valid('diterima', 'ditolak', 'selesai').required(),
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

// Konfigurasi Koneksi
const BROKER_URL = process.env.MQTT_BROKER_URL;
// GOOGLE_MAPS_API_KEY tidak lagi digunakan pada mode Haversine-Only
// const GOOGLE_MAPS_API_KEY = process.env.GOOGLE_MAPS_API_KEY;

// Kecepatan rata-rata untuk estimasi ETA (km/jam) — digunakan sebagai pengganti Distance Matrix
const AVERAGE_SPEED_KMH = 40;

// Kredensial Explicit MQTT (Opsional jika sudah menyatu di URL)
const MQTT_USER = process.env.MQTT_USERNAME || undefined;
const MQTT_PASS = process.env.MQTT_PASSWORD || undefined;

// Konfigurasi Klien
const client = mqtt.connect(BROKER_URL, {
    username: MQTT_USER,
    password: MQTT_PASS,
    protocol: 'mqtt',
    port: 1883
});

// Google Maps Client tidak lagi digunakan
// const gmapsClient = new Client({});

// Koneksi ke Neon DB (LISTEN/NOTIFY) — Mode LAZY
// LISTEN hanya aktif saat ada aktivitas. Tidak auto-reconnect saat Neon scale-to-zero.
let pgClient = null;
let listenActive = false;              // Apakah LISTEN sedang aktif?
let listenUsingBackup = false;         // Apakah LISTEN sedang ke backup?
let listenReconnectTimer = null;       // Timer reconnect (jika ada)

// Kata kunci error yang menandakan kuota/koneksi Neon habis
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
 * Memastikan koneksi LISTEN aktif. Dipanggil saat ada aktivitas DB
 * (misalnya saat MQTT message masuk). Jika sudah aktif, tidak melakukan apa-apa.
 */
function ensureListenConnection() {
    if (listenActive) return; // Sudah aktif, tidak perlu buat baru
    if (listenReconnectTimer) return; // Sedang dalam proses reconnect
    listenToDatabaseChanges();
}

async function listenToDatabaseChanges() {
    if (listenActive) return; // Guard: hindari koneksi ganda

    // Sinkronkan state LISTEN dengan state db.js
    const dbStatus = db.getStatus();
    if (dbStatus.usingBackup && !listenUsingBackup) {
        console.warn('[LISTEN] db.js sudah pakai Backup, LISTEN ikut beralih ke Backup.');
        listenUsingBackup = true;
    }

    // Pilih URL berdasarkan state failover
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
            console.error(`[LISTEN-${sourceLabel}] Koneksi LISTEN terputus:`, err.message);
            listenActive = false;
            pgClient.end().catch(() => { });
            // TIDAK auto-reconnect — biarkan Neon scale-to-zero.
            // LISTEN akan diaktifkan kembali saat ada aktivitas berikutnya via ensureListenConnection().
            console.log('[LISTEN] Tidak auto-reconnect. Menunggu aktivitas berikutnya...');
        });

        await pgClient.connect();
        listenActive = true;
        listenReconnectTimer = null;
        console.log(`[LISTEN-${sourceLabel}] Terhubung. Mendengarkan saluran Postgres...`);

        await pgClient.query('LISTEN status_ambulans_channel');

        // Saat koneksi ditutup (Neon scale-to-zero atau network drop):
        // TIDAK reconnect otomatis. Biarkan Neon tidur.
        pgClient.on('end', () => {
            listenActive = false;
            console.warn(`[LISTEN-${sourceLabel}] Koneksi LISTEN ditutup (Neon scale-to-zero). Menunggu aktivitas berikutnya...`);
            // TIDAK memanggil scheduleReconnect() — ini yang mencegah loop!
        });

        // Tangkap notifikasi dari Postgres trigger
        pgClient.on('notification', (msg) => {
            if (msg.channel === 'status_ambulans_channel') {
                const data = JSON.parse(msg.payload);
                console.log(`[REAL-TIME] Status Ambulans ${data.id_ambulans} berubah menjadi ${data.status_baru}`);
                const topic = `ambulans/status/sinkronisasi/${data.id_ambulans}`;
                client.publish(topic, JSON.stringify(data), { qos: 1 });
            }
        });

    } catch (err) {
        listenActive = false;
        listenReconnectTimer = null;
        const isUnavailable = isNeonUnavailableError(err);

        if (isUnavailable && !listenUsingBackup) {
            // Primary tidak tersedia → coba backup sekali
            console.warn(`[LISTEN-PRIMARY] Tidak tersedia (${err.message}). Mencoba BACKUP...`);
            listenUsingBackup = true;
            listenReconnectTimer = setTimeout(listenToDatabaseChanges, 2000);
        } else if (isUnavailable && listenUsingBackup) {
            // Backup juga tidak tersedia → TIDAK retry. Tunggu aktivitas berikutnya.
            console.warn(`[LISTEN-BACKUP] Juga tidak tersedia. Menunggu aktivitas berikutnya...`);
        } else {
            console.error(`[LISTEN-${sourceLabel}] Gagal:`, err.message);
            // TIDAK retry otomatis — biarkan idle.
        }
    }
}
// Jalankan listener saat service pertama kali start
listenToDatabaseChanges();

// Definisi Topik
const TOPIC_PERMINTAAN_AMBULANS = 'panggilan/masuk';                    // T2
const TOPIC_UPDATE_LOKASI_DRIVER = 'ambulans/lokasi/update/+';          // T1
const TOPIC_KONFIRMASI_TUGAS_DRIVER = 'ambulans/respons/konfirmasi';    // T4
const TOPIC_UPDATE_STATUS_OPERASIONAL = 'ambulans/status/update';       // Topik Tambahan
const TOPIC_PEMBATALAN_PASIEN = 'panggilan/batal/pasien';               // Topik Pembatalan
const TOPIC_PEMBATALAN_DRIVER = 'panggilan/batal/driver';               // Topik Pembatalan Driver

// Menyimpan daftar driver yang menolak panggilan
const rejectedDriversMap = {};

// --- KONEKSI MQTT & ROUTER ---

/**
 * Fungsi utama yang dieksekusi saat berhasil terhubung ke broker MQTT.
 * Fungsi ini subscribe ke semua topik yang relevan.
 */
client.on('connect', () => {
    console.log(`\n================================================================`);
    console.log(` AMBULANCE DISPATCH SERVICE v1.0`);
    console.log(` Mode  : PENGUJIAN ALGORITMA`);
    console.log(` Waktu : ${new Date().toISOString().replace('T', ' ').substring(0, 19)}`);
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
            if (!err) console.log(`[OK] Berhasil subscribe ke: ${t}`);
        });
    });

    console.log(`\n================================================================`);
    console.log(` Menunggu update lokasi driver dan panggilan pasien...`);
    console.log(`================================================================\n`);
}); // End Client Connect


/**
 * Router utama untuk semua pesan yang masuk dari MQTT.
 * Mem-parsing pesan dan memanggil fungsi handler yang sesuai berdasarkan topik.
 * @param {string} topic - Topik MQTT tempat pesan diterima.
 * @param {Buffer} message - Konten pesan dalam bentuk Buffer.
 */
client.on('message', async (topic, message) => {
    try {
        const payload = JSON.parse(message.toString());

        // Pastikan koneksi LISTEN aktif saat ada aktivitas MQTT
        ensureListenConnection();

        // 1. Alur Update Lokasi Driver
        if (topic.startsWith('ambulans/lokasi/update/')) { // T1
            const { error, value } = schemaT1.validate(payload);
            if (error) {
                console.error(`[VALIDASI] Payload T1 Error dari topik ${topic}:`, error.message);
                return;
            }
            const driverId = topic.split('/')[3]; // ambulans/lokasi/update/{id_ambulans}
            await handleDriverLocationUpdate(driverId, value);
            // menampilkan satu titik agar tau server masih hidup
            // process.stdout.write('.'); 

        }

        // 2. Alur Panggilan Darurat
        else if (topic === TOPIC_PERMINTAAN_AMBULANS) { // T2
            const { error, value } = schemaT2.validate(payload);
            if (error) {
                logger.error(`[VALIDASI] Payload T2 Error: ${error.message}`, { category: 'dispatch' });
                return;
            }
            logger.info('Menerima permintaan ambulans pasien...', { category: 'dispatch' });
            await handlePatientRequest(value);
        }

        // 3. Alur Konfirmasi dari Driver
        else if (topic === TOPIC_KONFIRMASI_TUGAS_DRIVER) { // T4
            const { error, value } = schemaT4.validate(payload);
            if (error) {
                console.error(`[VALIDASI] Payload T4 Error:`, error.message);
                return;
            }
            await handleDriverTaskConfirmation(value);
        }

        else if (topic === TOPIC_UPDATE_STATUS_OPERASIONAL) { // Topik Tambahan
            const { error, value } = schemaStatusOp.validate(payload);
            if (error) {
                console.error(`[VALIDASI] Payload Status Ops Error:`, error.message);
                return;
            }
            await handleOperationalStatusUpdate(value);
        }

        // 5. Alur Pembatalan Panggilan dari Pasien
        else if (topic === TOPIC_PEMBATALAN_PASIEN) {
            const { error, value } = schemaBatal.validate(payload);
            if (error) {
                console.error(`[VALIDASI] Payload Pembatalan Error:`, error.message);
                return;
            }
            await handlePatientCancellation(value);
        }
        else if (topic === TOPIC_PEMBATALAN_DRIVER) {
            const { error, value } = schemaBatal.validate(payload);
            if (error) {
                console.error(`[VALIDASI] Payload Pembatalan Driver Error:`, error.message);
                return;
            }
            await handleDriverCancellation(value);
        }
    }
    catch (e) {
        console.error(`Gagal memproses pesan di topik ${topic}:`, e.message || e);
    }
}); //End Client Message


/**
 * Handler untuk error koneksi MQTT.
 */
client.on('error', (error) => {
    console.error('Koneksi MQTT Error:', error);
});



/**
 * =======================================================
 * FUNGSI HANDLER (ALUR LOGIKA)
 * =======================================================
 */

/**
 * [ALUR 1 - T1] Menangani pembaruan lokasi real-time dari driver.
 * Menerima data dari T1 dan memperbarui tabel 'ambulans' di DB.
 * @param {string} driverId - ID driver yang diekstrak dari topik.
 * @param {object} data - Payload JSON dari T1 (lokasi_latitude, lokasi_longitude).
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
            console.error(`[T5] Data lokasi driver ${driverId} tidak lengkap. Payload:`, data);
            return;
        }
        const result = await db.query(sql, [lokasi_latitude, lokasi_longitude, driverId]);
        if (result.rowCount === 0) {
            console.warn(`[T5] Pembaruan lokasi gagal: Driver ID ${driverId} tidak ditemukan di DB.`);
        } else {
            // console.log(`[T5] Lokasi Driver ${driverId} diperbarui di DB.`);
        }
    } catch (error) {
        console.error(`[T5] Gagal memperbarui lokasi driver ${driverId}:`, error.sqlMessage || error.message);
    }
} //End function handleDriverLocationUpdate


/**
 * [ALUR 2 - T2] Menangani panggilan darurat baru dari pasien.
 * Ini adalah fungsi koordinator utama untuk proses dispatch.
 * @param {object} data - Payload JSON dari T2 (id_pasien, lokasi_pasien_lat, lokasi_pasien_lon, jenis_layanan).
 * @returns {Promise<void>}
 */
async function handlePatientRequest(data) {
    const { id_pasien, lokasi_pasien_lat, lokasi_pasien_lon, jenis_layanan, kondisi } = data;
    const layanan = jenis_layanan || 'DARURAT';

    if (!id_pasien || !lokasi_pasien_lat || !lokasi_pasien_lon) {
        logger.error("Data tidak lengkap. Payload: " + JSON.stringify(data), { category: 'dispatch' });
        return;
    }

    const patientLocation = { latitude: lokasi_pasien_lat, longitude: lokasi_pasien_lon };
    let newCallId;

    try {
        // Ambil nama pasien dari DB untuk logging
        let namaPasien = `Pasien ${id_pasien}`;
        const pasienData = await db.query(`SELECT nama FROM pasien WHERE id_pasien = $1`, [id_pasien]);
        if (pasienData.rows.length > 0 && pasienData.rows[0].nama) namaPasien = pasienData.rows[0].nama;

        // Banner panggilan darurat baru (Terminal)
        console.log(`\n================================================================`);
        console.log(` PANGGILAN DARURAT BARU`);
        console.log(` Pasien  : ${namaPasien}`);
        console.log(` Waktu   : ${new Date().toISOString().replace('T', ' ').substring(0, 19)}`);
        console.log(`================================================================`);
        console.log(``);
        console.log(` Lokasi Pasien:`);
        console.log(`   Latitude  : ${lokasi_pasien_lat}`);
        console.log(`   Longitude : ${lokasi_pasien_lon}`);
        console.log(`   Jenis     : ${layanan}`);
        console.log(``);

        // Log ke System Monitor
        logger.info(`----------------------------------------------------------------`, { category: 'dispatch' });
        logger.info(`PANGGILAN DARURAT BARU | Pasien: ${namaPasien}`, { category: 'dispatch' });
        logger.info(`Lokasi: ${lokasi_pasien_lat}, ${lokasi_pasien_lon} | Jenis: ${layanan}`, { category: 'dispatch' });
        logger.info(`----------------------------------------------------------------`, { category: 'dispatch' });

        // Langkah 1: Catat panggilan darurat ke DB untuk mendapatkan ID
        const sqlInsertCall = `
            INSERT INTO transaksi_panggilan
            (id_pasien, lokasi_pasien_lat, lokasi_pasien_lon, jenis_layanan, kondisi, status_panggilan, waktu_panggilan, createdAt, updatedAt)
            VALUES ($1, $2, $3, $4, $5, 'PENDING', NOW(), NOW(), NOW())
            RETURNING id_panggilan
        `;
        const result = await db.query(sqlInsertCall, [
            id_pasien,
            lokasi_pasien_lat,
            lokasi_pasien_lon,
            layanan,
            kondisi
        ]);
        newCallId = result.rows[0].id_panggilan;
        logger.info(`Panggilan baru dari ${namaPasien} dicatat di DB.`, { category: 'dispatch' });

        // Langkah 2: Temukan driver terbaik menggunakan Hybrid Model (Filter + Refine)
        logger.info(`Memulai proses pencarian driver...`, { category: 'dispatch' });
        const bestDriver = await _findBestDriver(patientLocation, newCallId, layanan);

        // Langkah 3: Tugaskan driver dan kirim notifikasi
        await _assignDriverToCall(bestDriver, newCallId, patientLocation, id_pasien, kondisi);

    } catch (error) {
        console.error(`Gagal memproses permintaan bantuan:`, error.message);
        if (newCallId) {
            await db.query(`UPDATE transaksi_panggilan SET status_panggilan = 'FAILED' WHERE id_panggilan = $1`, [newCallId]).catch(console.error);
        }
    }
} //End function handlePatientRequest


/**
 * [ALUR 3 - T4] Menangani konfirmasi tugas dari driver.
 * Menerima T4 (diterima/selesai) dan memperbarui status di DB.
 * @param {object} data - Payload JSON dari T4 (id_panggilan, id_ambulans, status).
 * @returns {Promise<void>}
 */
async function handleDriverTaskConfirmation(data) {
    // 1. EKSTRAKSI DATA & HANDLING MISMATCH
    const id_panggilan = data.id_panggilan;
    const status = data.status;
    const id_ambulans = data.id_driver || data.id_ambulans;

    // Coba ambil nama driver dari DB untuk log yang lebih baik
    let namaDriver = `Ambulans ${id_ambulans}`;
    try {
        const driverData = await db.query(`SELECT nama_driver FROM ambulans WHERE id_ambulans = $1`, [id_ambulans]);
        if (driverData.rows.length > 0 && driverData.rows[0].nama_driver) namaDriver = driverData.rows[0].nama_driver;
    } catch (e) { }

    logger.info(`Konfirmasi dari ${namaDriver}: ${status.toUpperCase()}`, { category: 'dispatch' });

    // Validasi Data
    if (!id_panggilan || !id_ambulans || !status) {
        logger.error(`Data konfirmasi tugas tidak lengkap. Payload: ${JSON.stringify(data)}`, { category: 'dispatch' });
        return;
    }

    try {
        // Skenario A: Diterima
        if (status == 'diterima') {
            // Update status_panggilan menjadi OTW
            await db.query(
                `UPDATE transaksi_panggilan SET status_panggilan = 'ON_THE_WAY', id_ambulans_respons = $1 WHERE id_panggilan = $2`,
                [id_ambulans, id_panggilan]
            );

            // Update status driver menjadi Sibuk (busy)
            await db.query(
                `UPDATE ambulans SET status_operasional = 'BUSY' WHERE id_ambulans = $1`,
                [id_ambulans]
            );

            // Hapus data penolakan dari memori agar bersih
            if (rejectedDriversMap[id_panggilan]) delete rejectedDriversMap[id_panggilan];

            console.log(`\n================================================================`);
            console.log(` PENUGASAN DITERIMA`);
            console.log(` Driver : ${namaDriver}`);
            console.log(` Status : Driver sedang menuju lokasi pasien.`);
            console.log(`================================================================\n`);
            logger.info(`${namaDriver} MENERIMA TUGAS`, { category: 'dispatch' });

            const callData = await db.query(
                `SELECT id_pasien FROM transaksi_panggilan WHERE id_panggilan = $1`,
                [id_panggilan]
            );

            if (callData.rows.length > 0) {
                const id_pasien = callData.rows[0].id_pasien;

                // 4. KIRIM NOTIFIKASI KE PASIEN BAHWA DRIVER SUDAH OTW
                const topicBalasanPasien = `panggilan/status/pasien/${id_pasien}`;
                const payloadUntukPasien = {
                    status_panggilan: "menuju_lokasi", // <-- Ini yang ditunggu SearchingActivity Android
                    id_panggilan: id_panggilan,
                    id_ambulans: id_ambulans,
                    eta_detik: 0 // Anda bisa hitung ulang ETA di sini jika perlu
                };

                client.publish(topicBalasanPasien, JSON.stringify(payloadUntukPasien), { qos: 1 });
                logger.info(`[T8] Balasan dikirim ke Pasien: ${topicBalasanPasien}`, { category: 'dispatch' });
            }
        }

        // Skenario B: Ditolak atau Kendala (re-dispatching)
        else if (status === 'ditolak' || status === 'kendala') {

            // 1. Validasi Kritis: Tanpa id_panggilan, kita tidak bisa mencari di DB
            if (!id_panggilan) {
                logger.error(`Eror Fatal: Driver mengirim status '${status}', tapi 'id_panggilan' tidak dikirim oleh Android!`, { category: 'dispatch' });
                return; // Hentikan proses agar server tidak crash
            }

            const actionName = status === 'kendala' ? 'MENGALAMI KENDALA PADA' : 'MENOLAK';

            console.log(`\n================================================================`);
            console.log(` PENUGASAN ${status === 'kendala' ? 'TERKENDALA' : 'DITOLAK'}`);
            console.log(` Driver : ${namaDriver}`);
            console.log(` Status : Mencari kandidat driver berikutnya...`);
            console.log(`================================================================\n`);
            logger.warn(`${namaDriver} ${actionName} tugas # ${id_panggilan}.`, { category: 'dispatch' });

            // 2. Inisialisasi daftar penolakan jika belum ada
            if (!Array.isArray(rejectedDriversMap[id_panggilan])) {
                rejectedDriversMap[id_panggilan] = [];
            }

            // 1. Kembalikan status driver penolak menjadi 'siaga'
            // await db.query(`UPDATE ambulans SET status_operasional = 'Siaga' WHERE id_ambulans = ?`, [id_ambulans]);

            // 3. Masukkan ke daftar hitam (Blacklist) untuk panggilan ini
            if (!rejectedDriversMap[id_panggilan].includes(id_ambulans)) {
                rejectedDriversMap[id_panggilan].push(id_ambulans);
            }

            try {
                // 4. Ambil data panggilan dari DB (Ambil jenis_layanan, lokasi & kondisi) 
                const callData = await db.query(
                    `SELECT lokasi_pasien_lat, lokasi_pasien_lon, jenis_layanan, id_pasien, kondisi FROM transaksi_panggilan WHERE id_panggilan = $1`,
                    [id_panggilan]
                );

                if (callData.length > 0) {
                    const row = callData[0];

                    const dataPasien = callData[0];
                    const patientLocation = {
                        latitude: parseFloat(row.lokasi_pasien_lat),
                        longitude: parseFloat(row.lokasi_pasien_lon)
                    };

                    // Validasi: Jika koordinat NaN, jangan lanjutkan
                    if (isNaN(patientLocation.latitude) || isNaN(patientLocation.longitude)) {
                        throw new Error(`Koordinat pasien untuk panggilan # ${id_panggilan} tidak valid/kosong di DB.`);
                    }

                    // 5. Jalankan Algoritma Seleksi Ulang (Pencarian Driver Terdekat Berikutnya)
                    const nextBestDriver = await _findBestDriver(
                        patientLocation,
                        id_panggilan,
                        dataPasien.jenis_layanan,
                        rejectedDriversMap[id_panggilan] //kirim daftar penolak
                    );

                    logger.info(`Kandidat pengganti ditemukan: ${nextBestDriver.nama}`, { category: 'dispatch' });

                    // 6. Tugaskan driver baru dengan menyertakan kondisi dari DB
                    await _assignDriverToCall(nextBestDriver, id_panggilan, patientLocation, dataPasien.id_pasien, dataPasien.kondisi);
                }
            } catch (errFind) {
                console.log(`\n================================================================`);
                console.log(` PENCARIAN GAGAL`);
                console.log(` Alasan : ${errFind.message}`);
                console.log(`================================================================\n`);
                logger.error(`Gagal mencari pengganti: ${errFind.message}`, { category: 'dispatch' });
                // JIKA TIDAK ADA DRIVER LAGI, BERITAHU PASIEN
                const callDataFallback = await db.query(`SELECT id_pasien FROM transaksi_panggilan WHERE id_panggilan = $1`, [id_panggilan]);
                if (callDataFallback.rows.length > 0) {
                    const id_pasien = callDataFallback.rows[0].id_pasien;
                    const topicBalasanPasien = `panggilan/status/pasien/${id_pasien}`;
                    client.publish(topicBalasanPasien, JSON.stringify({ status_panggilan: "ditolak" }), { qos: 1 });
                }
            }
        }

        // Skenario C: Selesai
        else if (status === 'selesai') {
            // Update status panggilan menjadi COMPLETED
            await db.query(`UPDATE transaksi_panggilan SET status_panggilan = 'COMPLETED' WHERE id_panggilan = $1`, [id_panggilan]);

            // Set driver kembali 'Available' 
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
                logger.info(`Notifikasi SELESAI dikirim ke Pasien: ${topicBalasanPasien}`, { category: 'dispatch' });
            }

            console.log(`\n================================================================`);
            console.log(` TUGAS SELESAI`);
            console.log(` Driver : ${namaDriver}`);
            console.log(` Status : Driver kembali berstatus AVAILABLE.`);
            console.log(`================================================================\n`);
            logger.info(`Tugas Selesai. ${namaDriver} kembali available.`, { category: 'dispatch' });


        }
    } catch (error) {
        console.error(`Gagal memproses konfirmasi T4:`, error.message);
    }
} //End function handleDriverTaskConfirmation


/**
 * Menangani perubahan status operasional driver (Siaga, Sibuk, Off)
 * Sesuai dengan Tabel 3.1 dan Gambar 3.4 di Proposal
 */
async function handleOperationalStatusUpdate(data) {
    const id_ambulans = data.id_ambulans || data.id_driver;
    const status_baru = data.status; // 'Offline', 'Available', 'Busy'

    if (!id_ambulans || !status_baru) {
        console.error("[T6] Data update status tidak lengkap:", data);
        return;
    }

    try {
        await db.query(
            `UPDATE ambulans SET status_operasional = $1 WHERE id_ambulans = $2`,
            [status_baru.toUpperCase(), id_ambulans]
        );
        console.log(`[T6] Status Driver ${id_ambulans} diubah manual menjadi: ${status_baru}`);
    } catch (error) {
        console.error(`[T6] Gagal update status manual:`, error.message);
    }
}


/**
 * Menangani pembatalan panggilan yang dikirim oleh pasien.
 * @param {object} data - Payload JSON yang berisi id_panggilan dan opsional id_ambulans.
 */
async function handlePatientCancellation(data) {
    try {
        logger.info(`[PEMBATALAN] Menerima pembatalan dari pasien untuk panggilan ${data.id_panggilan}`, { category: 'dispatch' });

        if (!data.id_panggilan) {
            logger.error(`[PEMBATALAN] Data pembatalan tidak lengkap (id_panggilan kosong): ${JSON.stringify(data)}`, { category: 'dispatch' });
            return;
        }

        let assignedDriverId = data.id_ambulans; // Fallback jika Android pasien kebetulan mengirimnya

        if (data.id_pasien) {
            // Batal berdasarkan ID Pasien
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
            // Batal menggunakan Integer asli (Jika tersedia)
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
            logger.error("[PEMBATALAN] Android mengirimkan format teks tetapi tidak memberikan id_pasien", { category: 'dispatch' });
            return;
        }

        // 2. Jika sudah ada driver yang ditugaskan, kembalikan ke AVAILABLE dan beri interupsi
        if (assignedDriverId) {
            await db.query(
                "UPDATE ambulans SET status_operasional = 'AVAILABLE' WHERE id_ambulans = $1",
                [assignedDriverId]
            );

            // Tembak Kill Signal ke Driver spesifik!
            const topicInterupsi = `ambulans/interupsi/batal/${assignedDriverId}`;
            client.publish(topicInterupsi, JSON.stringify({
                pesan: "Pasien membatalkan pesanan. Silakan kembali bersiaga."
            }), { qos: 1 });

            logger.info(`[PEMBATALAN] Sinyal batal berhasil dikirim ke armada ${assignedDriverId}`, { category: 'dispatch' });
        }
    } catch (error) {
        logger.error(`[PEMBATALAN] Gagal memproses pembatalan pasien: ${error.message}`, { category: 'dispatch' });
    }
}


/**
 * Menangani pembatalan panggilan yang dikirim oleh driver (misal: kendala kendaraan).
 */
async function handleDriverCancellation(data) {
    try {
        logger.info(`[PEMBATALAN DRIVER] Menerima pembatalan dari driver untuk panggilan ${data.id_panggilan}`, { category: 'dispatch' });

        if (!data.id_panggilan) {
            logger.error(`[PEMBATALAN DRIVER] Data pembatalan tidak lengkap (id_panggilan kosong)`, { category: 'dispatch' });
            return;
        }

        const validId = parseInt(data.id_panggilan);

        // Update transaksi menjadi CANCELLED dan ambil info untuk notifikasi
        const result = await db.query(`
            UPDATE transaksi_panggilan 
            SET status_panggilan = 'CANCELLED' 
            WHERE id_panggilan = $1
            RETURNING id_pasien, id_ambulans_respons`, [validId]);

        if (result.rows.length > 0) {
            const row = result.rows[0];

            // 1. Ubah status driver menjadi OFFLINE karena terdapat kendala kendaraan
            const driverId = row.id_ambulans_respons || data.id_ambulans;
            if (driverId) {
                await db.query(
                    "UPDATE ambulans SET status_operasional = 'OFFLINE' WHERE id_ambulans = $1",
                    [driverId]
                );
                logger.info(`[PEMBATALAN DRIVER] Status driver ${driverId} diubah menjadi OFFLINE akibat kendala kendaraan.`, { category: 'dispatch' });
            }

            // 2. Beri notifikasi pembatalan ke pasien
            if (row.id_pasien) {
                const topicBalasanPasien = `panggilan/status/pasien/${row.id_pasien}`;
                client.publish(topicBalasanPasien, JSON.stringify({
                    id_panggilan: validId,
                    status: 'BATAL_OLEH_DRIVER',
                    pesan: "Driver membatalkan pesanan karena kendala operasional kendaraan. Silakan lakukan pesanan ulang."
                }), { qos: 1 });
                logger.info(`[PEMBATALAN DRIVER] Sinyal batal berhasil dikirim ke pasien ${row.id_pasien}`, { category: 'dispatch' });
            }
        } else {
             logger.error(`[PEMBATALAN DRIVER] Panggilan ${validId} tidak ditemukan atau sudah batal.`, { category: 'dispatch' });
        }
    } catch (error) {
        logger.error(`[PEMBATALAN DRIVER] Gagal memproses pembatalan: ${error.message}`, { category: 'dispatch' });
    }
}


/**
 * =======================================================
 * FUNGSI HELPER
 * =======================================================
 */

/**
 * [HELPER UNTUK T3] Menjalankan logika pencarian driver terdekat (Haversine-Only).
 * Mode ini tidak menggunakan Google Distance Matrix API.
 * ETA diestimasi berdasarkan jarak Haversine dengan kecepatan rata-rata.
 * @param {object} patientLocation - Objek { latitude, longitude } pasien.
 * @param {number} newCallId - ID panggilan darurat yang baru dibuat.
 * @param {string} jenisLayanan - Jenis layanan (DARURAT/TRANSPORT).
 * @param {Array} excludedDriverIds - Daftar ID driver yang sudah menolak.
 * @returns {Promise<object>} Objek driver terbaik yang berisi { id, nama, etaSeconds, distanceHaversine }.
 * @throws {Error} Jika tidak ada driver yang ditemukan.
 */
async function _findBestDriver(patientLocation, newCallId, jenisLayanan, excludedDriverIds = []) {

    // logika klasifikasi armada
    let targetKategori = 'PSC';
    if (jenisLayanan === 'JENAZAH') {
        targetKategori = 'JENAZAH';
    } else if (jenisLayanan === 'TRANSPORT') {
        targetKategori = 'RELAWAN';
    }
    
    logger.info(`Mencari armada kategori ${targetKategori} untuk layanan ${jenisLayanan}...`, { category: 'dispatch' });

    // Gunakan safeBlacklist agar tidak crash
    const safeBlacklist = Array.isArray(excludedDriverIds) ? excludedDriverIds : [];

    // Gunakan status 'available' sesuai proposal untuk driver yang siap bertugas.
    const sqlGetDrivers = `
        SELECT id_ambulans, nama_driver, lokasi_latitude, lokasi_longitude 
        FROM ambulans 
        WHERE status_operasional = 'AVAILABLE'
          AND lokasi_latitude IS NOT NULL
          AND kategori_armada = $1
    `;


    // 1. Ambil driver dari DB
    const result = await db.query(sqlGetDrivers, [targetKategori]);
    const drivers = result.rows;

    if (drivers.length === 0) {
        await db.query(`UPDATE transaksi_panggilan SET status_panggilan = 'NO_DRIVERS' WHERE id_panggilan = $1`, [newCallId]);
        throw new Error(`Tidak ada armada ${targetKategori} yang online.`);
    }

    // Membuang driver yang ada di daftar 'excludeDriverIds'
    const validDrivers = drivers.filter(driver =>
        !safeBlacklist.includes(driver.id_ambulans) &&
        !safeBlacklist.includes(String(driver.id_ambulans))
    );

    if (validDrivers.length === 0) {
        // Jika semua driver menolak atau tidak ada driver
        await db.query(`UPDATE transaksi_panggilan SET status_panggilan = 'NO_DRIVERS_AVAILABLE' WHERE id_panggilan = $1`, [newCallId]);
        throw new Error(`Tidak ada driver ${targetKategori} yang tersedia.`);
    }

    // 2. TAHAP 1: FILTER (Haversine)
    console.log(`----------------------------------------------------------------`);
    logger.info(`--- TAHAP 1: FILTER -- Algoritma Haversine (Jarak Garis Lurus) ---`, { category: 'dispatch' });
    logger.info(`Menghitung jarak dari ${validDrivers.length} driver ke lokasi pasien...`, { category: 'dispatch' });
    console.log(``);
    console.log(` Driver          | Latitude       | Longitude      | Haversine (m)`);
    console.log(`----------------------------------------------------------------`);
    const driversWithDistance = validDrivers.map(driver => {
        const driverLocation = {
            latitude: parseFloat(driver.lokasi_latitude),
            longitude: parseFloat(driver.lokasi_longitude)
        };
        const distanceHaversine = getDistance(patientLocation, driverLocation);
        const nama = driver.nama_driver || `Ambulans ${driver.id_ambulans}`;

        // Estimasi ETA: jarak (m) / kecepatan (m/s)
        // Kecepatan rata-rata 40 km/jam = 11.11 m/s
        const speedMs = (AVERAGE_SPEED_KMH * 1000) / 3600; // konversi km/jam ke m/s
        const etaSeconds = Math.round(distanceHaversine / speedMs);
        const etaMinutes = Math.round(etaSeconds / 60);
        const etaText = etaMinutes < 1 ? '< 1 menit' : `${etaMinutes} menit`;

        console.log(` ${nama.padEnd(16)}| ${String(driver.lokasi_latitude).padEnd(15)}| ${String(driver.lokasi_longitude).padEnd(15)}| ${String(distanceHaversine).padStart(12)}`);
        logger.info(`  -> ${nama.padEnd(15)} | Haversine: ${distanceHaversine} m`, { category: 'dispatch' });

        return {
            id: driver.id_ambulans,
            nama: nama,
            location: driverLocation,
            distanceHaversine: distanceHaversine,
            etaSeconds: etaSeconds,
            etaText: etaText
        };
    });
    console.log(`----------------------------------------------------------------`);

    driversWithDistance.sort((a, b) => a.distanceHaversine - b.distanceHaversine); // Mengurutkan berdasarkan jarak haversine terdekat

    // Log peringkat Haversine
    console.log(``);
    console.log(` Hasil Peringkat Haversine (terurut terdekat ke terjauh):`);
    logger.info(`Hasil Peringkat Haversine (terurut terdekat ke terjauh):`, { category: 'dispatch' });
    driversWithDistance.forEach((c, i) => {
        const marker = i === 0 ? '  << Terdekat' : '';
        console.log(`   ${i + 1}. ${c.nama.padEnd(15)} -> ${c.distanceHaversine} m${marker}`);
        logger.info(`  ${i + 1}. ${c.nama} -> ${c.distanceHaversine} m${marker}`, { category: 'dispatch' });
    });
    console.log(``);

    // Pilih driver terdekat berdasarkan Haversine
    const bestDriver = driversWithDistance[0];

    // Kesimpulan
    console.log(``);
    console.log(`================================================================`);
    console.log(` KESIMPULAN:`);
    console.log(`  Driver terpilih : ${bestDriver.nama}`);
    console.log(`  Jarak           : ${bestDriver.distanceHaversine} m`);
    console.log(`  ETA (estimasi)  : ~${bestDriver.etaText}`);
    console.log(`================================================================\n`);
    logger.info(`--- KESIMPULAN ---`, { category: 'dispatch' });
    logger.info(`Driver terpilih: ${bestDriver.nama} | Jarak: ${bestDriver.distanceHaversine} m | ETA: ~${bestDriver.etaText}`, { category: 'dispatch' });

    return bestDriver;
} //End _findBestDriver


/**
 * [HELPER UNTUK T3] Menugaskan driver ke panggilan dan mengirim notifikasi.
 * @param {object} bestDriver - Objek driver terbaik { id, etaSeconds }.
 * @param {number} callId - ID panggilan darurat dari DB.
 * @param {object} patientLocation - Objek { latitude, longitude } pasien.
 * @param {number} id_pasien - ID pasien yang menelepon.
 * @param {string} kondisi - Kondisi pasien yang dikirim dari aplikasi.
 * @returns {Promise<void>}
 */
async function _assignDriverToCall(bestDriver, callId, patientLocation, id_pasien, kondisi) {
    let namaPasien = "Pasien Darurat"; // Nilai default

    const sqlUpdateCall = `
        UPDATE transaksi_panggilan 
        SET id_ambulans_respons = $1, status_panggilan = 'WAITING_FOR_DRIVER' 
        WHERE id_panggilan = $2
    `;

    console.log(`\n================================================================`);
    console.log(` MENGIRIM PENUGASAN`);
    console.log(` Target : ${bestDriver.nama}`);
    console.log(` Status : Menunggu konfirmasi driver (Timeout: 20 detik)`);
    console.log(`================================================================\n`);

    logger.info(`${bestDriver.nama} DITAWARI tugas panggilan ${callId}. Menunggu konfirmasi...`, { category: 'dispatch' });

    try {
        await db.query(sqlUpdateCall, [bestDriver.id, callId]);
        logger.info(`Update status ${callId} menjadi WAITING_FOR_DRIVER berhasil.`, { category: 'dispatch' });

        // 2. Ambil Nama Pasien dari DB Neon
        const sqlGetPatient = `SELECT nama FROM pasien WHERE id_pasien = $1`;

        const patientData = await db.query(sqlGetPatient, [id_pasien]);

        if (patientData.rows.length > 0) {
            namaPasien = patientData.rows[0].nama;
        }
    } catch (err) {
        console.error("[T3] ERROR SAAT UPDATE DB DI _assignDriverToCall:", err);
    }

    // 2. Update DB: Set status driver menjadi 'BUSY'
    // await db.query(
    //     `UPDATE ambulans SET status_operasional = 'BUSY' WHERE id_ambulans = ?`,
    //     [bestDriver.id]
    // );
    // console.log(`[T3] Driver ${bestDriver.id} ditugaskan untuk panggilan ${callId} di DB.`);

    // 3. Kirim T3: Notifikasi tugas ke driver terpilih
    const topicTugas = `ambulans/tugas/${bestDriver.id}`;
    const payloadTugas = {
        id_panggilan: callId,
        id_pasien: id_pasien,
        nama_pasien: namaPasien,
        lokasi_pasien_lat: patientLocation.latitude,
        lokasi_pasien_lon: patientLocation.longitude,
        eta_detik: bestDriver.etaSeconds,
        kondisi: kondisi // <-- Menggunakan parameter yang dilempar dari luar
    };
    client.publish(topicTugas, JSON.stringify(payloadTugas), { qos: 1 });
    logger.info(`Penawaran Tugas dikirim ke topik ${topicTugas}`, { category: 'dispatch' });

    // Di dalam fungsi _assignDriverToCall, setelah mengirim MQTT tugas:
    setTimeout(async () => {
        try {
            // Cek status panggilan saat ini di database
            const result = await db.query(
                `SELECT status_panggilan, jenis_layanan, id_ambulans_respons FROM transaksi_panggilan WHERE id_panggilan = $1`,
                [callId]
            );

            if (result.rows.length > 0) {
                const callData = result.rows[0];
                const currentStatus = callData.status_panggilan;
                const assignedDriver = callData.id_ambulans_respons;

                // Jika setelah 20 detik statusnya masih WAITING_FOR_DRIVER (belum diterima/ditolak manual) dan driver belum dialihkan
                if (currentStatus === 'WAITING_FOR_DRIVER' && assignedDriver == bestDriver.id) {
                    console.log(`\n================================================================`);
                    console.log(` TIMEOUT PENUGASAN`);
                    console.log(` Driver : ${bestDriver.nama}`);
                    console.log(` Status : Tidak ada respons dalam 20 detik, mencari pengganti.`);
                    console.log(`================================================================\n`);
                    logger.warn(`[TIMEOUT] ${bestDriver.nama} tidak merespons dalam 20 detik. Meneruskan ke driver lain...`, { category: 'dispatch' });

                    // 1. Tembak sinyal batal/timeout ke driver pertama agar aplikasi berhenti berdering
                    const topicInterupsi = `ambulans/interupsi/batal/${bestDriver.id}`;
                    client.publish(topicInterupsi, JSON.stringify({
                        pesan: "Waktu penawaran habis (20 detik). Pesanan otomatis dialihkan."
                    }), { qos: 1 });

                    // 2. Tambah driver ini ke rejectedDriversMap
                    if (!Array.isArray(rejectedDriversMap[callId])) {
                        rejectedDriversMap[callId] = [];
                    }
                    if (!rejectedDriversMap[callId].includes(bestDriver.id)) {
                        rejectedDriversMap[callId].push(bestDriver.id);
                    }

                    try {
                        // 2. Cari nextBestDriver lagi seperti di blok "ditolak"
                        const nextBestDriver = await _findBestDriver(
                            patientLocation,
                            callId,
                            callData.jenis_layanan,
                            rejectedDriversMap[callId]
                        );

                        logger.info(`[TIMEOUT] Kandidat pengganti ditemukan: ${nextBestDriver.nama}`, { category: 'dispatch' });

                        // 3. Panggil ulang _assignDriverToCall dengan driver yang baru
                        await _assignDriverToCall(nextBestDriver, callId, patientLocation, id_pasien, kondisi);
                    } catch (errFind) {
                        console.error(`[TIMEOUT] Gagal mencari pengganti: ${errFind.message}`);
                        // Jika tidak ada driver lain lagi, beritahu pasien
                        const topicBalasanPasien = `panggilan/status/pasien/${id_pasien}`;
                        client.publish(topicBalasanPasien, JSON.stringify({ status_panggilan: "ditolak" }), { qos: 1 });
                    }
                }
            }
        } catch (err) {
            console.error("Gagal melakukan pengecekan timeout:", err);
        }
    }, 20000); // 20000 ms = 20 detik
} //End _assignDriverToCall