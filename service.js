// --- KONFIGURASI ---
const mqtt = require('mqtt');
const db = require('./db.js');
const { getDistance } = require('geolib');
const { Client } = require("@googlemaps/google-maps-services-js");
const { Client: PgClient } = require('pg');

require('dotenv').config();

// Konfigurasi Koneksi
const BROKER_URL = process.env.MQTT_BROKER_URL;
const GOOGLE_MAPS_API_KEY = process.env.GOOGLE_MAPS_API_KEY;

// Konfigurasi Klien
const client = mqtt.connect(BROKER_URL);
const gmapsClient = new Client({});

// Koneksi ke Neon DB
const pgClient = new PgClient({ connectionString: process.env.DATABASE_URL });

async function listenToDatabaseChanges() {
    try {
        await pgClient.connect();
        console.log("[DB] Terhubung ke Neon. Mulai mendengarkan saluran Postgres...");
        // 1. Perintahkan Node.js untuk mendengarkan saluran dari Postgres
        await pgClient.query('LISTEN status_ambulans_channel');
        // 2. Tangkap event ketika Trigger dari Postgres mengeksekusi Notify
        pgClient.on('notification', (msg) => {
            if (msg.channel === 'status_ambulans_channel') {
                const data = JSON.parse(msg.payload);
                console.log(`[REAL-TIME] Status Ambulans ${data.id_ambulans} berubah menjadi ${data.status_baru}`);
                // 3. TERUSKAN KE ANDROID VIA MQTT!
                const topic = `ambulans/status/sinkronisasi/${data.id_ambulans}`;

                // Gunakan variabel 'client' (karena ini inisialisasi MQTT di service.js Anda)
                client.publish(topic, JSON.stringify(data), { qos: 1 });
            }
        });
    } catch (err) {
        console.error("Gagal menjalankan Listener Postgres: ", err);
    }
}
// Jalankan fungsi tersebutambulans/status/update
listenToDatabaseChanges();

// Definisi Topik
const TOPIC_PERMINTAAN_AMBULANS = 'panggilan/masuk';                    // T2
const TOPIC_UPDATE_LOKASI_DRIVER = 'ambulans/lokasi/update/+';          // T1
const TOPIC_KONFIRMASI_TUGAS_DRIVER = 'ambulans/respons/konfirmasi';    // T4
const TOPIC_UPDATE_STATUS_OPERASIONAL = 'ambulans/status/update';       // Topik Tambahan

// Menyimpan daftar driver yang menolak panggilan
const rejectedDriversMap = {};

// --- KONEKSI MQTT & ROUTER ---

/**
 * Fungsi utama yang dieksekusi saat berhasil terhubung ke broker MQTT.
 * Fungsi ini subscribe ke semua topik yang relevan.
 */
client.on('connect', () => {
    console.log('Service terhubung ke Broker MQTT.');

    client.subscribe(TOPIC_PERMINTAAN_AMBULANS, (err) => {
        if (!err) console.log(`Berhasil subscribe ke: ${TOPIC_PERMINTAAN_AMBULANS}`);
    });
    client.subscribe(TOPIC_UPDATE_LOKASI_DRIVER, (err) => {
        if (!err) console.log(`Berhasil subscribe ke: ${TOPIC_UPDATE_LOKASI_DRIVER}`);
    });
    client.subscribe(TOPIC_KONFIRMASI_TUGAS_DRIVER, (err) => {
        if (!err) console.log(`Berhasil subscribe ke: ${TOPIC_KONFIRMASI_TUGAS_DRIVER}`);
    });
    client.subscribe(TOPIC_UPDATE_STATUS_OPERASIONAL, (err) => {
        if (!err) console.log(`Berhasil subscribe ke: ${TOPIC_UPDATE_STATUS_OPERASIONAL}`);
    });
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
        // console.log(`\n======================================================`);
        // console.log(`Pesan diterima pada topik [${topic}]: ${payload}`);

        // 1. Alur Update Lokasi Driver
        if (topic.startsWith('ambulans/lokasi/update/')) { // T1
            const driverId = topic.split('/')[3]; // ambulans/lokasi/update/{id_ambulans}
            await handleDriverLocationUpdate(driverId, payload);
            // menampilkan satu titik agar tau server masih hidup
            // process.stdout.write('.'); 

        }

        // 2. Alur Panggilan Darurat
        else if (topic === TOPIC_PERMINTAAN_AMBULANS) { // T2
            console.log('Menerima permintaan ambulan pasien...');
            await handlePatientRequest(payload);
        }

        // 3. Alur Konfirmasi dari Driver
        else if (topic === TOPIC_KONFIRMASI_TUGAS_DRIVER) { // T4
            await handleDriverTaskConfirmation(payload);
        }

        else if (topic === TOPIC_UPDATE_STATUS_OPERASIONAL) { // Topik Tambahan
            await handleOperationalStatusUpdate(payload);
        }
    }
    catch (e) {
        console.error(`Gagal memproses pesan di topik ${topic}:`, e);
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
    // console.log(`[T2] Menerima permintaan ambulan pasien...`);

    const { id_pasien, lokasi_pasien_lat, lokasi_pasien_lon, jenis_layanan } = data;
    const layanan = jenis_layanan || 'DARURAT';
    console.log(`[T2] Menerima panggilan ${layanan} dari Pasien ${id_pasien}...`);

    if (!id_pasien || !lokasi_pasien_lat || !lokasi_pasien_lon) {
        console.error("[T3] Data tidak lengkap. Payload:", data);
        return;
    }

    const patientLocation = { latitude: lokasi_pasien_lat, longitude: lokasi_pasien_lon };
    let newCallId;

    try {
        // Langkah 1: Catat panggilan darurat ke DB untuk mendapatkan ID
        const sqlInsertCall = `
            INSERT INTO transaksi_panggilan
            (id_pasien, lokasi_pasien_lat, lokasi_pasien_lon, jenis_layanan, status_panggilan, waktu_panggilan, createdAt, updatedAt)
            VALUES ($1, $2, $3, $4, 'PENDING', NOW(), NOW(), NOW())
            RETURNING id_panggilan
        `;
        const result = await db.query(sqlInsertCall, [
            id_pasien,
            lokasi_pasien_lat,
            lokasi_pasien_lon,
            layanan
        ]);
        newCallId = result.rows[0].id_panggilan;
        console.log(`[T2] Panggilan baru (ID: ${newCallId}) dari Pasien ${id_pasien} tipe ${layanan} dicatat di DB.`);

        // Langkah 2: Temukan driver terbaik menggunakan Hybrid Model (Filter + Refine)
        console.log(`[T2] Memulai proses pencarian driver...`);
        const bestDriver = await _findBestDriver(patientLocation, newCallId, layanan);

        // Langkah 3: Tugaskan driver dan kirim notifikasi
        await _assignDriverToCall(bestDriver, newCallId, patientLocation, id_pasien);

    } catch (error) {
        console.error(`[T2] Gagal memproses permintaan bantuan (ID Panggilan: ${newCallId}):`, error.message);
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

    console.log(`[T4] Konfirmasi dari Ambulans ${id_ambulans}: ${status}`);

    // Validasi Data
    if (!id_panggilan || !id_ambulans || !status) {
        console.error(`[T4] Data konfirmasi tugas tidak lengkap. Payload:`, data);
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

            console.log(`[T4] Driver ${id_ambulans}  MENERIMA TUGAS`);

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
                console.log(`[T8] Balasan dikirim ke Pasien: ${topicBalasanPasien}`);
            }
        }

        // Skenario B: Ditolak (re-dispathcing)
        else if (status === 'ditolak') {

            // 1. Validasi Kritis: Tanpa id_panggilan, kita tidak bisa mencari di DB
            if (!id_panggilan) {
                console.error("Eror Fatal: Driver menolak, tapi 'id_panggilan' tidak dikirim oleh Android!");
                return; // Hentikan proses agar server tidak crash
            }

            console.warn(`[T4] Driver ${id_ambulans} MENOLAK tugas # ${id_panggilan}.`);

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
                // 4. Ambil data panggilan dari DB (Ambil jenis_layanan & lokasi) 
                const callData = await db.query(
                    `SELECT lokasi_pasien_lat, lokasi_pasien_lon, jenis_layanan, id_pasien FROM transaksi_panggilan WHERE id_panggilan = $1`,
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

                    console.log(`Kandidat penggant ditemukan: Driver ${nextBestDriver.id}`);

                    // 6. Tugaskan driver baru
                    await _assignDriverToCall(nextBestDriver, id_panggilan, patientLocation, dataPasien.id_pasien);
                }
            } catch (errFind) {
                console.error(`Gagal mencari pengganti: ${errFind.message}`);
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
                console.log(`[T8] Notifikasi SELESAI dikirim ke Pasien: ${topicBalasanPasien}`);
            }

            console.log(`[T7] Tugas Selesai. Driver ${id_ambulans} kembali available.`);


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
 * =======================================================
 * FUNGSI HELPER
 * =======================================================
 */

/**
 * [HELPER UNTUK T3] Menjalankan logika Hybrid Model (Filter + Refine).
 * @param {object} patientLocation - Objek { latitude, longitude } pasien.
 * @param {number} newCallId - ID panggilan darurat yang baru dibuat.
 * @returns {Promise<object>} Objek driver terbaik yang berisi { id, etaSeconds }.
 * @throws {Error} Jika tidak ada driver yang ditemukan atau API gagal.
 */
async function _findBestDriver(patientLocation, newCallId, jenisLayanan, excludedDriverIds = []) {

    // logika klasifikasi armada
    let targetKategori = (jenisLayanan === 'TRANSPORT') ? 'RELAWAN' : 'PSC';

    console.log(`[Filter] Mencari armada kategori ${targetKategori} untuk layanan ${jenisLayanan}`);

    // Gunakan safeBlacklist agar tidak crash
    const safeBlacklist = Array.isArray(excludedDriverIds) ? excludedDriverIds : [];

    // Gunakan status 'available' sesuai proposal untuk driver yang siap bertugas.
    const sqlGetDrivers = `
        SELECT id_ambulans, lokasi_latitude, lokasi_longitude 
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
        throw new Error(`Tidak ada driver ${targetCategory} yang tersedia.`);
    }

    // 2. TAHAP 1: FILTER (Haversine)
    console.log(`--- [T2] TAHAP 1: FILTER (Haversine) ---`);
    const driversWithDistance = validDrivers.map(driver => {
        const driverLocation = {
            latitude: parseFloat(driver.lokasi_latitude),
            longitude: parseFloat(driver.lokasi_longitude)
        };
        const distanceHaversine = getDistance(patientLocation, driverLocation);

        console.log(`  -> Driver ${driver.id_ambulans} | Haversine: ${distanceHaversine} m`);

        return {
            id: driver.id_ambulans,
            location: driverLocation,
            distanceHaversine: distanceHaversine
        };
    });

    driversWithDistance.sort((a, b) => a.distanceHaversine - b.distanceHaversine);
    const candidates = driversWithDistance.slice(0, 3); // Tempat pengambilan 3 kandidat teratas

    // 3. TAHAP 2: REFINE (Google Maps API)
    console.log(`--- [T2] TAHAP 2: REFINE (Google Maps API) ---`);
    console.log(`Mengambil ${candidates.length} kandidat teratas untuk dicek ETA: ${candidates.map(c => c.id).join(', ')}`);

    const origins = [patientLocation];
    const destinations = candidates.map(c => c.location);

    const apiResponse = await gmapsClient.distancematrix({
        params: { key: GOOGLE_MAPS_API_KEY, origins: origins, destinations: destinations, travelMode: 'DRIVING' } //travel mode untuk mobil
    });

    console.log(`--- [T2] HASIL PERHITUNGAN---`);
    const results = [];
    apiResponse.data.rows[0].elements.forEach((element, index) => {
        const candidate = candidates[index];
        if (element.status === 'OK') {
            const api_duration_text = element.duration.text;
            const api_duration_seconds = element.duration.value;
            console.log(`  -> Driver ${candidate.id} (Haversine: ${candidate.distanceHaversine} m) | ETA: ${api_duration_text}`);
            results.push({ id: candidate.id, etaSeconds: api_duration_seconds });
        } else {
            console.log(`  -> Driver ${candidate.id}: Google API Gagal (Status: ${element.status})`);
        }
    });

    if (results.length === 0) {
        await db.query(`UPDATE transaksi_panggilan SET status_panggilan = 'API_FAILED' WHERE id_panggilan = $1`, [newCallId]);
        throw new Error("Gagal mendapatkan hasil ETA dari Google.");
    }

    results.sort((a, b) => a.etaSeconds - b.etaSeconds);
    const bestDriverAPI = results[0];

    console.log(`--- [T2] KESIMPULAN AKHIR (Hybrid Model) ---`);
    console.log(`Driver terdekat (Haversine): ${driversWithDistance[0].id}`);
    console.log(`Driver tercepat (ETA Google API): ${bestDriverAPI.id} (${bestDriverAPI.etaSeconds} dtk)`);

    return bestDriverAPI;
} //End _findBestDriver


/**
 * [HELPER UNTUK T3] Menugaskan driver ke panggilan dan mengirim notifikasi.
 * @param {object} bestDriver - Objek driver terbaik { id, etaSeconds }.
 * @param {number} callId - ID panggilan darurat dari DB.
 * @param {object} patientLocation - Objek { latitude, longitude } pasien.
 * @param {number} id_pasien - ID pasien yang menelepon.
 * @returns {Promise<void>}
 */
async function _assignDriverToCall(bestDriver, callId, patientLocation, id_pasien) {
    let namaPasien = "Pasien Darurat"; // Nilai default

    const sqlUpdateCall = `
        UPDATE transaksi_panggilan 
        SET id_ambulans_respons = $1, status_panggilan = 'WAITING_FOR_DRIVER' 
        WHERE id_panggilan = $2
    `;

    console.log(`[T3] Driver ${bestDriver.id} DITAWARI tugas panggilan ${callId}. Menunggu konfirmasi...`);

    try {
        await db.query(sqlUpdateCall, [bestDriver.id, callId]);
        console.log(`[T3] Driver ${bestDriver.id} DITAWARI tugas panggilan ${callId}. Menunggu konfirmasi...`);

        // 2. Ambil Nama Pasien dari DB Neon
        const sqlGetPatient = `SELECT nama FROM pasien WHERE id_pasien = $1`;

        const patientData = await db.query(sqlGetPatient, [id_pasien]);

        if (patientData.rows.length > 0) {
            namaPasien = patientData.rows[0].nama;
        }

        if (patientData.length > 0) {
            namaPasien = patientData[0].nama_pasien;
        }
    } catch (err) {
        // console.error("[T3] Gagal mengambil nama pasien:", err.message);
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
        eta_detik: bestDriver.etaSeconds
    };
    client.publish(topicTugas, JSON.stringify(payloadTugas), { qos: 1 });
    console.log(`[T3] Penawaran Tugas dikirim ke topik ${topicTugas}`);

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
                    console.log(`[TIMEOUT] Driver ${bestDriver.id} tidak merespons dalam 20 detik. Meneruskan ke driver lain...`);

                    // 1. Tambah driver ini ke rejectedDriversMap
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

                        console.log(`[TIMEOUT] Kandidat pengganti ditemukan: Driver ${nextBestDriver.id}`);

                        // 3. Panggil ulang _assignDriverToCall dengan driver yang baru
                        await _assignDriverToCall(nextBestDriver, callId, patientLocation, id_pasien);
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




    // // 4. Kirim T5: Notifikasi status ke pasien
    // const topicBalasan = `panggilan/status/${callId}`;
    // const topicBalasanPasien = `panggilan/status/pasien/${id_pasien}`; //Perlu diperhatikan bahwa tidak terdapat topik ini
    // const payloadBalasan = {
    //     // status_panggilan: "menuju_lokasi",
    //     status_panggilan: "menunggu_konfirmasi_driver",
    //     id_panggilan: callId,
    //     id_ambulans: bestDriver.id,
    //     eta_detik: bestDriver.etaSeconds
    // };
    // const payloadString = JSON.stringify(payloadBalasan);
    // client.publish(topicBalasan, payloadString, { qos: 1 });
    // // client.publish(topicBalasanPasien, payloadString, { qos: 1 });
    // console.log(`[T5] Balasan dikirim ke ${topicBalasan} dan ${topicBalasanPasien}`);
} //End _assignDriverToCall