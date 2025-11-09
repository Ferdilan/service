// --- KONFIGURASI ---
const mqtt = require('mqtt');
const db = require('./db.js');
const { getDistance } = require('geolib');
const { Client } = require("@googlemaps/google-maps-services-js");

// Konfigurasi Koneksi
const BROKER_URL = 'mqtt://hjppbzvg:hjppbzvg:zUg3ysf369ZnibIfjtSc7Qtj-ezmi5IB@mustang.rmq.cloudamqp.com';
const GOOGLE_MAPS_API_KEY = "AIzaSyCNoOEGNx1eycV5Pc3SvY6a3BrG_fRqvbg"; 

// Konfigurasi Klien
const client = mqtt.connect(BROKER_URL);
const gmapsClient = new Client({});

// Definisi Topik
const TOPIC_REGISTRASI_PASIEN = 'pasien/registrasi/request';            // T1
const TOPIC_PERMINTAAN_DARURAT = 'panggilan/darurat/masuk';             // T3
const TOPIC_UPDATE_LOKASI_DRIVER = 'ambulans/lokasi/update/+';          // T5
const TOPIC_KONFIRMASI_TUGAS_DRIVER = 'ambulans/respons/konfirmasi';    // T7

// --- KONEKSI MQTT & ROUTER ---

/**
 * Fungsi utama yang dieksekusi saat berhasil terhubung ke broker MQTT.
 * Fungsi ini subscribe ke semua topik yang relevan.
 */
client.on('connect', () => {
    console.log('Service terhubung ke Broker MQTT.');

    client.subscribe(TOPIC_REGISTRASI_PASIEN, (err) => {
        if (!err) console.log(`Berhasil subscribe ke: ${TOPIC_REGISTRASI_PASIEN}`);
    });
    client.subscribe(TOPIC_PERMINTAAN_DARURAT, (err) => {
        if (!err) console.log(`Berhasil subscribe ke: ${TOPIC_PERMINTAAN_DARURAT}`);
    });
    client.subscribe(TOPIC_UPDATE_LOKASI_DRIVER, (err) => {
        if (!err) console.log(`Berhasil subscribe ke: ${TOPIC_UPDATE_LOKASI_DRIVER}`);
    });
    client.subscribe(TOPIC_KONFIRMASI_TUGAS_DRIVER, (err) => {
        if (!err) console.log(`Berhasil subscribe ke: ${TOPIC_KONFIRMASI_TUGAS_DRIVER}`);
    });
}); // End Client Connect


/**
 * Router utama untuk semua pesan yang masuk dari MQTT.
 * Mem-parsing pesan dan memanggil fungsi handler yang sesuai berdasarkan topik.
 * @param {string} topic - Topik MQTT tempat pesan diterima.
 * @param {Buffer} message - Konten pesan dalam bentuk Buffer.
 */
client.on('message', async (topic, message) => {
    let payload;
    try{
        const payload = JSON.parse(message.toString());
        console.log(`\n======================================================`);
        console.log(`Pesan diterima pada topik [${topic}]: ${payload}`);

        // 1. Alur Registrasi Pasien
        if (topic === TOPIC_REGISTRASI_PASIEN) { // T1 
            await handlePatientRegistration(payload);
        }

        // 2. Alur Update Lokasi Driver
         else if (topic.startsWith('ambulans/lokasi/update/')) { // Penyesuaian T5 
            const driverId = topic.split('/')[3]; // ambulans/lokasi/update/{id_ambulans}
            await handleDriverLocationUpdate(driverId, payload);

        }

        // 3. Alur Panggilan Darurat
        else if (topic === TOPIC_PERMINTAAN_DARURAT) { // T3
            console.log('Menerima permintaan ambulan pasien...');
            await handlePatientRequest(payload);
        }

        // 4. Alur Konfirmasi dari Driver
        else if (topic === TOPIC_KONFIRMASI_TUGAS_DRIVER) { // T7 
            await handleDriverTaskConfirmation(payload);
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


// --- FUNGSI HANDLER (ALUR LOGIKA) ---

/**
 * [ALUR 1 - T1] Menangani registrasi pasien baru.
 * Menerima data dari topik T1 dan menyimpannya ke tabel 'pasien'.
 * @param {object} data - Payload JSON dari T1 (nik, nama_lengkap, dll).
 * @returns {Promise<void>}
 */
async function handlePatientRegistration(data) {
    console.log(`[T1] Menerima data registrasi pasien baru...`);
    const sql = `
        INSERT INTO pasien (nik, nama_lengkap, tgl_lahir, alamat, jenis_kelamin) 
        VALUES (?, ?, ?, ?, ?)
    `;
    try {
        const { nik, nama_lengkap, tgl_lahir, alamat, jenis_kelamin } = data;
        if (!nik || !nama_lengkap || !tgl_lahir || !alamat || !jenis_kelamin) {
            console.error("[T1] Data registrasi tidak lengkap. Payload:", data);
            return;
        }
        const [result] = await db.execute(sql, [nik, nama_lengkap, tgl_lahir, alamat, jenis_kelamin]);
        console.log(`[T1] Pasien baru berhasil terdaftar dengan ID: ${result.insertId}`);
        // TODO: Kirim balasan ke T2 (pasien/registrasi/respons/{request_id}) //Untuk Login
    } catch (error) {
        console.error(`[T1] Gagal menyimpan data pasien:`, error.sqlMessage || error.message);
    }
} //End function handlePatientRegistration


/**
 * [ALUR 2 - T5] Menangani pembaruan lokasi real-time dari driver.
 * Menerima data dari T5 dan memperbarui tabel 'ambulans' di DB.
 * @param {string} driverId - ID driver yang diekstrak dari topik.
 * @param {object} data - Payload JSON dari T5 (lokasi_latitude, lokasi_longitude).
 * @returns {Promise<void>}
 */
async function handleDriverLocationUpdate(driverId, data) {
    const sql = `
        UPDATE ambulans 
        SET 
            lokasi_latitude = ?, 
            lokasi_longitude = ?, 
            timestamp_update = NOW(),
            status_operasional = 'ONLINE'
        WHERE id_ambulans = ?
    `;
    try {
        const { lokasi_latitude, lokasi_longitude } = data;
        if (lokasi_latitude === undefined || lokasi_longitude === undefined) {
             console.error(`[T5] Data lokasi driver ${driverId} tidak lengkap. Payload:`, data);
             return;
        }
        const [result] = await db.execute(sql, [lokasi_latitude, lokasi_longitude, driverId]);
        if (result.affectedRows === 0) {
            console.warn(`[T5] Pembaruan lokasi gagal: Driver ID ${driverId} tidak ditemukan di DB.`);
        } else {
            // console.log(`[T5] Lokasi Driver ${driverId} diperbarui di DB.`);
        }
    } catch (error) {
        console.error(`[T5] Gagal memperbarui lokasi driver ${driverId}:`, error.sqlMessage || error.message);
    }
} //End function handleDriverLocationUpdate
 

/**
 * [ALUR 3 - T3] Menangani panggilan darurat baru dari pasien.
 * Ini adalah fungsi koordinator utama untuk proses dispatch.
 * @param {object} data - Payload JSON dari T3 (id_pasien, lokasi_pasien_lat, dll).
 * @returns {Promise<void>}
 */
async function handlePatientRequest(data) {
    console.log(`[T3] Menerima permintaan ambulan pasien...`);
    const { id_pasien, lokasi_pasien_lat, lokasi_pasien_lon } = data;
    if (id_pasien === undefined || lokasi_pasien_lat === undefined || lokasi_pasien_lon === undefined) {
        console.error("[T3] Permintaan darurat tidak lengkap. Payload:", data);
        return;
    }

    const patientLocation = { latitude: lokasi_pasien_lat, longitude: lokasi_pasien_lon };
    let newCallId;

    try {
        // Langkah 1: Catat panggilan darurat ke DB untuk mendapatkan ID
        const sqlInsertCall = `
            INSERT INTO panggilan_darurat 
            (id_pasien, lokasi_pasien_lat, lokasi_pasien_lon, status_panggilan, waktu_panggilan)
            VALUES (?, ?, ?, 'PENDING', NOW())
        `;
        const [result] = await db.execute(sqlInsertCall, [
            id_pasien,
            lokasi_pasien_lat,
            lokasi_pasien_lon
        ]);
        newCallId = result.insertId;
        console.log(`[T3] Panggilan darurat baru (ID: ${newCallId}) dari Pasien ${id_pasien} dicatat di DB.`);

        // Langkah 2: Temukan driver terbaik menggunakan Hybrid Model (Filter + Refine)
        console.log(`[T3] Memulai proses pencarian driver...`);
        const bestDriver = await _findBestDriver(patientLocation, newCallId);

        // Langkah 3: Tugaskan driver dan kirim notifikasi
        await _assignDriverToCall(bestDriver, newCallId, patientLocation, id_pasien);

    }catch (error){
        console.error(`[T3] Gagal memproses permintaan bantuan (ID Panggilan: ${newCallId}):`, error.message);
        // Jika panggilan sudah dibuat tapi gagal di tengah jalan, update statusnya
        if (newCallId) {
            await db.execute(`UPDATE panggilan_darurat SET status_panggilan = 'FAILED' WHERE id_panggilan = ?`, [newCallId]);
        }
    }
} //End function handlePatientRequest


/**
 * [ALUR 4 - T7] Menangani konfirmasi tugas dari driver.
 * Menerima T7 (diterima/selesai) dan memperbarui status di DB.
 * @param {object} data - Payload JSON dari T7 (id_panggilan, id_ambulans, status).
 * @returns {Promise<void>}
 */
async function handleDriverTaskConfirmation(data) {
    console.log(`[T7] Menerima konfirmasi tugas dari driver...`);
    const { id_panggilan, id_ambulans, status } = data;
    if (!id_panggilan || !id_ambulans || !status) {
         console.error("[7] Data konfirmasi tugas tidak lengkap. Payload:", data);
         return;
    }

    let newStatusPanggilan = '';
    if (status === 'diterima') {
        newStatusPanggilan = 'ON_THE_WAY';
    } else if (status === 'selesai') {
        newStatusPanggilan = 'COMPLETED';
    } else {
        console.warn(`[7] Status konfirmasi tidak dikenal: ${status}`);
        return;
    }

    try {
        // Update status di DB
        const [result] = await db.execute(
            `UPDATE panggilan_darurat SET status_panggilan = ? WHERE id_panggilan = ? AND id_ambulans_respons = ?`,
            [newStatusPanggilan, id_panggilan, id_ambulans]
        );

        if (result.affectedRows === 0) {
            console.warn(`[T7] Konfirmasi gagal: Panggilan ${id_panggilan} / Driver ${id_ambulans} tidak cocok.`);
            return;
        }

        console.log(`[T7] Status panggilan ${id_panggilan} diperbarui menjadi ${newStatusPanggilan}`);
        
        // Jika tugas selesai, set driver kembali 'ONLINE'
        if (newStatusPanggilan === 'COMPLETED') {
            await db.execute(
                `UPDATE ambulans SET status_operasional = 'ONLINE' WHERE id_ambulans = ?`,
                [id_ambulans]
            );
            console.log(`[T7] Driver ${id_ambulans} kembali ONLINE.`);
        }
        
        // TODO: Kirim pembaruan ke pasien melalui T8  (panggilan/status/{id_panggilan})

    } catch (error) {
        console.error(`Gagal memproses konfirmasi T7:`, error.sqlMessage || error.message);
    }
} //End function handleDriverTaskConfirmation


// --- FUNGSI HELPER ---

/**
 * [HELPER UNTUK T3] Menjalankan logika Hybrid Model (Filter + Refine).
 * @param {object} patientLocation - Objek { latitude, longitude } pasien.
 * @param {number} newCallId - ID panggilan darurat yang baru dibuat.
 * @returns {Promise<object>} Objek driver terbaik yang berisi { id, etaSeconds }.
 * @throws {Error} Jika tidak ada driver yang ditemukan atau API gagal.
 */
async function _findBestDriver(patientLocation, newCallId) {
    // 1. Ambil driver dari DB
    const [drivers] = await db.execute(
        `SELECT id_ambulans, lokasi_latitude, lokasi_longitude 
         FROM ambulans 
         WHERE status_operasional = 'ONLINE' AND lokasi_latitude IS NOT NULL`
    );

    if (drivers.length === 0) {
        await db.execute(`UPDATE panggilan_darurat SET status_panggilan = 'NO_DRIVERS' WHERE id_panggilan = ?`, [newCallId]);
        throw new Error("Tidak ada driver yang online.");
    }

    // 2. TAHAP 1: FILTER (Haversine & Euclidean)
    console.log(`--- [T3] TAHAP 1: FILTER (Haversine & Euclidean) ---`);
    const driversWithDistance = drivers.map(driver => {
        const driverLocation = { latitude: driver.lokasi_latitude, longitude: driver.lokasi_longitude };
        const distanceHaversine = getDistance(patientLocation, driverLocation);
        const distanceEuclidean = getEuclideanDistance(patientLocation, driverLocation);
        
        console.log(`  -> Driver ${driver.id_ambulans} | Haversine: ${distanceHaversine} m | Euclidean: ${distanceEuclidean.toFixed(5)} "unit"`);
        
        return { 
            id: driver.id_ambulans, 
            location: driverLocation,
            distanceHaversine: distanceHaversine
        };
    });

    driversWithDistance.sort((a, b) => a.distanceHaversine - b.distanceHaversine);
    const candidates = driversWithDistance.slice(0, 3); // Ambil 3 teratas
    
    // 3. TAHAP 2: REFINE (Google Maps API)
    console.log(`--- [T3] TAHAP 2: REFINE (Google Maps API) ---`);
    console.log(`Mengambil ${candidates.length} kandidat teratas untuk dicek ETA: ${candidates.map(c => c.id).join(', ')}`);

    const origins = [patientLocation];
    const destinations = candidates.map(c => c.location);

    const apiResponse = await gmapsClient.distancematrix({
        params: { key: GOOGLE_MAPS_API_KEY, origins: origins, destinations: destinations, travelMode: 'DRIVING' }
    });

    console.log(`--- [T3] HASIL RISET PERBANDINGAN LENGKAP ---`);
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
        await db.execute(`UPDATE panggilan_darurat SET status_panggilan = 'API_FAILED' WHERE id_panggilan = ?`, [newCallId]);
        throw new Error("Gagal mendapatkan hasil ETA dari Google.");
    }

    results.sort((a, b) => a.etaSeconds - b.etaSeconds);
    const bestDriverAPI = results[0];

    console.log(`--- [T3] KESIMPULAN AKHIR (Hybrid Model) ---`);
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
    // 1. Update DB: Tugaskan driver ke panggilan
    await db.execute(
        `UPDATE panggilan_darurat SET id_ambulans_respons = ?, status_panggilan = 'ASSIGNED' WHERE id_panggilan = ?`,
        [bestDriver.id, callId]
    );

    // 2. Update DB: Set status driver menjadi 'BUSY'
    await db.execute(
        `UPDATE ambulans SET status_operasional = 'BUSY' WHERE id_ambulans = ?`,
        [bestDriver.id]
    );
    console.log(`[T3] Driver ${bestDriver.id} ditugaskan untuk panggilan ${callId} di DB.`);

    // 3. Kirim T6: Notifikasi tugas ke driver terpilih
    const topicTugas = `ambulans/tugas/${bestDriver.id}`;
    const payloadTugas = {
        id_panggilan: callId,
        lokasi_pasien_lat: patientLocation.latitude,
        lokasi_pasien_lon: patientLocation.longitude,
    };
    client.publish(topicTugas, JSON.stringify(payloadTugas), { qos: 1 });
    console.log(`[T6] Tugas dikirim ke topik ${topicTugas}`);

    // 4. Kirim T8: Notifikasi status ke pasien
    const topicBalasan = `panggilan/status/${callId}`;
    const topicBalasanPasien = `panggilan/status/pasien/${id_pasien}`;
    const payloadBalasan = {
        status_panggilan: "menuju_lokasi",
        id_panggilan: callId,
        id_ambulans: bestDriver.id,
        eta_detik: bestDriver.etaSeconds
    };
    const payloadString = JSON.stringify(payloadBalasan);
    client.publish(topicBalasan, payloadString, { qos: 1 });
    client.publish(topicBalasanPasien, payloadString, { qos: 1 });
    console.log(`[T8] Balasan dikirim ke ${topicBalasan} dan ${topicBalasanPasien}`);
} //End _assignDriverToCall


/**
 * [HELPER] Menghitung Jarak Euclidean (Metode Riset)
 * @param {object} patientLoc - Objek { latitude, longitude }.
 * @param {object} driverLoc - Objek { latitude, longitude }.
 * @returns {number} Jarak dalam "unit" lintang/bujur.
 */
function getEuclideanDistance(patientLoc, driverLoc) {
    const latDiff = patientLoc.latitude - driverLoc.latitude;
    const lonDiff = patientLoc.longitude - driverLoc.longitude;
    return Math.sqrt(Math.pow(latDiff, 2) + Math.pow(lonDiff, 2));
}