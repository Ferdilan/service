const mqtt = require('mqtt');
const mysql = require('mysql2/promise');
const db = require('./db.js');
const { getDistance } = require('geolib'); //library Haversine // (Perlu di-install: npm install geolib)
// const { Client } = require("@googlemaps/google-maps-services-js"); // (Perlu di-install)
// const gmapsClient = new Client({});

// Konfigurasi koneksi ke broker RabbitMQ
const BROKER_URL = 'mqtt://hjppbzvg:hjppbzvg:zUg3ysf369ZnibIfjtSc7Qtj-ezmi5IB@mustang.rmq.cloudamqp.com';
const client = mqtt.connect(BROKER_URL);

// Topik yang akan didengarkan oleh service
const TOPIC_REGISTRASI_PASIEN = 'pasien/registrasi/request';            // T1
const TOPIC_PERMINTAAN_DARURAT = 'panggilan/darurat/masuk';             // T3
const TOPIC_UPDATE_LOKASI_DRIVER = 'ambulans/lokasi/update/+';          // T5
const TOPIC_KONFIRMASI_TUGAS_DRIVER = 'ambulans/respons/konfirmasi';    // T7
// const TOPIC_REGISTRASI_OCR = 'pasien/registrasi/ocr';   

client.on('connect', () => {
    console.log('Service terhubung ke Broker MQTT.');

    // --- Subscribe ke semua topik yang relevan ---
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


client.on('message', async (topic, message) => {
    try{
            // 'message' adalah Buffer, ubah ke string
        const payload = JSON.parse(message.toString());
        console.log(`Pesan diterima pada topik [${topic}]: ${payload}`);

        // 1. Tangani Registrasi Pasien
        if (topic === TOPIC_REGISTRASI_PASIEN) { // T1 
                console.log('Menerima data registrasi pasien baru...');
                await handlePatientRegistration(payload);

        }
        // 2. Tangani permintaan ambulance dari pasien
        else if (topic === TOPIC_PERMINTAAN_DARURAT) { // T3
            console.log('Menerima permintaan ambulan pasien...');
            await handlePatientRequest(payload);
        }
        // 3. Tangani Lokasi Driver
        else if (topic.startsWith('ambulans/lokasi/update/')) { // Penyesuaian T5 
            const driverId = topic.split('/')[3]; // ambulans/lokasi/update/{id_ambulans}
            await handleDriverLocationUpdate(driverId, payload);

        }
        // 4. Tangani Konfirmasi driver (Terima/Tolak) panggilan darurat
        else if (topic === TOPIC_KONFIRMASI_TUGAS_DRIVER) { // T7 
            await handleDriverTaskConfirmation(payload);
        }
    }
    catch (e) {
        console.error(`Gagal memproses pesan di topik ${topic}:`, e.message);
    }
}); //End Client Message


/**
 * Menangani T1: pasien/registrasi/request
 * Menyimpan data ke tabel 'pasien'
 */
async function handlePatientRegistration(data) {
    const sql = `
        INSERT INTO pasien (nik, nama_lengkap, tgl_lahir, alamat, jenis_kelamin) 
        VALUES (?, ?, ?, ?, ?)
    `;
    try {
        const { nik, nama_lengkap, tgl_lahir, alamat, jenis_kelamin } = data;
        if (!nik || !nama_lengkap || !tgl_lahir || !alamat || !jenis_kelamin) {
            console.error("Data registrasi tidak lengkap. Payload:", data);
            return;
        }
        const [result] = await db.execute(sql, [nik, nama_lengkap, tgl_lahir, alamat, jenis_kelamin]);
        console.log(`Pasien baru berhasil terdaftar dengan ID: ${result.insertId}`);
    } catch (error) {
        if (error.code === 'ER_DUP_ENTRY') {
            console.warn(`Pasien dengan NIK ${data.nik} sudah ada.`);
        } else {
            console.error('Gagal menyimpan data pasien:', error);
        }
    }
}


/**
 * Menangani Penyesuaian T5: ambulans/lokasi/update/{id_ambulans}
 * Memperbarui tabel 'ambulans'
 */
async function handleDriverLocationUpdate(driverId, data) { // <-- DEFINISI FUNGSI
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
             console.error(`Data lokasi driver ${driverId} tidak lengkap. Payload:`, data);
             return;
        }
        const [result] = await db.execute(sql, [lokasi_latitude, lokasi_longitude, driverId]);
        if (result.affectedRows === 0) {
            console.warn(`Pembaruan lokasi gagal: Driver ID ${driverId} tidak ditemukan.`);
        } else {
            console.log(`Lokasi Driver ${driverId} diperbarui.`);
        }
    } catch (error) {
        console.error(`Gagal memperbarui lokasi driver ${driverId}:`, error);
    }
}


/**
 * Menangani T3: panggilan/darurat/masuk 
 * Inti Logika Hybrid Model (Filter & Refine)
 */
async function handlePatientRequest(data) {
    // Data payload dari T3: {id_pasien, lokasi_pasien_lat, lokasi_pasien_lon}
    const { id_pasien, lokasi_pasien_lat, lokasi_pasien_lon } = data;
    if (id_pasien === undefined || lokasi_pasien_lat === undefined || lokasi_pasien_lon === undefined) {
        console.error("Permintaan darurat tidak lengkap. Payload:", data);
        return;
    }
    const patientLocation = { latitude: lokasi_pasien_lat, longitude: lokasi_pasien_lon };
    let newCallId;
    try {
        // Langkah 1: Catat panggilan darurat ke DB
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
        console.log(`Panggilan darurat baru (ID: ${newCallId}) dari Pasien ${id_pasien} dicatat.`);

        // --- Mulai Logika Hybrid Model ---
        // Langkah 2 (Filter): Ambil semua driver yang 'ONLINE' dari tabel 'ambulans'
        const [drivers] = await db.execute(
            `SELECT id_ambulans, lokasi_latitude, lokasi_longitude 
             FROM ambulans 
             WHERE status_operasional = 'ONLINE' AND lokasi_latitude IS NOT NULL`
        );

        if (drivers.length === 0) {
            console.warn('Tidak ada driver yang online.');
            // TODO: Kirim notifikasi ke T8  bahwa tidak ada driver
            return;
        }

        // Langkah 3 (Filter menggunakan Haversine): Hitung jarak garis lurus
        const driversWithDistance = drivers.map(driver => {
            const driverLocation = { latitude: driver.lokasi_latitude, longitude: driver.lokasi_longitude };
            const distance = getDistance(patientLocation, driverLocation); // Jarak dalam meter
            return {
                id: driver.id_ambulans,
                distance: distance
            };
        });

        // Urutkan driver berdasarkan jarak terdekat
        driversWithDistance.sort((a, b) => a.distance - b.distance);

        // Ambil 5 kandidat teratas
        const candidates = driversWithDistance.slice(0, 5);
        console.log(`Kandidat teratas (Haversine): ${candidates.map(c => c.id).join(', ')}`);

        // Langkah 4 (Refine - API): Panggil Google Maps API untuk 5 kandidat
        // (Ini adalah PENGEMBANGAN LANJUTAN. Untuk saat ini, kita pilih yang terdekat)

        // --- Versi Sederhana: Pilih driver terdekat berdasarkan Haversine ---
        const bestDriver = candidates[0];

        console.log(`Driver terbaik (ID: ${bestDriver.id}) dipilih dengan jarak ${bestDriver.distance} meter.`);

        // Langkah 5 (Assign): Update DB & Kirim tugas ke Driver

        // Update tabel 'panggilan_darurat' dengan driver yang dipilih
        await db.execute(
            `UPDATE panggilan_darurat SET id_ambulans_respons = ?, status_panggilan = 'ASSIGNED' WHERE id_panggilan = ?`,
            [bestDriver.id, newCallId]
        );

        // Update status driver menjadi 'BUSY'
        await db.execute(
            `UPDATE ambulans SET status_operasional = 'BUSY' WHERE id_ambulans = ?`,
            [bestDriver.id]
        );

        // Kirim tugas ke driver terpilih (T6) 
        const topicTugas = `ambulans/tugas/${bestDriver.id}`;
        const payloadTugas = {
            id_panggilan: newCallId,
            lokasi_pasien_lat: lokasi_pasien_lat,
            lokasi_pasien_lon: lokasi_pasien_lon,
            // TODO: Tambahkan info pasien jika perlu
        };
        client.publish(topicTugas, JSON.stringify(payloadTugas), { qos: 1 });
        console.log(`Tugas dikirim ke topik ${topicTugas}`);

    } catch (error) {
        console.error('Gagal memproses permintaan bantuan:', error);
        // Jika panggilan sudah dibuat, update statusnya menjadi GAGAL
        if (newCallId) {
            await db.execute(`UPDATE panggilan_darurat SET status_panggilan = 'FAILED' WHERE id_panggilan = ?`, [newCallId]);
        }
    }
} // End handlePatientRequest function
  

/**
 * Menangani T7: ambulans/respons/konfirmasi 
 * Memperbarui status panggilan
 */
async function handleDriverTaskConfirmation(data) {
    // Payload T7: {id_panggilan, id_ambulans, status}
    const { id_panggilan, id_ambulans, status } = data;
    if (!id_panggilan || !id_ambulans || !status) {
         console.error("Data konfirmasi tugas tidak lengkap. Payload:", data);
         return;
    }

    let newStatusPanggilan = '';

    if (status === 'diterima') {
        newStatusPanggilan = 'ON_THE_WAY'; // Status kustom untuk 'menuju_lokasi'
    } else if (status === 'selesai') {
        newStatusPanggilan = 'COMPLETED';
    } else {
        console.warn(`Status konfirmasi tidak dikenal: ${status}`);
        return;
    }

    try {
        // Update status di tabel 'panggilan_darurat'
        const [result] = await db.execute(
            `UPDATE panggilan_darurat SET status_panggilan = ? WHERE id_panggilan = ? AND id_ambulans_respons = ?`,
            [newStatusPanggilan, id_panggilan, id_ambulans]
        );

        if (result.affectedRows > 0) {
            console.log(`Status panggilan ${id_panggilan} diperbarui menjadi ${newStatusPanggilan}`);
        } else {
            console.warn(`Konfirmasi T7  gagal: Panggilan ${id_panggilan} / Driver ${id_ambulans} tidak cocok.`);
        }
        
        // Jika tugas selesai, set driver kembali 'ONLINE'
        if (newStatusPanggilan === 'COMPLETED') {
            await db.execute(
                `UPDATE ambulans SET status_operasional = 'ONLINE' WHERE id_ambulans = ?`,
                [id_ambulans]
            );
            console.log(`Driver ${id_ambulans} kembali ONLINE.`);
        }
        
        // TODO: Kirim pembaruan ke pasien melalui T8  (panggilan/status/{id_panggilan})

    } catch (error) {
        console.error(`Gagal memproses konfirmasi T7:`, error);
    }
}


client.on('error', (error) => {
    console.error('Koneksi MQTT Error:', error);
});