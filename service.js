const mqtt = require('mqtt');
const mysql = require('mysql2/promise');

const db = require('./db.js');

// Konfigurasi koneksi ke broker RabbitMQ
const BROKER_URL = 'mqtt://hjppbzvg:hjppbzvg:zUg3ysf369ZnibIfjtSc7Qtj-ezmi5IB@mustang.rmq.cloudamqp.com';
const client = mqtt.connect(BROKER_URL);

// Topik yang akan didengarkan oleh service
const TOPIC_PERMINTAAN_PASIEN = 'pasien/request';
const TOPIC_LOKASI_DRIVER = 'driver/lokasi';
const TOPIC_REGISTRASI_OCR = 'pasien/registrasi/ocr';

client.on('connect', () => {
    console.log('Service terhubung ke Broker MQTT.');

    // Mulai berlangganan (subscribe) ke topik permintaan pasien
    client.subscribe(TOPIC_PERMINTAAN_PASIEN, (err) => {
        if (!err) {
            console.log(`Berhasil subscribe ke topik: ${TOPIC_PERMINTAAN_PASIEN}`);
        }
    });

    client.subscribe(TOPIC_LOKASI_DRIVER, (err) => {
        if (!err) {
            console.log(`Berhasil subscribe ke topik: ${TOPIC_LOKASI_DRIVER}`);
        }
    });

    client.subscribe(TOPIC_REGISTRASI_OCR, (err) => {
        if (!err) {
            console.log('Berlangganan ke topik registrasi OCR.');
        }
    });
});

client.on('message', async (topic, message) => {
    // 'message' adalah Buffer, ubah ke string
    const payload = JSON.parse(message.toString());
    console.log(`Pesan diterima pada topik [${topic}]: ${payload}`);

    // 1. Tangani Registrasi Pasien
    if (topic === 'pasien/registrasi/ocr') {
        console.log('Menerima data registrasi pasien baru...');
        await handlePatientRegistration(payload);
    } 
    // 2. Tangani permintaan ambulance dari pasien
    else if (topic === TOPIC_PERMINTAAN_PASIEN) {
        console.log('Menerima permintaan ambulan pasien...');
        await handlePatientRequest(payload);
    }
    // 3. Tangani Lokasi Driver
    else if (topic === TOPIC_LOKASI_DRIVER) {
        console.log('Menerima pembaruan lokasi driver...');
        // Di sini Anda akan memanggil fungsi, misal: await handleDriverLocationUpdate(payload);
    }
});


// --- FUNGSI UNTUK MENANGANI KOORDINAT ---
async function handlePatientRequest(data) {
    // data akan berisi: {"patientId":"...", "name":"...", "location":{"latitude":-6.2088, "longitude":106.8456}}
    console.log(`Permintaan dari Patient ID: ${data.patientId}`);
    console.log(`Lokasi: Lat ${data.location.latitude}, Lon ${data.location.longitude}`);

    // --- Langkah Selanjutnya: Simpan ke Database ---
    // Anda perlu membuat tabel baru, misalnya 'permintaan_bantuan'
    
    try {
        const sql = `
            INSERT INTO permintaan_bantuan 
            (patient_client_id, nama_pasien, latitude, longitude, status) 
            VALUES (?, ?, ?, ?, ?)
        `;
        const [result] = await db.execute(sql, [
            data.patientId,
            data.name,
            data.location.latitude,
            data.location.longitude,
            'PENDING' // Status awal
        ]);
        console.log(`Permintaan bantuan baru berhasil disimpan. ID Permintaan: ${result.insertId}`);

        // TODO:
        // 1. Kirim notifikasi balik ke pasien bahwa permintaan diterima
        //    const responseTopic = `client/${data.patientId}/notification`;
        //    const responseMessage = "Permintaan Anda telah diterima, kami sedang mencari ambulans terdekat.";
        //    client.publish(responseTopic, responseMessage);

        // 2. Jalankan logika untuk mencari driver terdekat (ini akan kompleks)

    } catch (error) {
        console.error('Gagal menyimpan permintaan bantuan:', error);
    }
}
  

// Fungsi untuk menangani penyimpanan data pasien
async function handlePatientRegistration(data) {
    try {
        const sql = 'INSERT INTO pasien (nama_lengkap, nik, tanggal_lahir, alamat) VALUES (?, ?, ?, ?)';
        // Perhatikan: urutan data harus sesuai dengan urutan '?'
        const [result] = await db.execute(sql, [
            data.nama_lengkap, 
            data.nik,
            data.tanggal_lahir,
            data.alamat, 
             // pastikan formatnya 'YYYY-MM-DD'
        ]);
        console.log(`Data pasien baru dengan NIK ${data.nik} berhasil disimpan. ID: ${result.insertId}`);
    } catch (error) {
        // Tangani kemungkinan NIK duplikat
        if (error.code === 'ER_DUP_ENTRY') {
            console.warn(`Data pasien dengan NIK ${data.nik} sudah ada.`);
        } else {
            console.error('Gagal menyimpan data pasien:', error);
        }
    }
}

client.on('error', (error) => {
    console.error('Koneksi MQTT Error:', error);
});