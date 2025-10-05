const mqtt = require('mqtt');
const mysql = require('mysql2/promise');

const db = require('./db.js');

// Konfigurasi koneksi ke broker RabbitMQ
const BROKER_URL = 'mqtt://hjppbzvg:hjppbzvg:zUg3ysf369ZnibIfjtSc7Qtj-ezmi5IB@mustang.rmq.cloudamqp.com';
const client = mqtt.connect(BROKER_URL);

// Topik yang akan didengarkan oleh service
// Penggunaan wildcard '+' berarti mendengarkan semua ID pasien atau driver
const TOPIC_PERMINTAAN_PASIEN = 'pasien/request';
const TOPIC_LOKASI_DRIVER = 'driver/lokasi';
const TOPIC_REGISTRASI_OCR = 'pasien/registrasi/ocr';

client.on('connect', () => {
    console.log('Service terhubung ke Broker MQTT.');

    // Mulai berlangganan (subscribe) ke topik yang relevan
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
    // const payload = message.toString();
    const payload = JSON.parse(message.toString());
    console.log(`Pesan diterima pada topik [${topic}]: ${payload}`);

    if (topic === 'pasien/registrasi/ocr') {
        console.log('📥 Menerima data registrasi pasien baru...');
        await handlePatientRegistration(payload);
    } 
    // Di sinilah logika bisnis Anda bersemayam
    // Contoh:
    if (topic.startsWith('pasien/permintaan/')) {
        // Proses permintaan dari pasien
        // 1. Parse data JSON dari payload
        // 2. Simpan permintaan ke database MySQL
        // 3. Cari driver terdekat
        // 4. Publikasikan tugas ke topik spesifik untuk driver tersebut
        //    client.publish('driver/tugas/driver_123', '{"tugas": "jemput pasien A"}');
    } else if (topic.startsWith('driver/lokasi/')) {
        // Proses pembaruan lokasi dari driver
        // 1. Parse data JSON (latitude, longitude)
        // 2. Update lokasi driver di database
        // 3. Mungkin teruskan lokasi ini ke pasien yang relevan
    }
});

// Buat fungsi baru untuk menangani penyimpanan data pasien
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
        console.log(`✅ Data pasien baru dengan NIK ${data.nik} berhasil disimpan. ID: ${result.insertId}`);
    } catch (error) {
        // Tangani kemungkinan NIK duplikat
        if (error.code === 'ER_DUP_ENTRY') {
            console.warn(`⚠️  Data pasien dengan NIK ${data.nik} sudah ada.`);
        } else {
            console.error('❌ Gagal menyimpan data pasien:', error);
        }
    }
}

client.on('error', (error) => {
    console.error('Koneksi MQTT Error:', error);
});