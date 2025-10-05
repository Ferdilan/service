const mqtt = require('mqtt');

// --- PENGATURAN PENGUJIAN ---
const BROKER_URL = 'mqtt://hjppbzvg:hjppbzvg:zUg3ysf369ZnibIfjtSc7Qtj-ezmi5IB@mustang.rmq.cloudamqp.com';

// Ganti dengan Client ID spesifik dari aplikasi Android yang sedang berjalan
// Anda bisa mendapatkan ini dari Logcat saat aplikasi pertama kali terhubung
const TARGET_CLIENT_ID = 'android-client-763c5f96-2c94-4c15-a323-d14adcb3b4e8'; 

// Topik tujuan sesuai dengan yang di-subscribe oleh aplikasi Android
// const TARGET_TOPIC = `client/${TARGET_CLIENT_ID}/notification`;
const TARGET_TOPIC = 'pasien/registrasi/ocr';

const DUMMY_PATIENT_DATA = {
    nama_lengkap: "Budi Santoso Wibowo",
    nik: "3573010101900001", // Ganti NIK ini jika Anda menguji beberapa kali
    tanggal_lahir: "1990-01-01",
    alamat: "Jl. Ijen No. 25, Kota Malang"
};

const TEST_MESSAGE = {
    type: "ASSIGNMENT_CONFIRMATION",
    message: "Ambulans terdekat telah ditugaskan dan sedang menuju lokasi Anda.",
    driverId: "DRIVER_007",
    eta: "10 menit"
};
// -----------------------------

// Buat koneksi ke broker
const client = mqtt.connect(BROKER_URL);

// Event handler ketika koneksi berhasil
client.on('connect', () => {
    console.log('✅ Berhasil terhubung ke Broker MQTT untuk pengujian.');

    // Kirim (publish) pesan ke topik target
    client.publish(TARGET_TOPIC, JSON.stringify(DUMMY_PATIENT_DATA), { qos: 1 }, (error) => {
        if (error) {
            console.error('❌ Gagal mengirim pesan:', error);
        } else {
            console.log(`🚀 Pesan berhasil dikirim ke topik: ${TARGET_TOPIC}`);
            console.log(`Isi Pesan: ${JSON.stringify(DUMMY_PATIENT_DATA)}`);
        }

        // Putuskan koneksi setelah pesan terkirim
        client.end();
    });
});

// Event handler jika terjadi eror koneksi
client.on('error', (error) => {
    console.error('❌ Gagal terhubung:', error);
    client.end();
});