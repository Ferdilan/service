const mqtt = require('mqtt');

// --- PENGATURAN KONEKSI ---
const BROKER_URL = 'mqtt://hjppbzvg:hjppbzvg:zUg3ysf369ZnibIfjtSc7Qtj-ezmi5IB@mustang.rmq.cloudamqp.com';
const client = mqtt.connect(BROKER_URL);

// --- DAPATKAN SKENARIO TES DARI COMMAND LINE ---
// Cara penggunaan:
// node test_publisher.js lokasi   -> (Mensimulasikan Driver Update Lokasi - T5)
// node test_publisher.js panggil  -> (Mensimulasikan Pasien Minta Bantuan - T3)
// node test_publisher.js daftar   -> (Mensimulasikan Pasien Baru Registrasi - T1)
// node test_publisher.js terima   -> (Mensimulasikan Driver Menerima Tugas - T7)
// node test_publisher.js selesai  -> (Mensimulasikan Driver Selesai Tugas - T7)

const testCase = process.argv[2]; // Ambil argumen ke-3 (setelah 'node' dan 'test_publisher.js')

let topic;
let payload;
let testDescription;

// --- DEFINISI SEMUA SKENARIO TES ---
switch (testCase) {
    /**
     * TES 1: (Penyesuaian T5) Driver secara proaktif mengirim update lokasi.
     * Server (service.js) akan subscribe ke 'ambulans/lokasi/update/+'
     * dan memanggil handleDriverLocationUpdate().
     * 78: -7.944628365626527, 112.61929899999713
     * 79: -7.949573357081375, 112.6152530498783
     * 80: -7.9418384221998455, 112.61571667358028
     */
    case 'lokasi':
        testDescription = "Tes T5 (Update Lokasi Driver)";
        topic = 'ambulans/lokasi/update/80'; // {id_ambulans} = 78 (pastikan ada di DB)
        payload = JSON.stringify({
            "lokasi_latitude": -7.9418384221998455, // Ubah sedikit nilainya setiap tes
            "lokasi_longitude":  112.61571667358028
        });
        break;
 
    /**
     * TES 2: (T3) Pasien mengirim panggilan darurat.
     * Server (service.js) akan subscribe ke 'panggilan/darurat/masuk'
     * dan memanggil handlePatientRequest().
     */
    case 'panggil':
        testDescription = "Tes T3 (Panggilan Darurat Masuk)";
        topic = 'panggilan/darurat/masuk';
        payload = JSON.stringify({
            "id_pasien": 123, // Pastikan id_pasien=123 ada di tabel 'pasien'
            "lokasi_pasien_lat": -7.9446742,
            "lokasi_pasien_lon": 112.6163573
        });
        break;
    
    /**
     * TES 3: (T1) Pasien baru mendaftar.
     * Server (service.js) akan subscribe ke 'pasien/registrasi/request'
     * dan memanggil handlePatientRegistration().
     */
    case 'daftar':
        testDescription = "Tes T1 (Registrasi Pasien Baru)";
        topic = 'pasien/registrasi/request';
        payload = JSON.stringify({
            "nik": `357001010190${Math.floor(1000 + Math.random() * 9000)}`, // NIK acak agar unik
            "nama_lengkap": "Budi Santoso",
            "tgl_lahir": "1990-01-01",
            "jenis_kelamin": "L", // Sesuaikan jika rancangan Anda berbeda
            "alamat": "Jl. Ijen No. 25, Malang"
        });
        break;
    /**
     * TES 4: (T7) Driver menerima tugas yang diberikan.
     * Server (service.js) akan subscribe ke 'ambulans/respons/konfirmasi'
     * dan memanggil handleDriverTaskConfirmation().
     */
    case 'terima':
        testDescription = "Tes T7 (Driver Menerima Tugas)";
        topic = 'ambulans/respons/konfirmasi';
        payload = JSON.stringify({
            "id_panggilan": 1, // Ganti '1' dengan id_panggilan yang dibuat dari tes 'panggil'
            "id_ambulans": 78,
            "status": "diterima"
        });
        break;

    /**
     * TES 5: (T7) Driver menyelesaikan tugas.
     * Server (service.js) akan subscribe ke 'ambulans/respons/konfirmasi'
     * dan memanggil handleDriverTaskConfirmation().
     */
    case 'selesai':
        testDescription = "Tes T7 (Driver Selesai Tugas)";
        topic = 'ambulans/respons/konfirmasi';
        payload = JSON.stringify({
            "id_panggilan": 1, // Ganti '1' dengan id_panggilan yang sedang berjalan
            "id_ambulans": 78,
            "status": "selesai"
        });
        break;

    default:
        console.error("Argumen tes tidak valid.");
        console.log("Gunakan salah satu dari:");
        console.log("  node test_publisher.js lokasi");
        console.log("  node test_publisher.js panggil");
        console.log("  node test_publisher.js daftar");
        console.log("  node test_publisher.js terima");
        console.log("  node test_publisher.js selesai");
        process.exit(1); // Keluar dari skrip
}

// --- LOGIKA KONEKSI DAN PUBLISH ---
client.on('connect', () => {
    console.log('Berhasil terhubung ke Broker MQTT untuk pengujian.');
    console.log(`Menjalankan tes: [${testDescription}]`);

    if (!topic || !payload) {
        console.error("Kesalahan: Topik atau Payload tidak terdefinisi.");
        client.end();
        return;
    }

    // Kirim (publish) pesan ke topik target
    client.publish(topic, payload, { qos: 1 }, (error) => {
        if (error) {
            console.error('Gagal mengirim pesan:', error);
        } else {
            console.log(`Pesan berhasil dikirim ke topik: ${topic}`);
            console.log(`Isi Pesan: ${JSON.stringify(payload)}`);
        }

        // Putuskan koneksi setelah pesan terkirim
        client.end();
    });
});

// Event handler jika terjadi eror koneksi
client.on('error', (error) => {
    console.error('Gagal terhubung:', error);
    client.end();
});