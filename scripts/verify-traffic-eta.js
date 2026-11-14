/**
 * SKRIP VERIFIKASI MANUAL: Validasi Ketersediaan Traffic-Aware ETA (Google Maps Platform)
 * ==============================================================================
 * CATATAN PENTING:
 * Skrip ini HANYA untuk dijalankan secara manual oleh peneliti ketika ingin
 * menguji apakah API Key Google Maps yang digunakan telah mengaktifkan fitur
 * live traffic dan mengembalikan field `duration_in_traffic`.
 * 
 * Penggunaan:
 *   node scripts/verify-traffic-eta.js
 * 
 * Prasyarat:
 *   - File .env memuat GOOGLE_MAPS_API_KEY yang valid dengan billing aktif.
 * ==============================================================================
 */

require('dotenv').config();
const { Client } = require('@googlemaps/google-maps-services-js');

// Dua koordinat uji tetap di Kota Malang:
// Origin: Alun-Alun Tugu Malang
const ORIGIN = { latitude: -7.978645, longitude: 112.631786 };
// Destination: RSUD Dr. Saiful Anwar Malang
const DESTINATION = { latitude: -7.965801, longitude: 112.624512 };

async function verifyTrafficEta() {
    console.log('================================================================');
    console.log(' VERIFIKASI GOOGLE DISTANCE MATRIX - TRAFFIC-AWARE ETA');
    console.log('================================================================\n');

    const apiKey = process.env.GOOGLE_MAPS_API_KEY;
    if (!apiKey) {
        console.error('❌ ERROR: Variabel GOOGLE_MAPS_API_KEY tidak ditemukan di environment (.env).');
        process.exit(1);
    }

    console.log(`- Origin (Pasien)     : ${ORIGIN.latitude}, ${ORIGIN.longitude} (Alun-Alun Tugu Malang)`);
    console.log(`- Destination (Armada): ${DESTINATION.latitude}, ${DESTINATION.longitude} (RSUD Dr. Saiful Anwar)`);
    console.log(`- Mode                : driving`);
    console.log(`- Departure Time      : now`);
    console.log(`- API Key             : [TERSEDIA - ${apiKey.substring(0, 4)}...${apiKey.substring(apiKey.length - 4)}]\n`);

    const gmapsClient = new Client({});

    try {
        console.log('Mengirim 1 request batch ke Google Distance Matrix API...');
        const response = await gmapsClient.distancematrix({
            params: {
                key: apiKey,
                origins: [ORIGIN],
                destinations: [DESTINATION],
                mode: 'driving',
                departure_time: 'now'
            }
        });

        console.log(`Status HTTP: ${response.status}`);
        const data = response.data;

        if (data.status !== 'OK') {
            console.error(`❌ Respon API gagal dengan status: ${data.status}`);
            if (data.error_message) {
                console.error(`   Pesan error: ${data.error_message}`);
            }
            return;
        }

        const element = data.rows[0].elements[0];
        console.log(`Status Elemen Rute: ${element.status}\n`);

        if (element.status !== 'OK') {
            console.error(`❌ Elemen rute tidak OK (status: ${element.status})`);
            return;
        }

        console.log('----------------------------------------------------------------');
        console.log('HASIL PENGUKURAN:');
        console.log(`- Jarak Jaringan Jalan (distance)          : ${element.distance.text} (${element.distance.value} meter)`);
        console.log(`- Durasi Statis Tanpa Macet (duration)     : ${element.duration.text} (${element.duration.value} detik)`);

        if (element.duration_in_traffic) {
            console.log(`- Durasi Kondisi Lalu Lintas (duration_in_traffic): ${element.duration_in_traffic.text} (${element.duration_in_traffic.value} detik)`);
            const selisihDetik = element.duration_in_traffic.value - element.duration.value;
            console.log(`- Efek Lalu Lintas Real-time               : ${selisihDetik >= 0 ? `+${selisihDetik}` : selisihDetik} detik dibanding durasi statis`);
            console.log('\n✅ KESIMPULAN: Akun Google Maps API Anda MENDUKUNG traffic-aware ETA.');
            console.log('   Field `duration_in_traffic` berhasil diperoleh dan dapat digunakan dalam dispatch.');
        } else {
            console.log(`- Durasi Kondisi Lalu Lintas (duration_in_traffic): TIDAK TERSEDIA (null/undefined)`);
            console.log('\n⚠️  KESIMPULAN: Fitur traffic-aware ETA TIDAK dikembalikan oleh Google API.');
            console.log('   Kemungkinan penyebab:');
            console.log('   1. Jenis penagihan atau kuota API Key tidak mengizinkan Advanced/Traffic Matrix.');
            console.log('   2. Data lalu lintas real-time pada ruas jalan tersebut sedang tidak tersedia.');
            console.log('   3. Sistem di backend akan otomatis fallback ke field `duration` statis.');
        }
        console.log('----------------------------------------------------------------\n');

    } catch (err) {
        console.error('❌ Terjadi kesalahan saat memanggil Distance Matrix API:');
        if (err.response && err.response.data) {
            console.error(JSON.stringify(err.response.data, null, 2));
        } else {
            console.error(err.message);
        }
    }
}

// Hanya jalankan jika dieksekusi langsung
if (require.main === module) {
    verifyTrafficEta();
}

module.exports = { verifyTrafficEta };
