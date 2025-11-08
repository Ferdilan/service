const mqtt = require('mqtt');
const { getDistance } = require('geolib'); // Pastikan sudah di-install: npm install geolib
const { Client } = require("@googlemaps/google-maps-services-js"); // Pastikan sudah di-install

// --- 1. PENGATURAN API DAN KONEKSI ---
const BROKER_URL = 'mqtt://hjppbzvg:hjppbzvg:zUg3ysf369ZnibIfjtSc7Qtj-ezmi5IB@mustang.rmq.cloudamqp.com';

// !!! GANTI "YOUR_API_KEY_HERE" DENGAN API KEY ANDA !!!
const GOOGLE_MAPS_API_KEY = "AIzaSyCNoOEGNx1eycV5Pc3SvY6a3BrG_fRqvbg"; 

// Inisialisasi Klien
const client = mqtt.connect(BROKER_URL);
const gmapsClient = new Client({});

// --- 2. "DATABASE" IN-MEMORY ---
let driverLocations = {};
console.log("Server Simulasi (In-Memory) dimulai...");
console.log("Menunggu update lokasi driver dan panggilan pasien...");

// --- 3. TOPIK MQTT (Sesuai Rancangan Anda) ---
const TOPIC_PERMINTAAN_DARURAT = 'panggilan/darurat/masuk';   // T3
const TOPIC_UPDATE_LOKASI_DRIVER = 'ambulans/lokasi/update/+'; // Penyesuaian T5

client.on('connect', () => {
    console.log('Service terhubung ke Broker MQTT.');
    
    // Subscribe ke topik yang relevan untuk simulasi
    client.subscribe(TOPIC_PERMINTAAN_DARURAT, (err) => {
        if (!err) console.log(`Berhasil subscribe ke: ${TOPIC_PERMINTAAN_DARURAT}`);
    });
    client.subscribe(TOPIC_UPDATE_LOKASI_DRIVER, (err) => {
        if (!err) console.log(`Berhasil subscribe ke: ${TOPIC_UPDATE_LOKASI_DRIVER}`);
    });
});

client.on('message', async (topic, message) => {
    let payload;
    try {
        payload = JSON.parse(message.toString());
        console.log(`\n======================================================`);
        console.log(`Pesan diterima pada topik [${topic}]: ${message.toString()}`);

        // --- 4. LOGIKA ROUTING (Tanpa Database) ---
        
        if (topic.startsWith('ambulans/lokasi/update/')) {
            // T5 DITERIMA: Driver mengirim lokasi
            const driverId = topic.split('/')[3];
            handleDriverLocationUpdate(driverId, payload);
        
        } else if (topic === TOPIC_PERMINTAAN_DARURAT) {
            // T3 DITERIMA: Pasien meminta bantuan
            await handlePatientRequest(payload); // Pastikan 'await' ada di sini
        }

    } catch (e) {
        console.error(`Gagal memproses pesan di topik ${topic}:`, e);
    }
});

/**
 * Menangani T5 (Penyesuaian): Menyimpan lokasi driver ke variabel 'driverLocations'
 */
function handleDriverLocationUpdate(driverId, data) {
    const { lokasi_latitude, lokasi_longitude } = data;
    
    if (lokasi_latitude === undefined || lokasi_longitude === undefined) {
         console.error(`❌ Data lokasi driver ${driverId} tidak lengkap.`);
         return;
    }

    // Simpan ke "database" in-memory kita
    driverLocations[driverId] = {
        latitude: lokasi_latitude,
        longitude: lokasi_longitude,
        last_update: new Date().toISOString()
    };
    
    console.log(`[SIMULASI] Lokasi Driver ${driverId} disimpan di memori.`);
    console.log(`Total driver online yang terlacak: ${Object.keys(driverLocations).length}`);
}

/**
 * Menghitung Jarak Euclidean (Metode Riset 2 - Tidak Akurat)
 */
function getEuclideanDistance(patientLoc, driverLoc) {
    const latDiff = patientLoc.latitude - driverLoc.latitude;
    const lonDiff = patientLoc.longitude - driverLoc.longitude;
    return Math.sqrt(Math.pow(latDiff, 2) + Math.pow(lonDiff, 2));
}

/**
 * Menangani T3: Melakukan PERHITUNGAN JARAK (Inti Riset)
 */
async function handlePatientRequest(data) { // Pastikan 'async' ada
    const { id_pasien, lokasi_pasien_lat, lokasi_pasien_lon } = data;
    
    if (id_pasien === undefined || lokasi_pasien_lat === undefined || lokasi_pasien_lon === undefined) {
        console.error("❌ Permintaan darurat tidak lengkap.");
        return;
    }

    console.log(`[SIMULASI] Memulai perhitungan jarak untuk Pasien ${id_pasien}...`);
    const patientLocation = { latitude: lokasi_pasien_lat, longitude: lokasi_pasien_lon };

    const availableDrivers = Object.keys(driverLocations);

    if (availableDrivers.length === 0) {
        console.warn('⚠️ SIMULASI GAGAL: Tidak ada data driver di memori.');
        console.log('Harap jalankan "node test_publisher.js lokasi" terlebih dahulu.');
        return;
    }

    // --- TAHAP 1: FILTER (Haversine & Euclidean) ---
    console.log(`--- TAHAP 1: FILTER (Haversine & Euclidean) ---`);
    console.log(`Lokasi Pasien: ${patientLocation.latitude}, ${patientLocation.longitude}`);
    
    const driversWithDistance = availableDrivers.map(driverId => {
        const driverLocation = driverLocations[driverId];
        const distanceHaversine = getDistance(patientLocation, driverLocation);
        const distanceEuclidean = getEuclideanDistance(patientLocation, driverLocation);
        
        console.log(`  -> Driver ${driverId} | Haversine: ${distanceHaversine} meter | Euclidean: ${distanceEuclidean.toFixed(5)} "unit"`);
        
        return { 
            id: driverId, 
            location: driverLocation, // Kita butuh ini untuk API
            distanceHaversine: distanceHaversine
        };
    });

    // Urutkan berdasarkan Haversine
    driversWithDistance.sort((a, b) => a.distanceHaversine - b.distanceHaversine);
    
    // ====================================================================
    // INILAH BARIS YANG MENYEBABKAN ERROR JIKA HILANG
    // Ambil 3 kandidat teratas (atau kurang jika driver lebih sedikit)
    const candidates = driversWithDistance.slice(0, 3);
    // ====================================================================
    
    console.log(`\n--- TAHAP 2: REFINE (Google Maps API) ---`);
    
    // Baris ini (sebelumnya baris 145) sekarang aman karena 'candidates' ada
    console.log(`Mengambil ${candidates.length} kandidat teratas untuk dicek ETA: ${candidates.map(c => c.id).join(', ')}`);

    // Siapkan format untuk Google API
    const origins = [patientLocation];
    const destinations = candidates.map(c => c.location); // Ini juga aman

    try {
        // --- PANGGILAN API GOOGLE (Metode 3) ---
        const apiResponse = await gmapsClient.distancematrix({
            params: {
                key: GOOGLE_MAPS_API_KEY,
                origins: origins,
                destinations: destinations,
                travelMode: 'DRIVING'
            }
        });

        console.log(`\n--- HASIL RISET PERBANDINGAN LENGKAP ---`);
        
        const results = [];
        apiResponse.data.rows[0].elements.forEach((element, index) => {
            const candidate = candidates[index]; // Ambil kandidat yang sesuai
            if (element.status === 'OK') {
                const api_distance = element.distance.text; // "10.5 km"
                const api_duration_text = element.duration.text; // "15 menit"
                const api_duration_seconds = element.duration.value; // 900 (detik)
                
                console.log(`  -> Driver ${candidate.id} (Haversine: ${candidate.distanceHaversine} m)`);
                console.log(`     Google API Jarak: ${api_distance}`);
                console.log(`     Google API Waktu (ETA): ${api_duration_text}`);
                
                results.push({
                    id: candidate.id,
                    etaSeconds: api_duration_seconds
                });
            } else {
                console.log(`  -> Driver ${candidate.id}: Google API Gagal (Status: ${element.status})`);
            }
        });

        if (results.length === 0) {
            console.error("❌ Gagal mendapatkan hasil ETA dari Google. Cek API Key Anda.");
            return;
        }

        // Urutkan berdasarkan ETA terendah
        results.sort((a, b) => a.etaSeconds - b.etaSeconds);
        const bestDriverAPI = results[0];

        console.log(`\n--- KESIMPULAN AKHIR (Hybrid Model) ---`);
        console.log(`Driver terdekat (Haversine): ${driversWithDistance[0].id}`);
        console.log(`Driver tercepat (ETA Google API): ${bestDriverAPI.id}`);

    } catch (e) {
        // Tangani error API Key atau error jaringan
        console.error(`❌ Gagal memanggil Google Maps API:`, e.response ? e.response.data : e.message);
        if (e.response && e.response.data.error_message) {
            console.error(`   Pesan Error Google: ${e.response.data.error_message}`);
            console.error(`   ==> Pastikan API Key Anda benar dan billing telah diaktifkan.`);
        }
    }
}

client.on('error', (error) => {
    console.error('Koneksi MQTT Error:', error);
});