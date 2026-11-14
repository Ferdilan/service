/**
 * Unit Test Mock untuk Logika Traffic-Aware ETA & Google Distance Matrix Processing
 * (Tanpa jaringan, tanpa database, tanpa broker MQTT riil)
 */

const assert = require('assert');
const {
    _buildDistanceMatrixParams,
    _processDistanceMatrixElements
} = require('../service');

function runTrafficEtaTests() {
    console.log('================================================================');
    console.log(' UNIT TEST MOCK: TRAFFIC-AWARE ETA IMPLEMENTATION (N2)');
    console.log('================================================================\n');

    const dummyPatientLocation = { latitude: -7.945, longitude: 112.615 };
    const dummyCandidates = [
        { id: 1, nama: 'Ambulans 1', location: { latitude: -7.950, longitude: 112.620 }, distanceHaversine: 800 },
        { id: 2, nama: 'Ambulans 2', location: { latitude: -7.960, longitude: 112.630 }, distanceHaversine: 1200 },
        { id: 3, nama: 'Ambulans 3', location: { latitude: -7.970, longitude: 112.640 }, distanceHaversine: 1800 }
    ];

    // -------------------------------------------------------------------------
    // TEST 1: Request memuat departure_time dan mode, serta TIDAK memuat travelMode
    // -------------------------------------------------------------------------
    console.log('Test 1: Memeriksa parameter Distance Matrix request...');
    const params = _buildDistanceMatrixParams(dummyPatientLocation, dummyCandidates, 'TEST_KEY');
    assert.strictEqual(params.departure_time, 'now', "departure_time harus bernilai 'now'");
    assert.strictEqual(params.mode, 'driving', "mode harus bernilai 'driving'");
    assert.strictEqual(params.travelMode, undefined, "travelMode TIDAK boleh ada");
    assert.strictEqual(params.key, 'TEST_KEY', "API key harus diteruskan");
    assert.strictEqual(params.origins.length, 1, "origins harus berupa array 1 elemen");
    assert.strictEqual(params.destinations.length, 3, "destinations harus memuat 3 elemen");
    console.log('✓ Test 1 Lolos: Request memuat departure_time: "now" dan mode: "driving", travelMode dieliminasi.\n');

    // -------------------------------------------------------------------------
    // TEST 2: Respons dengan duration_in_traffic ada -> dipakai dan etaSource = 'duration_in_traffic'
    // -------------------------------------------------------------------------
    console.log('Test 2: duration_in_traffic tersedia...');
    const mockApiResponseTraffic = {
        data: {
            rows: [{
                elements: [
                    {
                        status: 'OK',
                        distance: { text: '1.2 km', value: 1200 },
                        duration: { text: '5 menit', value: 300 },
                        duration_in_traffic: { text: '8 menit', value: 480 }
                    }
                ]
            }]
        }
    };
    const mockLogger2 = { info: () => {}, warn: () => {} };
    const res2 = _processDistanceMatrixElements([dummyCandidates[0]], mockApiResponseTraffic, 901, mockLogger2);
    assert.strictEqual(res2.results.length, 1);
    assert.strictEqual(res2.results[0].etaSource, 'duration_in_traffic', "etaSource harus 'duration_in_traffic'");
    assert.strictEqual(res2.results[0].etaSeconds, 480, "etaSeconds harus mengambil duration_in_traffic (480s)");
    assert.strictEqual(res2.results[0].durationSeconds, 300, "durationSeconds statis harus 300s");
    assert.strictEqual(res2.results[0].durationInTrafficSeconds, 480, "durationInTrafficSeconds harus 480s");
    assert.strictEqual(res2.allFallback, false, "allFallback harus false jika traffic tersedia");
    console.log('✓ Test 2 Lolos: duration_in_traffic (480s) berhasil dipakai sebagai ETA.\n');

    // -------------------------------------------------------------------------
    // TEST 3: Respons tanpa duration_in_traffic -> fallback ke duration dan etaSource = 'duration'
    // -------------------------------------------------------------------------
    console.log('Test 3: Respons tanpa duration_in_traffic (fallback)...');
    const mockApiResponseNoTraffic = {
        data: {
            rows: [{
                elements: [
                    {
                        status: 'OK',
                        distance: { text: '1.5 km', value: 1500 },
                        duration: { text: '6 menit', value: 360 }
                        // duration_in_traffic absen
                    }
                ]
            }]
        }
    };
    const mockLogger3 = { info: () => {}, warn: () => {} };
    const res3 = _processDistanceMatrixElements([dummyCandidates[0]], mockApiResponseNoTraffic, 902, mockLogger3);
    assert.strictEqual(res3.results.length, 1);
    assert.strictEqual(res3.results[0].etaSource, 'duration', "etaSource harus 'duration'");
    assert.strictEqual(res3.results[0].etaSeconds, 360, "etaSeconds harus mengambil duration (360s)");
    assert.strictEqual(res3.results[0].durationInTrafficSeconds, null, "durationInTrafficSeconds harus null");
    console.log('✓ Test 3 Lolos: Fallback ke duration statis (360s) dengan etaSource="duration".\n');

    // -------------------------------------------------------------------------
    // TEST 4: Campuran elemen (sebagian ada traffic, sebagian tidak)
    // -------------------------------------------------------------------------
    console.log('Test 4: Campuran elemen...');
    const mockApiResponseMixed = {
        data: {
            rows: [{
                elements: [
                    {
                        status: 'OK',
                        distance: { text: '1.0 km', value: 1000 },
                        duration: { text: '4 menit', value: 240 },
                        duration_in_traffic: { text: '7 menit', value: 420 }
                    },
                    {
                        status: 'OK',
                        distance: { text: '2.0 km', value: 2000 },
                        duration: { text: '8 menit', value: 480 }
                        // element kedua tanpa traffic
                    }
                ]
            }]
        }
    };
    const mockLogger4 = { info: () => {}, warn: () => {} };
    const res4 = _processDistanceMatrixElements([dummyCandidates[0], dummyCandidates[1]], mockApiResponseMixed, 903, mockLogger4);
    assert.strictEqual(res4.results.length, 2);
    assert.strictEqual(res4.results[0].etaSource, 'duration_in_traffic');
    assert.strictEqual(res4.results[0].etaSeconds, 420);
    assert.strictEqual(res4.results[1].etaSource, 'duration');
    assert.strictEqual(res4.results[1].etaSeconds, 480);
    assert.strictEqual(res4.allFallback, false, "allFallback harus false jika ada minimal 1 elemen dengan traffic");
    console.log('✓ Test 4 Lolos: Elemen campuran diproses tepat sesuai sumber masing-masing.\n');

    // -------------------------------------------------------------------------
    // TEST 5: Semua elemen tanpa duration_in_traffic -> WARNING tercatat
    // -------------------------------------------------------------------------
    console.log('Test 5: Peringatan jika seluruh kandidat fallback...');
    let loggedWarning = null;
    const mockLogger5 = {
        info: () => {},
        warn: (msg) => { loggedWarning = msg; }
    };
    const mockApiResponseAllFallback = {
        data: {
            rows: [{
                elements: [
                    { status: 'OK', distance: { text: '1 km', value: 1000 }, duration: { text: '5 m', value: 300 } },
                    { status: 'OK', distance: { text: '2 km', value: 2000 }, duration: { text: '7 m', value: 420 } }
                ]
            }]
        }
    };
    const res5 = _processDistanceMatrixElements([dummyCandidates[0], dummyCandidates[1]], mockApiResponseAllFallback, 904, mockLogger5);
    assert.strictEqual(res5.allFallback, true, "allFallback harus bernilai true");
    assert.strictEqual(loggedWarning, "duration_in_traffic tidak tersedia pada seluruh elemen", "Peringatan traffic harus dicatat");
    console.log('✓ Test 5 Lolos: Log level WARNING tercatat jika seluruh kandidat fallback.\n');

    // -------------------------------------------------------------------------
    // TEST 6: Regresi: Perilaku elemen gagal dan kasus semua gagal (results.length === 0)
    // -------------------------------------------------------------------------
    console.log('Test 6: Uji regresi penanganan elemen gagal...');
    let failureWarningCount = 0;
    const mockLogger6 = {
        info: () => {},
        warn: (msg) => {
            if (msg.includes('Google API Gagal')) failureWarningCount++;
        }
    };
    const mockApiResponseWithFailures = {
        data: {
            rows: [{
                elements: [
                    { status: 'NOT_FOUND' },
                    { status: 'ZERO_RESULTS' },
                    {
                        status: 'OK',
                        distance: { text: '3.0 km', value: 3000 },
                        duration: { text: '10 menit', value: 600 },
                        duration_in_traffic: { text: '12 menit', value: 720 }
                    }
                ]
            }]
        }
    };
    const res6 = _processDistanceMatrixElements(dummyCandidates, mockApiResponseWithFailures, 905, mockLogger6);
    assert.strictEqual(res6.results.length, 1, "Hanya elemen berstatus OK yang masuk ke results");
    assert.strictEqual(failureWarningCount, 2, "2 elemen gagal harus dicatat di log warning");
    assert.strictEqual(res6.results[0].id, 3, "Kandidat ke-3 yang OK harus berhasil disimpan");

    // Kasus semua gagal
    const mockApiResponseAllFail = {
        data: {
            rows: [{
                elements: [
                    { status: 'NOT_FOUND' },
                    { status: 'ZERO_RESULTS' }
                ]
            }]
        }
    };
    const resAllFail = _processDistanceMatrixElements([dummyCandidates[0], dummyCandidates[1]], mockApiResponseAllFail, 906, mockLogger6);
    assert.strictEqual(resAllFail.results.length, 0, "Jika semua gagal, results harus kosong");
    console.log('✓ Test 6 Lolos: Perilaku eliminasi elemen gagal dan kondisi results kosong tetap terjaga (tanpa regresi).\n');

    console.log('================================================================');
    console.log(' SELURUH UNIT TEST TRAFFIC-AWARE ETA BERHASIL (100% PASS)');
    console.log('================================================================\n');

    process.exit(0);
}

runTrafficEtaTests();

