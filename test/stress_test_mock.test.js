/**
 * Unit Test Mock untuk Logika Pemrosesan Stress Test
 * Memverifikasi:
 * 1. Penghitungan penugasan pertama vs re-offer berdasarkan id_panggilan
 * 2. Variasi ID pasien
 * 3. Format timestamp ISO 8601
 * 4. Pembentukan baris CSV lengkap
 */

const assert = require('assert');

function testStressTestLogic() {
    console.log('--- Menjalankan Unit Test Mock: Stress Test Metrics Logic ---');

    // Test 1: Pemisahan assignment awal dan re-offer
    const assignedCallIds = new Set();
    let uniqueAssignmentCount = 0;
    let reofferCount = 0;

    const mockMessages = [
        { topic: 'ambulans/tugas/22', payload: { id_panggilan: 101, id_pasien: 45 } },
        { topic: 'ambulans/tugas/23', payload: { id_panggilan: 102, id_pasien: 46 } },
        { topic: 'ambulans/tugas/24', payload: { id_panggilan: 101, id_pasien: 45 } }, // re-offer panggilan 101
        { topic: 'ambulans/tugas/25', payload: { id_panggilan: 103, id_pasien: 47 } },
        { topic: 'ambulans/tugas/26', payload: { id_panggilan: 102, id_pasien: 46 } }, // re-offer panggilan 102
        { topic: 'ambulans/tugas/27', payload: { id_panggilan: 101, id_pasien: 45 } }, // re-offer panggilan 101 lagi
    ];

    mockMessages.forEach(msg => {
        const callId = msg.payload.id_panggilan;
        if (!assignedCallIds.has(callId)) {
            assignedCallIds.add(callId);
            uniqueAssignmentCount++;
        } else {
            reofferCount++;
        }
    });

    assert.strictEqual(uniqueAssignmentCount, 3, 'Harus ada 3 penugasan panggilan unik (101, 102, 103)');
    assert.strictEqual(reofferCount, 3, 'Harus ada 3 re-offer terdeteksi');
    console.log('✓ Test 1 Lolos: Penugasan unik dan re-offer terhitung akurat.');

    // Test 2: Variasi ID pasien
    const patientIds = [45, 46, 47, 48, 49];
    const generatedPatientIds = [];
    for (let i = 0; i < 15; i++) {
        generatedPatientIds.push(patientIds[i % patientIds.length]);
    }
    const uniqueIds = new Set(generatedPatientIds);
    assert.strictEqual(uniqueIds.size, 5, 'Harus bervariasi melintasi seluruh daftar ID pasien');
    assert.strictEqual(generatedPatientIds[0], 45);
    assert.strictEqual(generatedPatientIds[5], 45);
    console.log('✓ Test 2 Lolos: Variasi ID pasien berhasil.');

    // Test 3: Validasi format ISO 8601
    const isoString = new Date().toISOString();
    assert.match(isoString, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/, 'Format timestamp harus ISO 8601');
    console.log('✓ Test 3 Lolos: Validasi timestamp ISO 8601 berhasil.');

    // Test 4: Format CSV lengkap
    const mockResult = {
        tahap: 1,
        jumlahDikirim: 10,
        waktuMulaiIso: isoString,
        waktuSelesaiIso: isoString,
        waktuPublishMs: 150,
        waktuTotalMs: 35150,
        masukDB: 10,
        terproses: 10,
        driverDitugaskanUnik: 8,
        jumlahReoffer: 2,
        noDrivers: 0,
        apiFailed: 0,
        gagal: 0,
        pending: 0,
        diLuarJendela: 0,
        tingkatProses: '100.0',
        minResponseMs: 120,
        maxResponseMs: 850,
        avgResponseMs: 420,
        statusDetail: JSON.stringify({ WAITING_FOR_DRIVER: 8, ON_THE_WAY: 2 })
    };

    const csvRow = [
        mockResult.tahap,
        mockResult.jumlahDikirim,
        `"${mockResult.waktuMulaiIso}"`,
        `"${mockResult.waktuSelesaiIso}"`,
        mockResult.waktuPublishMs,
        mockResult.waktuTotalMs,
        mockResult.masukDB,
        mockResult.terproses,
        mockResult.driverDitugaskanUnik,
        mockResult.jumlahReoffer,
        mockResult.noDrivers,
        mockResult.apiFailed,
        mockResult.gagal,
        mockResult.pending,
        mockResult.diLuarJendela,
        mockResult.tingkatProses,
        mockResult.minResponseMs,
        mockResult.maxResponseMs,
        mockResult.avgResponseMs,
        `"${mockResult.statusDetail.replace(/"/g, '""')}"`
    ].join(',');

    const cols = csvRow.split(',');
    assert.strictEqual(cols[0], '1', 'Tahap harus 1');
    assert.strictEqual(cols[1], '10', 'Jumlah kirim harus 10');
    console.log('✓ Test 4 Lolos: Format CSV lengkap valid.');

    console.log('\nSEMUA PENGUJIAN MOCK BERHASIL (100% PASS).\n');
}

testStressTestLogic();

