# CHANGELOG REVISION (KOREKSI DAN DOKUMENTASI REVISI NASKAH)

Dokumen ini mencatat seluruh penyesuaian teknis pada branch `revision-fixes`, alasan perubahan, pemetaannya terhadap catatan komentar reviewer (khususnya N2, N4, N6, N7, C9, D14, D17, F22, dan B7), serta daftar pengujian yang harus diulang.

---

## 1. Pemetaan Komentar Reviewer Terhadap Tindakan

| Kode Reviewer | Topik Komentar Reviewer | Temuan Faktual pada Kode | Tindakan yang Diambil pada `revision-fixes` |
|---|---|---|---|
| **N2** | Ketiadaan `departure_time` dan klaim traffic-based ETA | [`service.js`](../service.js) sebelumnya tidak mengirim `departure_time`, membaca `duration` (statis), dan menggunakan parameter `travelMode: 'DRIVING'` yang tidak sesuai dengan library client. | **Diimplementasikan (Jalur B)**: Menambahkan `departure_time: 'now'`, mengganti parameter menjadi `mode: 'driving'`, membaca `element.duration_in_traffic.value` dengan fallback otomatis ke `element.duration.value`, mencatat `etaSource` (`'duration_in_traffic'` vs `'duration'`), dan mencatat warning bila seluruh elemen fallback. |
| **B7** | Estimasi Biaya Pemanggilan API Google Maps | Penggunaan `departure_time` mengubah tier penagihan Google Maps Platform dari *Basic Distance Matrix* menjadi *Advanced / Traffic-aware Distance Matrix*. | Dicatat dalam dokumentasi bahwa penggunaan fitur traffic-aware berdampak pada struktur tarif API yang lebih tinggi per 1.000 elemen. Estimasi biaya pada naskah wajib dihitung ulang sesuai tanggal akses tarif resmi. |
| **N4** | Pelaporan kuantitas stress test tidak lengkap | [`stress-test.js`](../stress-test.js) sebelumnya hanya mengekspor 6 kolom waktu respons, membuang data database, status error, dan tingkat pemrosesan. | Skrip [`stress-test.js`](../stress-test.js) diperbarui untuk mengekspor 20 kolom metrik lengkap ke CSV (termasuk status breakdown, failure count, dan panggilan di luar jendela observasi). |
| **N6** | Pelabelan jeda antar tahap pengujian | [`stress-test.js`](../stress-test.js) menggunakan jeda 180 detik berlabel "Screenshot Time". | Label diganti menjadi label netral konfiguratif (`WAIT_STAGE_LABEL`, default: *"Jeda stabilisasi antar tahap"*). |
| **N7** | Pencatatan timestamp pengujian | Hanya timestamp awal dan akhir total yang dicatat; tidak ada timestamp per tahap. | Menambahkan pencatatan timestamp absolut format ISO 8601 (`waktuMulaiIso` dan `waktuSelesaiIso`) per masing-masing tahap pengujian, disimpan ke CSV. |
| **C9** | Korelasi ID panggilan pada stress test & re-offer inflation | Assignment dihitung naif tanpa validasi ID panggilan. Re-offer dalam jendela 35 detik ikut terhitung sebagai penugasan baru. Semua panggilan memakai `id_pasien = 45`. | [`stress-test.js`](../stress-test.js) diperbarui dengan tracking `id_panggilan` via `Set`: pesan pertama dihitung sebagai penugasan unik, pesan berulang untuk ID yang sama dilaporkan terpisah sebagai `reofferCount`. Daftar ID pasien divariasikan melintasi pool ID. |
| **D14** | Penanganan kegagalan elemen Distance Matrix (Fallback) | Kandidat dengan status selain `OK` dibuang tanpa fallback ke Haversine. Jika semua gagal, status `API_FAILED`. | Sesuai keputusan penulis: mempertahankan perilaku asli tanpa mengubah alur seleksi elemen gagal/sukses. Perilaku didokumentasikan di [`docs/IMPLEMENTATION_NOTES.md`](docs/IMPLEMENTATION_NOTES.md). |
| **D17** | Aturan pengurutan kandidat (Sorting rules) | Urutan berdasarkan `etaSeconds` lalu `distanceMeters`. Haversine tidak menjadi tie-breaker. | Sesuai keputusan penulis: mempertahankan aturan sorting asli. Didokumentasikan di [`docs/IMPLEMENTATION_NOTES.md`](docs/IMPLEMENTATION_NOTES.md). |
| **F22** | Kebocoran kredensial dan PII di repositori publik | `service_simulation.js` dan `test_publisher.js` berisi secret hard-coded. File `.sql` berisi NIK, nama, alamat, dan foto KTP. | File dump SQL dan skrip ber-secret dihapus dari working tree (`git rm`), `.gitignore` diperbarui, dibuat `.env.example`, dan disusun panduan pembersihan riwayat git di [`MANUAL_STEPS.md`](MANUAL_STEPS.md). |

---

## 2. Rincian Perubahan Kode (Tag `traffic-aware-v1`)

### A. Backend Dispatch Service ([`service.js`](../service.js))
- **Baris 2–3**: Penambahan modul native `fs` dan `path` untuk audit logging.
- **Baris 768–780**: Penambahan fungsi `_buildDistanceMatrixParams(patientLocation, candidates, apiKey)`:
  - Menyusun `origins` dan `destinations`.
  - Mengirim `mode: 'driving'` (sesuai enum `TravelMode` library).
  - Mengirim `departure_time: 'now'` untuk mengaktifkan kalkulasi lalu lintas real-time.
- **Baris 782–878**: Penambahan fungsi `_processDistanceMatrixElements(candidates, apiResponse, newCallId, logTarget)`:
  - Memeriksa ketersediaan `element.duration_in_traffic.value`.
  - Menetapkan `etaSeconds` dan `etaSource` (`'duration_in_traffic'` vs `'duration'`).
  - Mencatat warning jika seluruh kandidat fallback ke durasi statis.
  - Menuliskan log per kandidat secara numerik (`haversineMeters`, `durationSeconds`, `durationInTrafficSeconds`, `etaSeconds`, `etaSource`, `distanceMeters`, `timestamp` ISO 8601) ke console, MQTT transport, dan file lokal `logs/dispatch-candidates.jsonl`.
- **Baris 1007–1020**: Refactoring tahap pemanggilan Google Maps API di `_findBestDriver` menggunakan fungsi helper di atas.
- **Baris 1184–1189**: Ekspor fungsi `_findBestDriver`, `_assignDriverToCall`, `_buildDistanceMatrixParams`, dan `_processDistanceMatrixElements` untuk pengujian modular.

### B. Skrip Verifikasi Mandiri & Testing
- **Berkas Baru**: [`scripts/verify-traffic-eta.js`](../scripts/verify-traffic-eta.js)
  - Skrip mandiri untuk memverifikasi apakah akun Google Maps API pengguna mengembalikan `duration_in_traffic` pada 2 koordinat tetap di Kota Malang.
- **Berkas Baru**: [`test/traffic_aware_eta.test.js`](../test/traffic_aware_eta.test.js)
  - Unit test berbasis mock (tanpa jaringan) yang memvalidasi ketersediaan traffic, fallback statis, penanganan elemen campuran, log warning fallback total, validasi parameter request, dan uji regresi kegagalan elemen.

---

## 3. Daftar Pengujian yang HARUS Diulang

Karena implementasi kini beralih ke **Traffic-Aware ETA (Jalur B)**, hasil empiris pada naskah yang sebelumnya diambil dari model statis wajib diperbarui:

1. **Pengujian Kualitas Layanan (QoS)**:
   - **Status**: **HARUS DIULANG**.
   - **Komponen**: Pengukuran waktu respons, delay transmisi MQTT, dan waktu eksekusi dispatch pada skenario lalu lintas nyata.
2. **Pengujian Pemilihan Driver (Selection Accuracy & Fleet Ranking)**:
   - **Status**: **HARUS DIULANG**.
   - **Komponen**: Tabel 3 (Perbandingan peringkat Haversine vs Google Maps), Analisis S2, dan Gambar 4 (Peta rute pemilihan driver).
   - **Alasan**: Kondisi macet pada jam sibuk dapat mengubah urutan driver tercepat dibanding data durasi statis sebelumnya.
3. **Pengujian Titik Jenuh (Stress Test)**:
   - **Status**: **HARUS DIULANG**.
   - **Komponen**: Tabel 4 dan Gambar 5 (Grafik waktu respons vs jumlah panggilan bersamaan).
   - **Alasan**: Menggunakan skrip `stress-test.js` versi revisi yang memisahkan assignment pertama dan re-offer serta memvariasikan ID pasien.
4. **Estimasi Biaya Operasional (B7)**:
   - **Status**: **HARUS DIULANG**.
   - **Alasan**: Menghitung ulang estimasi biaya berdasarkan SKU *Advanced / Traffic-aware Distance Matrix* Google Maps Platform.

---

## 4. Daftar Bagian Naskah yang HARUS Diubah

1. **Abstrak & Kesimpulan**:
   - Nyatakan bahwa sistem mengintegrasikan *traffic-aware ETA* berbasis Google Distance Matrix API dengan fallback otomatis ke *standard driving duration* saat data lalu lintas tidak tersedia.
2. **Section 2.2.2**:
   - Jelaskan penggunaan parameter `mode='driving'` dan `departure_time='now'`.
   - Paparkan skema fallback bertingkat dan struktur payload data yang dikembalikan.
3. **Algorithm 1**:
   - Perbarui pseudocode untuk mencerminkan evaluasi bersyarat antara `duration_in_traffic` dan `duration`.
4. **Section 4 (Results & Discussion)**:
   - Masukkan tabel dan grafik hasil eksperimen baru dari pengujian QoS, akurasi seleksi lalu lintas, dan stress test terdistribusi.
