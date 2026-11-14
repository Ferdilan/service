# CHANGELOG REVISION (KOREKSI DAN DOKUMENTASI REVISI NASKAH)

Dokumen ini mencatat seluruh penyesuaian teknis pada branch `revision-fixes` (yang bercabang dari tag `tested-version` / commit `9e9dbc3`), alasan perubahan, serta pemetaannya terhadap catatan komentar reviewer.

---

## 1. Pemetaan Komentar Reviewer Terhadap Tindakan

| Kode Reviewer | Topik Komentar Reviewer | Temuan Faktual pada Kode | Tindakan yang Diambil pada `revision-fixes` |
|---|---|---|---|
| **N2** | Ketiadaan `departure_time` dan klaim traffic-based ETA | [`service.js:878, 891`](../service.js#L878): request tidak mengirim `departure_time`, membaca `duration` (statis), bukan `duration_in_traffic`. Parameter `travelMode` diabaikan oleh library client. | Logika dispatch dipertahankan sesuai versi uji naskah (jalur efisiensi biaya). Perilaku sebenarnya didokumentasikan secara presisi di [`docs/IMPLEMENTATION_NOTES.md`](docs/IMPLEMENTATION_NOTES.md) untuk perbaikan teks Section 2.2.2 naskah. |
| **N4** | Pelaporan kuantitas stress test tidak lengkap | [`stress-test.js:270`](../stress-test.js#L270): File CSV sebelumnya hanya memuat 6 kolom waktu respons, membuang data database, status error, dan tingkat pemrosesan. | Skrip [`stress-test.js`](../stress-test.js) diperbarui untuk mengekspor 20 kolom metrik lengkap ke CSV (termasuk status breakdown, failure count, dan panggilan di luar jendela observasi). |
| **N6** | Pelabelan jeda antar tahap pengujian | [`stress-test.js:233`](../stress-test.js#L233): Jeda 180 detik berlabel "Screenshot Time". | Label diganti menjadi label netral konfiguratif (`WAIT_STAGE_LABEL`, default: *"Jeda stabilisasi antar tahap"*). |
| **N7** | Pencatatan timestamp pengujian | [`stress-test.js:51, 240`](../stress-test.js#L51): Hanya timestamp awal dan akhir total yang dicatat; tidak ada timestamp per tahap. | Menambahkan pencatatan timestamp absolut format ISO 8601 (`waktuMulaiIso` dan `waktuSelesaiIso`) per masing-masing tahap pengujian, disimpan ke CSV. |
| **C9** | Korelasi ID panggilan pada stress test & re-offer inflation | [`stress-test.js:86-92`](../stress-test.js#L86): Assignment dihitung naif tanpa validasi ID panggilan. Re-offer dalam jendela 35 detik ikut terhitung sebagai penugasan baru. Semua panggilan memakai `id_pasien = 45`. | [`stress-test.js`](../stress-test.js) diperbarui dengan tracking `id_panggilan` via `Set`: pesan pertama dihitung sebagai penugasan unik, pesan berulang untuk ID yang sama dilaporkan terpisah sebagai `reofferCount`. Daftar ID pasien divariasikan melintasi pool ID. |
| **D14** | Penanganan kegagalan elemen Distance Matrix (Fallback) | [`service.js:889-899`](../service.js#L889): Kandidat dengan status selain `OK` dibuang tanpa fallback ke Haversine. Jika semua gagal, status `API_FAILED`. | Sesuai keputusan penulis: mempertahankan perilaku asli tanpa mengubah kode dispatch. Perilaku didokumentasikan di [`docs/IMPLEMENTATION_NOTES.md`](docs/IMPLEMENTATION_NOTES.md) untuk koreksi Algorithm 1 naskah. |
| **D17** | Aturan pengurutan kandidat (Sorting rules) | [`service.js:908`](../service.js#L908): Urutan berdasarkan `etaSeconds` lalu `distanceMeters`. Haversine tidak menjadi tie-breaker. | Sesuai keputusan penulis: mempertahankan kode asli. Aturan pengurutan faktual didokumentasikan di [`docs/IMPLEMENTATION_NOTES.md`](docs/IMPLEMENTATION_NOTES.md). |
| **F22** | Kebocoran kredensial dan PII di repositori publik | `service_simulation.js:9` dan `test_publisher.js:4` berisi secret hard-coded. File `.sql` berisi NIK, nama, alamat, dan foto KTP. | File dump SQL dan skrip berisi secret dihapus dari working tree (`git rm`), `.gitignore` diperbarui, dibuat `.env.example`, dan disusun panduan pembersihan riwayat git di [`MANUAL_STEPS.md`](MANUAL_STEPS.md). |

---

## 2. Rincian Perubahan Berdasarkan Berkas

### A. Keamanan & Sanitasi Berkas (P0)
- **File Dihapus**:
  - `backup_full.sql`: Dihapus (berisi dump data riil tabel ambulans & pasien dengan NIK, nama, alamat, nomor telepon, dan password hash).
  - `backup_schema.sql`: Dihapus (berisi seed INSERT data profil ambulans & pasien).
  - `exported_data.sql`: Dihapus (berisi dump data duplikat).
  - `service_simulation.js`: Dihapus (berisi hard-coded Google Maps API Key dan kredensial MQTT broker CloudAMQP).
  - `test_publisher.js`: Dihapus (berisi hard-coded kredensial MQTT broker CloudAMQP).
- **File Ditambahkan**:
  - `.env.example`: Template variabel lingkungan tanpa nilai rahasia asli.
  - `.gitignore`: Diperbarui untuk memblokir `.env`, `*.sql`, `*.log`, `logs/`, dan `*.csv`.
  - `MANUAL_STEPS.md`: Panduan langkah eksekusi manual bagi pemilik repositori untuk rotasi API key, pembersihan commit history via `git-filter-repo`, dan koordinasi GitHub Support.

### B. Dokumentasi Naskah
- **File Ditambahkan**:
  - `docs/IMPLEMENTATION_NOTES.md`: Menjelaskan perilaku teknis faktual sistem yang diuji untuk panduan revisi naskah jurnal (Section 2.2.2, Algorithm 1, Section 3.4, Abstract, dan Conclusion).

### C. Pembaruan Skrip Pengujian Titik Jenuh (`stress-test.js`)
- **File Dimodifikasi**: `stress-test.js`
  - **Baris 24–33**: Menambahkan konfigurasi `ID_PASIEN_LIST` (daftar variasi ID pasien) dan konfigurasi env untuk durasi jendela serta jeda tahap.
  - **Baris 89–137**: Implementasi penelusuran `id_panggilan` menggunakan `Set`:
    - Mengurai payload `ambulans/tugas/+` untuk mendapatkan `id_panggilan`.
    - Panggilan pertama dicatat ke `uniqueAssignmentCount`.
    - Pengiriman ulang untuk `id_panggilan` yang sama dihitung terpisah sebagai `reofferCount`.
    - Opsi `SIMULATE_DRIVER_ACCEPT` untuk simulasi penerimaan armada.
  - **Baris 140–165**: Variasi `id_pasien` per panggilan uji menggunakan modulo `ID_PASIEN_LIST`.
  - **Baris 205–245**: Pencatatan timestamp ISO 8601 awal dan akhir tiap tahap, penghitungan panggilan di luar jendela observasi (`outsideWindow`).
  - **Baris 275–330**: Ekspor CSV komprehensif (20 metrik kuantitatif lengkap).
- **File Ditambahkan**:
  - `test/stress_test_mock.test.js`: Unit test berbasis mock untuk memverifikasi logika pemilahan re-offer, variasi ID pasien, timestamp ISO, dan integritas kolom CSV tanpa menembak infrastruktur jaringan/database.

---

## 3. Daftar Pengujian yang HARUS Diulang vs Tidak Perlu

1. **Pengujian Titik Jenuh (Stress Test)**:
   - **Status**: **HARUS DIULANG**.
   - **Alasan**: Skrip pengujian sebelumnya (`stress-test.js`) menghitung re-offer sebagai penugasan baru sehingga menggelembungkan metrik `Driver Ditugaskan`, menggunakan ID pasien tunggal (45), dan tidak mengekspor status breakdown ke CSV. Pengujian ulang dengan skrip baru akan menghasilkan data yang valid dan dapat dipertanggungjawabkan di hadapan reviewer.
2. **Pengujian Seleksi Algoritma (Selection Accuracy & Consistency)**:
   - **Status**: **TIDAK PERLU DIULANG** (asalkan teks naskah disesuaikan).
   - **Alasan**: Kode logika dispatch di `service.js` tidak diubah sama sekali. Jika teks naskah disinkronkan dengan perilaku aktual (Distance Matrix standar dengan $k = 5$ tanpa live-traffic), maka hasil evaluasi akurasi seleksi yang telah dilaporkan tetap valid sesuai dataset yang diuji.

---

## 4. Daftar Bagian Naskah yang HARUS Diubah

1. **Abstract**: Koreksi klaim "live-traffic ETA" menjadi "standard road network travel time based on Google Distance Matrix", dan cantumkan shortlist $k = 5$.
2. **Section 2.2.2**: Hapus klaim `departure_time=now` dan `duration_in_traffic`. Jelaskan pemanggilan batch Distance Matrix satu arah dengan mode berkendara standar.
3. **Algorithm 1 (Lines 34–36)**: Koreksi pseudocode: jelaskan bahwa kandidat gagal dieliminasi tanpa fallback ke Haversine, dan tie-breaker didasarkan pada jarak rute Google Maps.
4. **Section 3.4**: Paparkan bahwa status armada tetap `AVAILABLE` selama penawaran 20 detik dan baru beralih ke `BUSY` setelah konfirmasi terima.
5. **Section 4 / Results**: Perbarui tabel hasil stress test menggunakan data keluaran dari skrip `stress-test.js` versi revisi yang memisahkan assignment pertama dan re-offer.
