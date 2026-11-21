# CATATAN IMPLEMENTASI DAN PERILAKU SISTEM (IMPLEMENTATION NOTES)

Dokumen ini mendokumentasikan secara rinci arsitektur dan perilaku faktual sistem dispatch ambulans pada dua versi kode:
1. **Versi yang Diuji Sebelumnya (`tested-version` / commit `9e9dbc3`)**: Implementasi berbasis *standard road-network duration* (tanpa traffic).
2. **Versi Traffic-Aware (`revision-fixes` / tag `traffic-aware-v1`)**: Implementasi rute berbasis kondisi lalu lintas real-time (*live-traffic ETA* dengan fallback statis).

---

## 1. Komparasi Arsitektur Dispatch: `tested-version` vs `traffic-aware-v1`

| Aspek / Parameter | Versi yang Diuji Sebelumnya (`tested-version`) | Versi Traffic-Aware (`traffic-aware-v1`) |
|---|---|---|
| **Parameter Waktu Keberangkatan** | Tidak diset (`departure_time` absen) | `departure_time: 'now'` ([`service.js:777`](../service.js#L777)) |
| **Parameter Moda Transportasi** | `travelMode: 'DRIVING'` (diabaikan library serializer) | `mode: 'driving'` ([`service.js:776`](../service.js#L776)) |
| **Field Pembacaan ETA Utama** | `element.duration.value` (durasi statis) | `element.duration_in_traffic.value` ([`service.js:819-823`](../service.js#L819)) |
| **Mekanisme Fallback ETA** | Tidak ada (hanya membaca field statis) | Fallback otomatis ke `element.duration.value` jika `duration_in_traffic` absen ([`service.js:824-826`](../service.js#L824)) |
| **Pelacakan Sumber ETA** | Tidak ada (implisit statis) | Menyimpan `etaSource` (`'duration_in_traffic'` atau `'duration'`) dan `etaSeconds` ([`service.js:820, 825`](../service.js#L820)) |
| **Peringatan Fallback Total** | Tidak ada | Log warning: *"duration_in_traffic tidak tersedia pada seluruh elemen"* ([`service.js:873-875`](../service.js#L873)) |
| **Logging Kandidat Numerik** | Parsial (text string format) | Lengkap ke console, MQTT, & file JSON Lines ([`service.js:844-850`](../service.js#L844), `logs/dispatch-candidates.jsonl`) |
| **Ukuran Shortlist ($k$)** | $k = 5$ ([`service.js:891`](../service.js#L891)) | $k = 5$ (dipertahankan, tanpa perubahan) |
| **Aturan Pengurutan (Sorting)** | `etaSeconds` ASC, lalu `distanceMeters` ASC | `etaSeconds` ASC, lalu `distanceMeters` ASC (dipertahankan) |
| **Penanganan Elemen Gagal** | Status selain OK dibuang; jika semua gagal: `API_FAILED` | Status selain OK dibuang; jika semua gagal: `API_FAILED` (dipertahankan) |
| **Status Driver Saat Penawaran** | Tetap `AVAILABLE` (baris `BUSY` di-comment) | Tetap `AVAILABLE` (dipertahankan sebagai batasan desain) |

---

## 2. Rincian Implementasi Versi Traffic-Aware (`traffic-aware-v1`)

### A. Konstruksi Request Distance Matrix
- **Lokasi Kode**: [`service.js:768-780, 1007-1008`](../service.js#L768)
- Fungsi pembantu `_buildDistanceMatrixParams` menyusun parameter sesuai standar library `@googlemaps/google-maps-services-js`:
  ```javascript
  function _buildDistanceMatrixParams(patientLocation, candidates, apiKey = GOOGLE_MAPS_API_KEY) {
      const origins = [patientLocation];
      const destinations = candidates.map(c => c.location);
      return {
          key: apiKey,
          origins: origins,
          destinations: destinations,
          mode: 'driving',
          departure_time: 'now'
      };
  }
  ```
- **Penting Mengenai Penagihan & Akses API Google**:
  - Google Maps Platform hanya mengembalikan objek `duration_in_traffic` jika parameter `departure_time` disertakan dan akun penagihan Google Cloud Anda memiliki hak akses untuk endpoint terkait.
  - Permintaan Distance Matrix dengan menyertakan `departure_time` (traffic-aware) dikenai tarif *Advanced / Traffic-aware SKU* (memiliki struktur biaya per 1.000 elemen yang lebih tinggi dibandingkan *Basic Distance Matrix*). Rincian tarif resmi bergantung pada ketentuan harga Google Maps Platform pada tanggal akses penagihan Anda.

### B. Pemrosesan Elemen & Fallback Bertingkat
- **Lokasi Kode**: [`service.js:782-878, 1014`](../service.js#L782)
- Pada setiap elemen kandidat dengan `status === 'OK'`:
  1. Jika `element.duration_in_traffic` tersedia dan memiliki nilai numerik `value`, sistem menetapkan:
     - `etaSeconds = element.duration_in_traffic.value`
     - `etaSource = 'duration_in_traffic'`
     - Flag `allFallback = false`
  2. Jika `element.duration_in_traffic` tidak tersedia (misal data macet tidak terdeteksi atau API key tidak mendukung), sistem otomatis fallback:
     - `etaSeconds = element.duration.value`
     - `etaSource = 'duration'`
  3. Jika seluruh kandidat dalam panggilan tersebut terpaksa menggunakan durasi statis (`allFallback === true`), sistem mencatat log peringatan:
     ```javascript
     logger.warn("duration_in_traffic tidak tersedia pada seluruh elemen", { category: 'dispatch' });
     ```
     Hal ini menandakan bahwa pada panggilan tersebut, klaim pemilihan berbasis lalu lintas tidak terpenuhi secara riil.

### C. Logging Numerik Lengkap dan Berkas Audit
- **Lokasi Kode**: [`service.js:830-858`](../service.js#L830)
- Setiap kandidat yang diproses dicatat dengan struktur data numerik:
  ```json
  {
    "callId": 105,
    "candidateId": 22,
    "candidateName": "Ambulans 1",
    "haversineMeters": 850,
    "durationSeconds": 320,
    "durationInTrafficSeconds": 460,
    "etaSeconds": 460,
    "etaSource": "duration_in_traffic",
    "distanceMeters": 1400,
    "timestamp": "2026-11-14T13:23:13.853Z"
  }
  ```
- Data di atas dialirkan ke:
  1. Terminal Console & Winston Logger (MQTT transport dengan metadata).
  2. Berkas persistensi JSON Lines lokal: `logs/dispatch-candidates.jsonl` (direktori ini telah masuk dalam `.gitignore` sehingga tidak terekspos ke repositori).

---

## 3. Skrip Verifikasi Mandiri (`scripts/verify-traffic-eta.js`)

Untuk memverifikasi secara langsung apakah kredensial Google Maps API Anda saat ini telah aktif dan mengembalikan `duration_in_traffic`, gunakan skrip uji mandiri:

```bash
# Pastikan file .env memuat GOOGLE_MAPS_API_KEY yang valid
node scripts/verify-traffic-eta.js
```

### Parameter Uji Skrip:
- **Origin**: Alun-Alun Tugu Kota Malang (`-7.978645, 112.631786`)
- **Destination**: RSUD Dr. Saiful Anwar Malang (`-7.965801, 112.624512`)
- **Metode**: 1 panggilan tunggal batch (2 titik) dengan `mode: 'driving'` dan `departure_time: 'now'`.
- **Keluaran**: Menampilkan `distance`, `duration` statis, `duration_in_traffic`, serta selisih keterlambatan akibat macet.

---

## 4. Bagian Naskah Jurnal yang Harus Disesuaikan

Implementasi versi traffic-aware ini berdampak langsung pada klaim metodologi dan hasil eksperimen pada naskah:

1. **Section 2.2.2 (Distance Matrix API & Routing Formulation)**:
   - Paparkan bahwa sistem mengirim request batch berparameter `mode='driving'` dan `departure_time='now'`.
   - Jelaskan skema pembacaan `duration_in_traffic` dengan fallback deterministik ke `duration` statis bila data lalu lintas tidak tersedia.
   - Cantumkan analisis implikasi biaya operasional API (Advanced Distance Matrix pricing tier).

2. **Algorithm 1 (Pseudocode)**:
   - Perbarui pseudocode pada tahap kalkulasi ETA:
     $$\text{ETA} \leftarrow \begin{cases} \text{element.duration\_in\_traffic}, & \text{jika tersedia} \\ \text{element.duration}, & \text{fallback} \end{cases}$$
   - Pertahankan logika pembuangan elemen gagal (`status != OK`) dan status `API_FAILED` bila seluruh elemen gagal.

3. **Section 3.4 & Pembahasan Batasan (Limitations)**:
   - Dokumentasikan bahwa status driver tetap `AVAILABLE` saat penawaran (fase 20 detik) untuk mengakomodasi fleksibilitas penugasan, namun berpotensi memicu tawaran ganda pada lonjakan beban panggilan.

4. **Koreksi Terminologi di Seluruh Naskah**:
   - Jika naskah sebelumnya menggunakan istilah umum *"traffic-aware"* tanpa bukti implementasi, kini klaim tersebut valid dengan syarat eksperimen pemilihan armada diulang menggunakan versi kode ini.
   - Jika membandingkan dengan data eksperimen lama (`tested-version`), gunakan istilah *"standard road-network ETA"* untuk data lama dan *"live-traffic ETA"* untuk data baru.
