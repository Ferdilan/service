# CATATAN IMPLEMENTASI DAN PERILAKU SISTEM (IMPLEMENTATION NOTES)

Dokumen ini mendokumentasikan perilaku faktual sistem dispatch ambulans pada versi kode yang diuji (`tested-version` / commit `9e9dbc3`), serta pemetaan ketidaksesuaian terhadap naskah publikasi ilmiah yang sedang direvisi.

---

## 1. Analisis Perilaku Sistem Faktual (Tested Codebase)

### A. Shortlist Kandidat ($k = 5$)
- **Lokasi Kode**: [`service.js:853`](../service.js#L853)
  ```javascript
  const candidates = driversWithDistance.slice(0, 5); // Tempat pengambilan 5 kandidat teratas
  ```
- **Perilaku Faktual**:
  Sistem menghitung jarak garis lurus (Haversine) antara lokasi panggilan pasien dan seluruh armada yang berstatus `AVAILABLE` pada kategori yang sesuai (`PSC` atau `Relawan`). Hasil perhitungan diurutkan ascending berdasarkan jarak terdekat, lalu dipotong tepat $k = 5$ kandidat teratas.
- **Catatan Konfigurasi**: Nilai $k$ bersifat *hard-coded* ($k = 5$), bukan parameter dinamis dari environment variable.

---

### B. Request Distance Matrix & Parameter yang Dikirim
- **Lokasi Kode**: [`service.js:878-880`](../service.js#L878-L880)
  ```javascript
  const apiResponse = await gmapsClient.distancematrix({
      params: { key: GOOGLE_MAPS_API_KEY, origins: origins, destinations: destinations, travelMode: 'DRIVING' }
  });
  ```
- **Perilaku Faktual**:
  1. **Batch Request**: Sistem mengirim **satu request batch** untuk seluruh $k$ kandidat sekaligus (`origins: [patientLocation]`, `destinations: candidates.map(...)`), bukan satu per satu.
  2. **Tanpa `departure_time`**: Request tidak menyertakan parameter `departure_time: 'now'` maupun waktu tertentu.
  3. **Parameter `travelMode` vs `mode`**: Library klien `@googlemaps/google-maps-services-js` (v3.4.0) mendefinisikan skema parameter moda transportasi sebagai `mode?: TravelMode` (lihat file definisi tipe library di `node_modules/@googlemaps/google-maps-services-js/dist/distance.d.ts:61`), bukan `travelMode`. Akibatnya, parameter `travelMode: 'DRIVING'` diabaikan oleh serializer library dan API secara default menggunakan fallback bawaan Google (`mode=driving`).
  4. **Field ETA yang Dibaca**:
     - **Lokasi Kode**: [`service.js:891-893, 896`](../service.js#L891-L893)
     ```javascript
     const api_distance_text = element.distance.text;
     const api_duration_text = element.duration.text;
     const api_duration_seconds = element.duration.value;
     ...
     results.push({ id: candidate.id, nama: candidate.nama, etaSeconds: api_duration_seconds, distanceMeters: element.distance.value, ... });
     ```
     Sistem membaca `element.duration.value` (durasi statis tanpa lalu lintas real-time), **bukan** `element.duration_in_traffic`. Karena request tidak menyertakan `departure_time`, Google Maps API memang tidak mengembalikan field `duration_in_traffic`.

---

### C. Penanganan Kegagalan Elemen & Skenario Seluruh Elemen Gagal
- **Lokasi Kode**: [`service.js:889-899`](../service.js#L889-L899) (Elemen individual) dan [`service.js:903-906`](../service.js#L903-L906) (Semua elemen)
  ```javascript
  apiResponse.data.rows[0].elements.forEach((element, index) => {
      const candidate = candidates[index];
      if (element.status === 'OK') {
          ...
          results.push({ ... });
      } else {
          logger.warn(`  -> ${candidate.nama}: Google API Gagal (Status: ${element.status})`, { category: 'dispatch' });
      }
  });

  if (results.length === 0) {
      await db.query(`UPDATE transaksi_panggilan SET status_panggilan = 'API_FAILED' WHERE id_panggilan = $1`, [newCallId]);
      throw new Error("Gagal mendapatkan hasil ETA dari Google.");
  }
  ```
- **Perilaku Faktual**:
  1. Jika satu atau sebagian elemen pada Distance Matrix berstatus selain `OK` (misalnya `ZERO_RESULTS` atau `NOT_FOUND`), kandidat tersebut **langsung dibuang** dari daftar hasil pemeringkatan, hanya dicatat sebagai peringatan di log, dan **TIDAK memiliki mekanisme fallback** ke urutan Haversine.
  2. Jika **seluruh** elemen gagal (`results.length === 0`), transaksi panggilan diperbarui di database menjadi `API_FAILED`, dan sistem melempar `Error` (proses dispatch dihentikan).

---

### D. Aturan Pengurutan Kandidat (Candidate Sorting)
- **Lokasi Kode**: [`service.js:908`](../service.js#L908)
  ```javascript
  results.sort((a, b) => a.etaSeconds - b.etaSeconds || a.distanceMeters - b.distanceMeters);
  ```
- **Perilaku Faktual**:
  1. Kriteria utama: `etaSeconds` terkecil (waktu tempuh Google Maps tercepat).
  2. Kriteria sekunder (tie-breaker): `distanceMeters` terpendek (jarak tempuh rute Google Maps).
  3. **Jarak Haversine TIDAK digunakan sebagai tie-breaker ketiga** pada kode ini. Jika ETA dan jarak rute sama persis, urutan ditentukan oleh urutan elemen array.

---

### E. Siklus Status Driver Saat Penawaran (Offer Flow)
- **Lokasi Kode Penawaran**: [`service.js:994-998`](../service.js#L994-L998)
  ```javascript
  // 2. Update DB: Set status driver menjadi 'BUSY'
  // await db.query(
  //     `UPDATE ambulans SET status_operasional = 'BUSY' WHERE id_ambulans = ?`,
  //     [bestDriver.id]
  // );
  ```
- **Lokasi Kode Konfirmasi Driver**: [`service.js:467-470`](../service.js#L467-L470)
  ```javascript
  // Update status driver menjadi Sibuk (busy)
  await db.query(
      `UPDATE ambulans SET status_operasional = 'BUSY' WHERE id_ambulans = $1`,
      [id_ambulans]
  );
  ```
- **Perilaku Faktual**:
  - Pembaruan status driver menjadi `BUSY` saat penawaran dikirim **sengaja dinonaktifkan (di-comment out)**.
  - Driver yang sedang diberi tawaran tugas tetap berstatus `AVAILABLE` di tabel `ambulans` (hanya transaksi panggilan yang mencatat `WAITING_FOR_DRIVER`).
  - Driver baru benar-benar diubah menjadi `BUSY` setelah mengirimkan respons konfirmasi terima (`status: 'diterima'`) melalui MQTT topik `ambulans/respons/konfirmasi`.
  - *Batasan Desain*: Jika dua panggilan darurat diproses dalam waktu yang sangat berdekatan, driver yang sama dapat berpotensi menerima penawaran ganda sebelum konfirmasi pertama diterima.

---

### F. Timeout Re-offer & Karakteristik Pesan
- **Lokasi Kode Timeout**: [`service.js:1015, 1075`](../service.js#L1015)
  ```javascript
  setTimeout(async () => {
      ...
  }, 20000); // 20000 ms = 20 detik
  ```
- **Perilaku Faktual**:
  1. Durasi timeout adalah **20 detik** (bersifat *hard-coded*).
  2. Jika driver tidak merespons dalam 20 detik, sistem mengirim interupsi pembatalan ke driver lama (`ambulans/interupsi/batal/${bestDriver.id}`), memasukkan driver ke daftar penolakan sementara panggilan tersebut (`rejectedDriversMap[callId]`), lalu mencari kandidat terbaik berikutnya (`nextBestDriver`).
  3. **Karakteristik Pesan Re-offer**:
     - Notifikasi re-offer ke driver berikutnya dikirim melalui fungsi `_assignDriverToCall()` ([`service.js:1063`](../service.js#L1063)).
     - Payload yang dikirim ke topik MQTT `ambulans/tugas/${nextBestDriver.id}` memiliki skema yang **identik** dengan penawaran pertama (hanya berisi `id_panggilan`, `id_pasien`, `nama_pasien`, koordinat, `eta_detik`, `kondisi`). Tidak ada flag pembeda seperti `is_reoffer`, `attempt_count`, atau timestamp percobaan.

---

## 2. Bagian Naskah Jurnal yang Harus Disesuaikan

Berdasarkan kesenjangan antara teks naskah dan kode sumber riil di atas, penulis wajib melakukan sinkronisasi pada bagian-bagian naskah berikut:

1. **Abstrak (Abstract)**:
   - Klarifikasi klaim estimasi waktu tempuh: jangan mengklaim penggunaan *live-traffic ETA* atau *duration in traffic*, melainkan estimasi rute jaringan jalan raya Google Maps (*standard driving duration*).
   - Sebutkan ukuran shortlist penyaringan awal secara eksplisit ($k = 5$).

2. **Section 2.2.2 (Distance Matrix API & Routing Formulation)**:
   - Hapus pernyataan bahwa request menyertakan `departure_time=now` dan membaca `duration_in_traffic`.
   - Jelaskan bahwa request menggunakan batch query satu arah dari titik insiden ke $k$ kandidat terpilih.
   - Perbaiki dokumentasi parameter pemanggilan API: jelaskan bahwa parameter moda yang dipakai adalah mode berkendara standar (`driving`).

3. **Algorithm 1 (Baris 34–36 - Handling Element Failure & Sorting)**:
   - **Kandidat Gagal**: Sesuaikan pseudocode agar mencerminkan bahwa elemen yang gagal (`status != OK`) dieksklusi dari himpunan hasil, dan sistem tidak melakukan fallback bertingkat ke nilai Haversine untuk kandidat tersebut.
   - **Semua Gagal**: Dokumentasikan bahwa jika seluruh $k$ elemen gagal, panggilan beralih ke status `API_FAILED`.
   - **Aturan Pengurutan**: Nyatakan dengan presisi bahwa tie-breaker hanya didasarkan pada `distanceMeters` dari Google Maps jika `etaSeconds` bernilai identik, tanpa tie-breaker tersier ke Haversine.

4. **Section 3.4 (Ambulance Availability State & Dispatch Concurrency)**:
   - Paparkan alur transisi status armada: status tetap `AVAILABLE` saat fase penawaran (`WAITING_FOR_DRIVER`) dengan batas tunggu konfirmasi 20 detik, dan baru beralih ke `BUSY` setelah konfirmasi `ACCEPTED`. Diskusikan hal ini sebagai batasan operasional (*operational design constraint*).

5. **Kesimpulan (Conclusion)**:
   - Sesuaikan ringkasan kontribusi agar selaras dengan implementasi deterministik berbasis Distance Matrix standar (non-live traffic) dengan shortlist $k = 5$.
