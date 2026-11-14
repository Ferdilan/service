# PANDUAN LANGKAH MANUAL: REMEDIASI KEAMANAN & PEMBERSIHAN DATA PRIBADI

Dokumen ini berisi panduan instruksi manual yang **wajib dijalankan oleh pemilik repositori** untuk mencabut kredensial yang pernah terekspos dan membersihkan riwayat git secara permanen.

> [!WARNING]
> Menghapus file dengan `git rm` di commit terbaru **belum** menghapus data rahasia dan data pribadi dari riwayat git (`git history`). Kredensial yang pernah ter-commit harus dianggap telah terkompromi dan wajib di-revoke (dicabut) segera.

---

## 1. Pencabutan & Rotasi Kredensial (Wajib Segera)

Semua kredensial berikut pernah tersimpan di git history dan harus diganti:

### A. Google Maps API Keys
1. Buka [Google Cloud Console - Credentials](https://console.cloud.google.com/apis/credentials).
2. Temukan dan **Hapus / Cabut (Revoke)** dua API Key yang pernah ada di repositori:
   - Key #1 (sebelumnya di `service_simulation.js`, awalan `AIza...pvbg`)
   - Key #2 (sebelumnya di `.env`, awalan `AIza...pUNg`)
3. Buat API Key baru untuk keperluan pengujian.
4. Terapkan pembatasan keamanan pada API Key baru:
   - **API restrictions**: Batasi hanya untuk *Distance Matrix API*.
   - **Application restrictions**: Batasi IP Address server VPS/pengujian jika memungkinkan.
5. Masukkan API Key baru ke file `.env` lokal Anda (jangan di-commit).

### B. MQTT Broker (CloudAMQP / Broker Publik)
1. Masuk ke dashboard CloudAMQP / penyedia broker MQTT Anda (`mustang.rmq.cloudamqp.com`).
2. Masuk ke tab **Users** atau **Security**.
3. Hapus user lama (`hjpp...`) atau ganti password untuk akun instance tersebut.
4. Perbarui connection string pada file `.env` lokal Anda.

### C. Neon PostgreSQL Credentials (Primary & Backup)
1. Buka [Neon Console](https://console.neon.tech/).
2. Untuk proyek database **Primary** (`ep-red-cloud-a1zb5ffb`):
   - Masuk ke menu **Roles / Users**.
   - Reset password role `neondb_owner`.
3. Untuk proyek database **Backup** (`ep-bitter-dream-ao3dkwpn`):
   - Masuk ke menu **Roles / Users**.
   - Reset password role `neondb_owner`.
4. Perbarui variabel `DATABASE_URL`, `DATABASE_DIRECT_URL`, `DATABASE_URL_BACKUP`, dan `DATABASE_DIRECT_URL_BACKUP` pada file `.env` lokal.

---

## 2. Pembersihan Riwayat Git Menggunakan `git-filter-repo`

Alat resmi yang direkomendasikan oleh GitHub dan tim pengembang Git untuk pembersihan riwayat adalah `git-filter-repo` (berbasis Python).

### Persiapan
Pastikan Python 3 dan `git-filter-repo` telah terpasang:
```bash
pip install git-filter-repo
```

### Langkah Eksekusi Pembersihan
> [!CAUTION]
> Buat salinan cadangan (backup) folder repositori sebelum menjalankan perintah di bawah ini!

Jalankan perintah berikut di terminal:

```bash
# 1. Pastikan working directory bersih
git status

# 2. Hapus file sensitif dan PII dari SELURUH riwayat git
git filter-repo --invert-paths \
  --path backup_full.sql \
  --path backup_schema.sql \
  --path exported_data.sql \
  --path service_simulation.js \
  --path test_publisher.js \
  --path .env

# 3. Verifikasi bahwa file-file tersebut sudah bersih dari seluruh commit
git log --all -- backup_full.sql
git log --all -- backup_schema.sql
git log --all -- exported_data.sql
git log --all -- service_simulation.js
git log --all -- test_publisher.js
git log --all -- .env
# (Semua perintah verifikasi di atas seharusnya tidak menghasilkan output apa pun)
```

---

## 3. Sinkronisasi ke Remote GitHub & Penanganan Cache

Setelah riwayat git lokal dibersihkan:

### A. Force Push ke Remote
Karena `git filter-repo` menulis ulang commit hash:
```bash
# Tambahkan kembali remote origin jika terlepas oleh filter-repo
git remote add origin https://github.com/Ferdilan/service.git

# Force-push seluruh branch dan tag yang telah dibersihkan
git push origin --force --all
git push origin --force --tags
```

### B. Hubungi GitHub Support untuk Pembersihan Commit Ref & Cache
GitHub menyimpan referensi dangling commit (cache PR, commit view via direct hash) selama beberapa waktu.
1. Hubungi [GitHub Support](https://support.github.com/contact) melalui tiket bantuan.
2. Mintalah tim GitHub untuk:
   - Menjalankan garbage collection manual (`git gc`) pada repositori `Ferdilan/service`.
   - Mengosongkan cache pull request / commit view dari referensi commit lama yang memuat file sensitif.
3. Periksa apakah repositori memiliki *fork* publik. Jika ada pihak ketiga yang telah mem-fork repositori sebelum pembersihan, minta pemilik fork untuk menghapus atau menyinkronkan ulang fork mereka.

---

## 4. Catatan Tambahan Terkait Repositori Terkait

- **Repositori Frontend / Mobile / Material**:
  Terdeteksi bahwa materi pengujian dan repositori pendukung aplikasi mobile mungkin juga menyimpan data seed dengan nama driver asli atau log koordinat.
  *Tindakan*: Lakukan audit mandiri pada repositori klien Android / frontend untuk memastikan tidak ada nama driver asli, data NIK, atau foto KTP uji yang tersimpan di aset atau riwayat commit repositori tersebut.
