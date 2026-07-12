-- ============================================================
--  AMBULANCE SYSTEM - BACKUP DATABASE SETUP SCRIPT
--  Untuk: Neon PostgreSQL (Akun Cadangan)
--  Jalankan script ini di SQL Editor Neon dashboard
--  pada akun Neon BACKUP Anda.
-- ============================================================


-- ============================================================
-- BAGIAN 1: HAPUS OBJEK LAMA (Safe Reset)
-- Urutan DROP harus terbalik dari urutan CREATE (dependensi dulu)
-- ============================================================

DROP TRIGGER IF EXISTS trg_notify_status_ambulans ON ambulans;
DROP FUNCTION IF EXISTS fn_notify_status_ambulans();
DROP TABLE IF EXISTS user_sessions CASCADE;
DROP TABLE IF EXISTS transaksi_panggilan CASCADE;
DROP TABLE IF EXISTS pasien CASCADE;
DROP TABLE IF EXISTS ambulans CASCADE;
DROP TYPE IF EXISTS enum_ambulans_kategori_armada;
DROP TYPE IF EXISTS enum_ambulans_status_akun;
DROP TYPE IF EXISTS enum_ambulans_status_operasional;
DROP TYPE IF EXISTS enum_pasien_jenis_kelamin;
DROP TYPE IF EXISTS enum_transaksi_panggilan_status_panggilan;


-- ============================================================
-- BAGIAN 2: BUAT TIPE ENUM
-- ============================================================

CREATE TYPE enum_ambulans_kategori_armada AS ENUM ('PSC', 'Relawan');
CREATE TYPE enum_ambulans_status_akun AS ENUM ('pending', 'active', 'rejected');
CREATE TYPE enum_ambulans_status_operasional AS ENUM ('AVAILABLE', 'BUSY', 'OFFLINE');
CREATE TYPE enum_pasien_jenis_kelamin AS ENUM ('Laki-laki', 'Perempuan');
CREATE TYPE enum_transaksi_panggilan_status_panggilan AS ENUM (
    'PENDING',
    'WAITING_FOR_DRIVER',
    'ON_THE_WAY',
    'COMPLETED',
    'CANCELLED',
    'NO_DRIVERS',
    'NO_DRIVERS_AVAILABLE',
    'FAILED',
    'API_FAILED'
);


-- ============================================================
-- BAGIAN 3: BUAT TABEL UTAMA
-- ============================================================

-- ------------------------------------------------------------
-- Tabel: ambulans
-- Menyimpan data driver/armada ambulans
-- ------------------------------------------------------------
CREATE TABLE ambulans (
    id_ambulans         SERIAL          PRIMARY KEY,
    nomor_polisi        VARCHAR(255)    NOT NULL,
    nama_driver         VARCHAR(255)    NOT NULL,
    nik                 VARCHAR(16)     NOT NULL,
    email               VARCHAR(255)    NOT NULL UNIQUE,
    password            VARCHAR(255)    NOT NULL,
    nomor_telepon       VARCHAR(255)    NOT NULL,
    instansi            VARCHAR(255),
    kategori_armada     enum_ambulans_kategori_armada   NOT NULL,
    foto_url            VARCHAR(255),
    status_akun         enum_ambulans_status_akun       NOT NULL DEFAULT 'pending',
    status_operasional  enum_ambulans_status_operasional NOT NULL DEFAULT 'OFFLINE',
    lokasi_latitude     DECIMAL(10, 8),
    lokasi_longitude    DECIMAL(11, 8),
    timestamp_update    TIMESTAMPTZ     DEFAULT NOW(),
    "createdAt"         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    "updatedAt"         TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Tabel: pasien
-- Menyimpan data pengguna/pasien
-- ------------------------------------------------------------
CREATE TABLE pasien (
    id_pasien       SERIAL          PRIMARY KEY,
    nik             VARCHAR(255)    NOT NULL UNIQUE,
    nama            VARCHAR(255)    NOT NULL,
    tgl_lahir       VARCHAR(255)    NOT NULL,
    jenis_kelamin   enum_pasien_jenis_kelamin NOT NULL,
    alamat          TEXT,
    foto_ktp        VARCHAR(255)    NOT NULL,
    password        VARCHAR(255)    NOT NULL,
    createdat       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updatedat       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Tabel: transaksi_panggilan
-- Menyimpan riwayat seluruh panggilan ambulans
-- ------------------------------------------------------------
CREATE TABLE transaksi_panggilan (
    id_panggilan        SERIAL          PRIMARY KEY,
    id_pasien           INTEGER         NOT NULL
                            REFERENCES pasien(id_pasien)
                            ON UPDATE CASCADE ON DELETE RESTRICT,
    id_ambulans_respons INTEGER
                            REFERENCES ambulans(id_ambulans)
                            ON UPDATE CASCADE ON DELETE SET NULL,
    jenis_layanan       VARCHAR(255)    NOT NULL,
    kondisi             VARCHAR(255),
    lokasi_pasien_lat   DOUBLE PRECISION NOT NULL,
    lokasi_pasien_lon   DOUBLE PRECISION NOT NULL,
    waktu_panggilan     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    waktu_selesai       TIMESTAMPTZ,
    status_panggilan    enum_transaksi_panggilan_status_panggilan NOT NULL DEFAULT 'PENDING',
    createdat           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updatedat           TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Tabel: user_sessions
-- Dikelola otomatis oleh connect-pg-simple (session store Express)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_sessions (
    sid     VARCHAR         NOT NULL    PRIMARY KEY,
    sess    JSON            NOT NULL,
    expire  TIMESTAMPTZ(6)  NOT NULL
);
CREATE INDEX IF NOT EXISTS IDX_session_expire ON user_sessions (expire);


-- ============================================================
-- BAGIAN 4: INDEX PERFORMA
-- Mempercepat query yang sering dipakai oleh service.js
-- ============================================================

-- Untuk query pencarian driver AVAILABLE dengan filter kategori
CREATE INDEX idx_ambulans_status_kategori
    ON ambulans (status_operasional, kategori_armada);

-- Untuk query riwayat panggilan berdasarkan pasien
CREATE INDEX idx_transaksi_id_pasien
    ON transaksi_panggilan (id_pasien);

-- Untuk query riwayat panggilan berdasarkan driver
CREATE INDEX idx_transaksi_id_ambulans
    ON transaksi_panggilan (id_ambulans_respons);

-- Untuk query filter status panggilan
CREATE INDEX idx_transaksi_status
    ON transaksi_panggilan (status_panggilan);


-- ============================================================
-- BAGIAN 5: FUNGSI & TRIGGER POSTGRES LISTEN/NOTIFY
-- Digunakan oleh service.js untuk sinkronisasi status real-time
-- ============================================================

CREATE OR REPLACE FUNCTION fn_notify_status_ambulans()
RETURNS TRIGGER AS $$
DECLARE
    payload JSON;
BEGIN
    -- Hanya kirim notifikasi jika status_operasional berubah
    IF OLD.status_operasional IS DISTINCT FROM NEW.status_operasional THEN
        payload = json_build_object(
            'id_ambulans',  NEW.id_ambulans,
            'status_lama',  OLD.status_operasional,
            'status_baru',  NEW.status_operasional,
            'timestamp',    NOW()
        );
        PERFORM pg_notify('status_ambulans_channel', payload::TEXT);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notify_status_ambulans
    AFTER UPDATE ON ambulans
    FOR EACH ROW
    EXECUTE FUNCTION fn_notify_status_ambulans();


-- ============================================================
-- BAGIAN 6: DATA SEED (Driver & Pasien)
-- Password default semua akun: 123456
-- Catatan: Hash di bawah adalah bcrypt hash dari '123456'
--          Jika tidak cocok, generate ulang via seed.js
-- ============================================================

-- PENTING: Sesuaikan hash berikut dengan yang ada di database primary Anda.
-- Cara mendapatkan hash: jalankan di Node.js:
--   const bcrypt = require('bcryptjs');
--   bcrypt.hash('123456', 10).then(h => console.log(h));
-- Lalu ganti nilai '$2a$10$PLACEHOLDER_HASH' di bawah dengan hasilnya.

DO $$
DECLARE
    pwd_hash TEXT := '$2b$10$W66g1ZcvRYrGnWUg7LrLLuQGJM3OlktzFWyYCi2RwO7WwiTD13AsG';
BEGIN

    -- ---- DATA AMBULANS - PSC ----
    INSERT INTO ambulans (
        id_ambulans, nomor_polisi, nama_driver, nik, email, password,
        nomor_telepon, instansi, kategori_armada, status_akun,
        status_operasional, lokasi_latitude, lokasi_longitude,
        "createdAt", "updatedAt"
    ) VALUES
        (101, 'N 9991 AA', 'Agus Prasetyo',    '3573010101000001', 'agus.psc@rsudsa.id',      pwd_hash, '08120000101', 'RSUD Dr. Saiful Anwar',  'PSC',     'active', 'AVAILABLE', -7.965801, 112.624512, NOW(), NOW()),
        (102, 'N 8812 KJ', 'Bagus Herlambang', '3573010101000002', 'bagus.psc@rsudkota.id',   pwd_hash, '08120000102', 'RSUD Kota Malang',        'PSC',     'active', 'AVAILABLE', -7.972100, 112.633900, NOW(), NOW()),
        (103, 'N 7711 GR', 'Suyitno',          '3573010101000003', 'suyitno.psc@rssalma.id',  pwd_hash, '08120000103', 'RS Islam UNISMA',         'PSC',     'active', 'AVAILABLE', -7.952900, 112.615500, NOW(), NOW()),
        (104, 'N 5521 HH', 'Dedi Ramadhan',    '3573010101000004', 'dedi.psc@rssifa.id',      pwd_hash, '08120000104', 'RS Lavalette',            'PSC',     'active', 'AVAILABLE', -7.977200, 112.647200, NOW(), NOW()),
        (105, 'N 4421 MQ', 'M. Rizal',         '3573010101000005', 'rizal.psc@aulsari.id',    pwd_hash, '08120000105', 'RS Panti Nirmala',        'PSC',     'active', 'AVAILABLE', -7.984000, 112.630100, NOW(), NOW()),

    -- ---- DATA AMBULANS - RELAWAN ----
        (201, 'N 8831 QW', 'Roni Saputra',       '3573020202000001', 'roni.relawan@yki.id',     pwd_hash, '08130000201', 'Yayasan Kemanusiaan Indonesia', 'Relawan', 'active', 'AVAILABLE', -7.990210, 112.620500, NOW(), NOW()),
        (202, 'N 2322 WW', 'Febriansyah',         '3573020202000002', 'febri.relawan@bass.id',   pwd_hash, '08130000202', 'Barisan Ambulance Sosial',      'Relawan', 'active', 'AVAILABLE', -7.987100, 112.629200, NOW(), NOW()),
        (203, 'N 6621 PJ', 'Samsul Huda',         '3573020202000003', 'samsul.relawan@sahabat.id', pwd_hash, '08130000203', 'Ambulans Sahabat Malang',    'Relawan', 'active', 'AVAILABLE', -7.998001, 112.633100, NOW(), NOW()),
        (204, 'N 3341 PL', 'Andri Firmansyah',    '3573020202000004', 'andri.relawan@yamas.id',  pwd_hash, '08130000204', 'Yayasan Malang Sehat',          'Relawan', 'active', 'AVAILABLE', -7.981000, 112.612001, NOW(), NOW()),
        (205, 'N 9121 SD', 'Adi Gunawan',         '3573020202000005', 'adi.relawan@bsm.id',      pwd_hash, '08130000205', 'Brigade Sosial Malang',         'Relawan', 'active', 'AVAILABLE', -7.970200, 112.642220, NOW(), NOW())
    ON CONFLICT (id_ambulans) DO NOTHING;

    -- Sync sequence agar SERIAL tidak bentrok dengan data yang diinsert manual
    PERFORM setval('ambulans_id_ambulans_seq', (SELECT MAX(id_ambulans) FROM ambulans));

    -- ---- DATA PASIEN ----
    INSERT INTO pasien (nik, nama, tgl_lahir, jenis_kelamin, alamat, foto_ktp, password)
    VALUES
        ('3573011503850003', 'Budi Santoso',  '1985-03-15', 'Laki-laki',  'Jl. Soekarno Hatta No. 3, Malang', 'dummy_ktp_budi.jpg',  pwd_hash),
        ('3573012504880004', 'Dewi Lestari',  '1988-04-25', 'Perempuan',  'Jl. Veteran No. 4, Malang',         'dummy_ktp_dewi.jpg',  pwd_hash),
        ('3573011001900001', 'Ahmad Fauzi',   '1990-01-10', 'Laki-laki',  'Jl. Kawi No. 1, Malang',            'dummy_ktp_ahmad.jpg', pwd_hash),
        ('3573012002910002', 'Siti Rahayu',   '1991-02-20', 'Perempuan',  'Jl. Semeru No. 2, Malang',          'dummy_ktp_siti.jpg',  pwd_hash)
    ON CONFLICT (nik) DO NOTHING;

END $$;


-- ============================================================
-- BAGIAN 7: VERIFIKASI (Opsional - Jalankan untuk cek)
-- ============================================================

-- Uncomment baris di bawah ini untuk memverifikasi hasil:
-- SELECT 'ambulans' AS tabel, COUNT(*) AS jumlah FROM ambulans
-- UNION ALL
-- SELECT 'pasien', COUNT(*) FROM pasien
-- UNION ALL
-- SELECT 'transaksi_panggilan', COUNT(*) FROM transaksi_panggilan;

-- SELECT tgname AS trigger_name, relname AS table_name
-- FROM pg_trigger
-- JOIN pg_class ON pg_trigger.tgrelid = pg_class.oid
-- WHERE NOT tgisinternal;
