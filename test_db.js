const db = require('./db.js');

async function testConnection() {
    console.log("🔄 Mencoba koneksi database...");
    try {
        const [rows] = await db.execute('SELECT 1 + 1 AS solution');
        console.log("✅ KONEKSI SUKSES! Hasil: ", rows[0].solution);
        process.exit(0);
    } catch (err) {
        console.error("❌ KONEKSI GAGAL:", err.message);
        console.error("Kode Error:", err.code);
        process.exit(1);
    }
}

testConnection();