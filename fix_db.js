const db = require('./db.js');

async function fixColumn() {
    try {
        console.log("Melakukan ALTER TABLE untuk memperlebar kolom status_panggilan...");
        await db.query(`ALTER TABLE transaksi_panggilan ALTER COLUMN status_panggilan TYPE VARCHAR(50)`);
        console.log("SUKSES: Tipe data status_panggilan berhasil diubah menjadi VARCHAR(50).");
    } catch (err) {
        console.error("GAGAL: ", err.message);
    } finally {
        process.exit(0);
    }
}

fixColumn();
