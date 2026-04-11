const db = require('./db.js');

async function getCols() {
    try {
        const res = await db.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'pasien'");
        console.log("Kolom tabel pasien:", res.rows);
    } catch(err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}
getCols();
