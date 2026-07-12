// rebuild_backup.js
const fs = require('fs');

const schema = fs.readFileSync('backup_schema.sql', 'utf8');
const data = fs.readFileSync('exported_data.sql', 'utf8');

const cutStart = schema.indexOf('-- ============================================================\n-- BAGIAN 6:');
const cutEnd = schema.indexOf('\n-- ============================================================\n-- BAGIAN 7:');

const part1 = schema.substring(0, cutStart);
const part2 = schema.substring(cutEnd);

let final = part1 +
'-- ============================================================\n' +
'-- BAGIAN 6: DATA REAL DARI NEON PRIMARY\n' +
'-- Diekspor pada: ' + new Date().toISOString() + '\n' +
'-- ============================================================\n\n' +
data + '\n' +
part2;

// Pastikan enum sudah include lowercase variants
const OLD_ENUM = "CREATE TYPE enum_ambulans_status_operasional AS ENUM ('AVAILABLE', 'BUSY', 'OFFLINE');";
const NEW_ENUM = "CREATE TYPE enum_ambulans_status_operasional AS ENUM ('AVAILABLE', 'BUSY', 'OFFLINE', 'available', 'busy', 'offline');";
final = final.replace(OLD_ENUM, NEW_ENUM);

fs.writeFileSync('backup_full.sql', final, 'utf8');
const lines = final.split('\n').length;
console.log('SUKSES! backup_full.sql diperbarui - ' + lines + ' baris');

// Cek apakah masih ada lowercase di INSERT
const matches = final.match(/'offline'\)/g) || [];
console.log('Sisa nilai lowercase di INSERT: ' + matches.length + ' (seharusnya 0)');
