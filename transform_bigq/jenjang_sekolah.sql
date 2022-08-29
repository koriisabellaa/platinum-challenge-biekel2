  -- Query untuk tabel jenjang_sekolah
CREATE OR REPLACE TABLE
  kelompok_2.jenjang_sekolah AS
SELECT
  JSON_VALUE(data_sekolah, '$.id') AS id,
  JSON_VALUE(data_sekolah, '$.kode_prop') AS kode_prov,
  JSON_VALUE(data_sekolah, '$.kode_kab_kota') AS kode_kabkota,
  JSON_VALUE(data_sekolah, '$.kode_kec') AS kode_kecamatan,
  JSON_VALUE(data_sekolah, '$.npsn') AS npsn,
  JSON_VALUE(data_sekolah, '$.sekolah') AS nama_sekolah,
  JSON_VALUE(data_sekolah, '$.bentuk') AS jenjang,
  JSON_VALUE(data_sekolah, '$.status') AS status,
  JSON_VALUE(data_sekolah, '$.alamat_jalan') AS alamat_jalan,
  JSON_VALUE(data_sekolah, '$.lintang') AS lintang,
  JSON_VALUE(data_sekolah, '$.bujur') AS bujur
FROM
  `binar-bie7.kelompok_2_stg.data_sekolah`;