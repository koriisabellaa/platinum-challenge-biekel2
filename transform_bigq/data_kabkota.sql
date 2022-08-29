-- Query untuk tabel data_kabkota
CREATE OR REPLACE TABLE
  kelompok_2.data_kabkota AS
SELECT
  JSON_VALUE(data_sekolah, '$.kode_kab_kota') AS kode_kabkota,
  JSON_VALUE(data_sekolah, '$.kabupaten_kota') AS kabupaten_kota,
  JSON_VALUE(data_sekolah, '$.kode_kec') AS kode_kecamatan,
  JSON_VALUE(data_sekolah, '$.kecamatan') AS kecamatan,
FROM
  `binar-bie7.kelompok_2_stg.data_sekolah`;