-- Query untuk tabel data_provinsi
CREATE OR REPLACE TABLE
  kelompok_2.data_provinsi AS
SELECT
  JSON_VALUE(data_sekolah, '$.kode_prop') AS kode_prov,
  JSON_VALUE(data_sekolah, '$.propinsi') AS provinsi,
FROM
  `binar-bie7.kelompok_2_stg.data_sekolah`;