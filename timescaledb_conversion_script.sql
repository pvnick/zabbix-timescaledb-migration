SELECT 'Start - ' || now();

/* helper function to convert intervals and timestamps to integer of seconds. time/interval should be provided as an argument */
CREATE OR REPLACE FUNCTION pg_temp.time_to_int(t anyelement) RETURNS integer AS $$
  BEGIN
    RETURN CAST(EXTRACT(epoch FROM t) AS INTEGER);
  END;
$$ LANGUAGE plpgsql;

/* define a function which will duplicate data coming into one of our history tables on to a temp table.
   target table should be provided as an argument */
CREATE OR REPLACE FUNCTION duplicate_rows() RETURNS TRIGGER AS $$
  BEGIN
    /* add new incoming data to a temp table to be appended later */
    EXECUTE 'INSERT INTO ' || TG_ARGV[0]::text || '(itemid, clock, value, ns) VALUES ($1.itemid, $1.clock, $1.value, $1.ns)' USING NEW;
    RETURN NEW;
  END;
$$ LANGUAGE plpgsql;

/* temporary store for bulk insert start times */
CREATE TEMPORARY TABLE start_times (table_name TEXT PRIMARY KEY, value timestamptz);
CREATE FUNCTION pg_temp.start_time (table_name TEXT) RETURNS timestamptz AS $$
  SELECT value FROM start_times WHERE table_name = table_name;
$$ LANGUAGE SQL;

/* history_uint_tmp receives new imcoming data as its written to history_uint so that it can be combined
   with history_uint_new after the bulk insert is finished */
SELECT 'Adding temp table which receives new imcoming data - ' || now();
DROP TABLE IF EXISTS history_uint_tmp;
CREATE TABLE history_uint_tmp (LIKE history_uint);
CREATE OR REPLACE TRIGGER dup_data AFTER INSERT ON history_uint FOR EACH ROW EXECUTE FUNCTION duplicate_rows('history_uint_tmp');
INSERT INTO start_times (table_name, value) VALUES ('history_uint_tmp', now());

SELECT 'Starting bulk insert - ' || now();
/* the following code sets up a new timescaledb hyptertable with compression enabled and bulk inserts existing data in compressed chunks.
   in production, we will have one such transaction for all relevant history/trend tables. */
DROP TABLE IF EXISTS history_uint_new;
/* history_uint_new gets created as a timescaledb table and we bulk insert history_uint data into it */
CREATE TABLE history_uint_new (LIKE history_uint);
/* set up a new timescaledb table with compression and daily partitioning */
SELECT create_hypertable('history_uint_new', 'clock', chunk_time_interval => pg_temp.time_to_int(interval '1 day'));
ALTER TABLE history_uint_new SET (
  timescaledb.compress,
  timescaledb.compress_segmentby='itemid',
  timescaledb.compress_orderby='clock,ns'); /* note to self: compress_orderby is different from trand tables */

/* bulk insert data in compressed chunks to the new hypertable */
SELECT 'Inserting first chunk - ' || now();
INSERT INTO history_uint_new SELECT * FROM history_uint
  WHERE clock >= pg_temp.time_to_int(pg_temp.start_time('history_uint_tmp') - interval '4 weeks')
  AND clock < pg_temp.time_to_int(pg_temp.start_time('history_uint_tmp') - interval '3 weeks');
SELECT 'First chunk inserted, starting compression - ' || now();
SELECT compress_chunk(i, if_not_compressed => true)
  FROM show_chunks('history_uint_new', older_than => pg_temp.time_to_int(
    pg_temp.start_time('history_uint_tmp') - interval '3 weeks 1 day')) i;

SELECT 'Inserting second chunk - ' || now();
INSERT INTO history_uint_new SELECT * FROM history_uint
  WHERE clock >= pg_temp.time_to_int(pg_temp.start_time('history_uint_tmp') - interval '3 weeks')
  AND clock < pg_temp.time_to_int(pg_temp.start_time('history_uint_tmp') - interval '2 weeks');
SELECT 'Second chunk inserted, starting compression - ' || now();
SELECT compress_chunk(i, if_not_compressed => true)
  FROM show_chunks('history_uint_new', older_than => pg_temp.time_to_int(
    pg_temp.start_time('history_uint_tmp') - interval '2 weeks 1 day')) i;

SELECT 'Inserting third chunk - ' || now();
INSERT INTO history_uint_new SELECT * FROM history_uint
  WHERE clock >= pg_temp.time_to_int(pg_temp.start_time('history_uint_tmp') - interval '2 weeks')
  AND clock < pg_temp.time_to_int(pg_temp.start_time('history_uint_tmp') - interval '1 weeks');
SELECT 'Third chunk inserted, starting compression - ' || now();
SELECT compress_chunk(i, if_not_compressed => true)
  FROM show_chunks('history_uint_new', older_than => pg_temp.time_to_int(
    pg_temp.start_time('history_uint_tmp') - interval '1 week 1 day')) i;

SELECT 'Inserting forth chunk - ' || now();
INSERT INTO history_uint_new SELECT * FROM history_uint
  WHERE clock >= pg_temp.time_to_int(pg_temp.start_time('history_uint_tmp') - interval '1 weeks')
  AND clock < pg_temp.time_to_int(pg_temp.start_time('history_uint_tmp'));
SELECT 'Bulk insert finished - ' || now();

/* add the index at the end since its faster to do it that way */
SELECT 'All chunks inserted. Building index - ' || now();
CREATE INDEX ON history_uint_new (itemid, clock);

/* once all of the tables are finished in the previous transactions, we will run the following transaction and include all tables
   to swap the original tables and the new tables, and then append the imcoming data that arrived during bulk insert and was written
   to the *_tmp tables. this will temporarily introduce a gap in all metric tables until the *_tmp tables can be appended
   to the new timescaledb tables. this doesn't have to run immediately after the previous transaction, since we will have two new
   sets of tables, *_new and *_tmp. *_new will have most of the existing history data in new compressed timescaledb tables, and
   *_tmp will be a set of growing tables actively receiving imcoming data. */
BEGIN;
  /* swap the original table with the new timescaledb table */
  ALTER TABLE history_uint RENAME TO history_uint_old;
  ALTER TABLE history_uint_new RENAME TO history_uint;
  /* enable timescaledb on zabbix */
  UPDATE config SET db_extension='timescaledb',hk_history_global=1,hk_trends_global=1;
  UPDATE config SET compression_status=1,compress_older='7d';
COMMIT;

/* append the data that came in during bulk insert */
SELECT 'Appending temp data received during bulk insert to the end of the new table - ' || now();
INSERT INTO history_uint SELECT * FROM history_uint_tmp;
DROP TABLE history_uint_tmp;

/* once those are finished and after testing, a set of "drop table *_old" queries can be run to drop the old history tables */
SELECT 'Finish - ' || now();
