START TRANSACTION;

SET SCHEMA 'musicbrainz';

CREATE EXTENSION IF NOT EXISTS musicbrainz_unaccent WITH SCHEMA musicbrainz;
CREATE EXTENSION IF NOT EXISTS musicbrainz_collate WITH SCHEMA musicbrainz;

-- create search configuration
CREATE TEXT SEARCH CONFIGURATION mb_simple (COPY = pg_catalog.simple);
ALTER TEXT SEARCH CONFIGURATION mb_simple
ALTER MAPPING FOR word, numword, hword, numhword, hword_part, hword_numpart
WITH musicbrainz_unaccentdict, simple;

-- add text searchable columns
ALTER TABLE artist ADD COLUMN ts_name TSVECTOR;
ALTER TABLE artist_alias ADD COLUMN ts_name TSVECTOR;
ALTER TABLE release_group ADD COLUMN ts_name TSVECTOR;

-- populate created columns
UPDATE artist
SET ts_name = to_tsvector('mb_simple', name);

UPDATE artist_alias
SET ts_name = to_tsvector('mb_simple', name);

UPDATE release_group
SET ts_name = to_tsvector('mb_simple', name);

-- create indexes
CREATE INDEX artist_ts_name_idx ON artist USING GIN (ts_name);
CREATE INDEX release_group_ts_name_idx ON release_group USING GIN (ts_name);

-- create triggers
CREATE TRIGGER artist_ts_update BEFORE INSERT OR UPDATE ON artist
FOR EACH ROW EXECUTE PROCEDURE tsvector_update_trigger('ts_name', 'musicbrainz.mb_simple', 'name');

CREATE TRIGGER artist_alias_ts_update BEFORE INSERT OR UPDATE ON artist_alias
FOR EACH ROW EXECUTE PROCEDURE tsvector_update_trigger('ts_name', 'musicbrainz.mb_simple', 'name');

CREATE TRIGGER release_group_ts_update BEFORE INSERT OR UPDATE ON release_group
FOR EACH ROW EXECUTE PROCEDURE tsvector_update_trigger('ts_name', 'musicbrainz.mb_simple', 'name');

COMMIT;
