#!/usr/bin/env bash

{

    echo Starting fullexport. &&

    FULLEXPORT=http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport &&
    DATA_DIR=/data2 &&

    cd ${DATA_DIR} &&

    rm -f LATEST &&
    wget ${FULLEXPORT}/LATEST &&
    LATEST=`cat LATEST` &&
    echo Latest dump is "$LATEST" &&

    echo getting mbdump.tar.bz2 &&
    wget ${FULLEXPORT}/${LATEST}/mbdump.tar.bz2 &&
    echo getting mbdump-derived.tar.bz2 &&
    wget ${FULLEXPORT}/${LATEST}/mbdump-derived.tar.bz2 &&

    echo creating database &&
    createdb -p 5433 -l C -E UTF-8 -T template0 -O musicbrainz musicbrainz_new &&

    cd /home/musicbrainz/mbslave &&

    echo creating schema musicbrainz &&
    echo 'CREATE SCHEMA musicbrainz;' | ./mbslave-psql.py -S &&

    echo creating tables &&
    ./mbslave-remap-schema.py <sql/CreateTables.sql | sed 's/CUBE/TEXT/' | ./mbslave-psql.py &&

    echo importing data &&
    ./mbslave-import.py ${DATA_DIR}/mbdump.tar.bz2 ${DATA_DIR}/mbdump-derived.tar.bz2 &&

    echo creating primary keys &&
    ./mbslave-remap-schema.py <sql/CreatePrimaryKeys.sql | ./mbslave-psql.py &&
    echo creating indexes &&
    ./mbslave-remap-schema.py <sql/CreateIndexes.sql | grep -vE '(collate|medium_index)' | ./mbslave-psql.py &&
    echo creating views &&
    ./mbslave-remap-schema.py <sql/CreateViews.sql | ./mbslave-psql.py &&

    echo executing vacuum analyze &&
    echo 'VACUUM ANALYZE;' | ./mbslave-psql.py &&

    echo Done.


} || {

    echo Caught exception!
    exit 1

}

