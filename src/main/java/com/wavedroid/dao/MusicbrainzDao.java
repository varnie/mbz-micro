package com.wavedroid.dao;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Dmitriy Khvatov (<i>dimax4@gmail.com</i>)
 * @version $Id$
 */
public class MusicbrainzDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(MusicbrainzDao.class);
    private static final int PAGE_SIZE = 100;

    private final PGSimpleDataSource dataSource;

    private static final MusicbrainzDao INSTANCE = new MusicbrainzDao();

    private static final String RELEASES_BY_ARTISTS = "SELECT\n" +
            "  rg_year  AS year,\n" +
            "  rg_month AS month,\n" +
            "  artist,\n" +
            "  release_name,\n" +
            "  total_tracks,\n" +
            "  artist_id,\n" +
            "  release_group_id\n" +
            "FROM (\n" +
            "       SELECT DISTINCT ON (release_group_id)\n" +
            "         sum(track_count)\n" +
            "         OVER (PARTITION BY release_id) total_tracks,\n" +
            "         *\n" +
            "       FROM (\n" +
            "              SELECT\n" +
            "                r.type,\n" +
            "                re.country,\n" +
            "                medium.track_count         AS track_count,\n" +
            "                medium.id                  AS medium_id,\n" +
            "                r.id                       AS release_group_id,\n" +
            "                rel.id                     AS release_id,\n" +
            "                a.id                       AS artist_id,\n" +
            "                a.name                     AS artist,\n" +
            "                r.name                     AS release_name,\n" +
            "                re.date_year               AS year,\n" +
            "                re.date_month              AS month,\n" +
            "                m.first_release_date_year  AS rg_year,\n" +
            "                m.first_release_date_month AS rg_month,\n" +
            "                a.rank                     AS rank\n" +
            "              FROM\n" +
            "                (SELECT\n" +
            "                   name,\n" +
            "                   id,\n" +
            "                   ts_rank_cd(ts_name, plainto_tsquery('mb_simple', ?), 2) rank\n" +
            "                 FROM artist\n" +
            "                 WHERE ts_name @@ plainto_tsquery('mb_simple', ?)\n" +
            "                 ORDER BY rank DESC\n" +
            "                ) AS a\n" +
            "                INNER JOIN artist_credit_name c ON a.id = c.artist_credit\n" +
            "                INNER JOIN release_group r ON c.artist_credit = r.artist_credit\n" +
            "                INNER JOIN release_group_meta m ON m.id = r.id\n" +
            "                INNER JOIN release rel ON rel.release_group = r.id\n" +
            "                INNER JOIN release_event re ON re.release = rel.id\n" +
            "                INNER JOIN medium ON medium.release = rel.id\n" +
            "                                     AND r.type = 1\n" +
            "                                     AND NOT exists(SELECT 1\n" +
            "                                                    FROM release_group_secondary_type_join j\n" +
            "                                                    WHERE j.release_group = r.id)\n" +
            "              ORDER BY rank DESC, year\n" +
            "            )\n" +
            "         AS tbl) AS tbl2\n" +
            "ORDER BY rank DESC, year\n";

    private static final String RELEASE_BY_ID = "\n" +
            "select rg_year as year, rg_month as month, artist, release_name, total_tracks, artist_id, release_group_id from (\n" +
            "select distinct on (release_group_id) \n" +
            "sum(track_count) over (partition by release_id) total_tracks, * from (\n" +
            "select r.type, re.country, medium.track_count as track_count, medium.id as medium_id, r.id as release_group_id, rel.id as release_id, \n" +
            "a.id as artist_id, a.name as artist, r.name as release_name, re.date_year as year, re.date_month as month, m.first_release_date_year as rg_year, m.first_release_date_month as rg_month from artist a\n" +
            "inner join artist_credit_name c on a.id = c.artist_credit\n" +
            "inner join release_group r on c.artist_credit = r.artist_credit\n" +
            "inner join release_group_meta m on m.id = r.id\n" +
            "inner join release rel on rel.release_group = r.id\n" +
            "inner join release_event re on re.release = rel.id\n" +
            "inner join medium on medium.release = rel.id\n" +
            "where r.id = ?\n" +
            ")\n" +
            "as tbl) as tbl2\n" +
            "order by artist, rg_year, rg_month\n";

    private static final String RELEASE_BY_NAME = "SELECT\n" +
            "  rg_year  AS year,\n" +
            "  rg_month AS month,\n" +
            "  artist,\n" +
            "  release_name,\n" +
            "  total_tracks,\n" +
            "  artist_id,\n" +
            "  release_group_id\n" +
            "FROM (\n" +
            "       SELECT DISTINCT ON (release_group_id)\n" +
            "         sum(track_count)\n" +
            "         OVER (PARTITION BY release_id) total_tracks,\n" +
            "         *\n" +
            "       FROM (\n" +
            "              SELECT\n" +
            "                r.type,\n" +
            "                re.country,\n" +
            "                medium.track_count         AS track_count,\n" +
            "                medium.id                  AS medium_id,\n" +
            "                r.id                       AS release_group_id,\n" +
            "                rel.id                     AS release_id,\n" +
            "                a.id                       AS artist_id,\n" +
            "                a.name                     AS artist,\n" +
            "                r.name                     AS release_name,\n" +
            "                re.date_year               AS year,\n" +
            "                re.date_month              AS month,\n" +
            "                m.first_release_date_year  AS rg_year,\n" +
            "                m.first_release_date_month AS rg_month,\n" +
            "                r.rank                     AS rank\n" +
            "              FROM\n" +
            "                (SELECT\n" +
            "                   name,\n" +
            "                   type,\n" +
            "                   id,\n" +
            "                   artist_credit,\n" +
            "                   ts_rank_cd(ts_name, plainto_tsquery('mb_simple', ?), 2) rank\n" +
            "                 FROM release_group\n" +
            "                 WHERE ts_name @@ plainto_tsquery('mb_simple', ?)\n" +
            "                 ORDER BY rank DESC\n" +
            "                ) AS r\n" +
            "                INNER JOIN artist_credit_name c ON r.artist_credit = c.artist_credit\n" +
            "                INNER JOIN artist a ON a.id = c.artist_credit\n" +
            "                INNER JOIN release_group_meta m ON m.id = r.id\n" +
            "                INNER JOIN release rel ON rel.release_group = r.id\n" +
            "                INNER JOIN release_event re ON re.release = rel.id\n" +
            "                INNER JOIN medium ON medium.release = rel.id\n" +
            "                                     AND r.type = 1\n" +
            "                                     AND NOT exists(SELECT 1\n" +
            "                                                    FROM release_group_secondary_type_join j\n" +
            "                                                    WHERE j.release_group = r.id)\n" +
            "            )\n" +
            "         AS tbl) AS tbl2\n" +
            "ORDER BY rank DESC, year\n";

    private static final String RELEASE_BY_ARTIST = "select rg_year as year, rg_month as month, artist, release_name, total_tracks, artist_id, release_group_id from (\n" +
            "select distinct on (release_group_id) \n" +
            "sum(track_count) over (partition by release_id) total_tracks, * from (\n" +
            "select r.type, re.country, medium.track_count as track_count, medium.id as medium_id, r.id as release_group_id, rel.id as release_id, \n" +
            "a.id as artist_id, a.name as artist, r.name as release_name, re.date_year as year, re.date_month as month, m.first_release_date_year as rg_year, m.first_release_date_month as rg_month from artist a\n" +
            "inner join artist_credit_name c on a.id = c.artist_credit\n" +
            "inner join release_group r on c.artist_credit = r.artist_credit\n" +
            "inner join release_group_meta m on m.id = r.id\n" +
            "inner join release rel on rel.release_group = r.id\n" +
            "inner join release_event re on re.release = rel.id\n" +
            "inner join medium on medium.release = rel.id\n" +
            "where a.id = ?\n" +
            "and r.type = 1\n" +
            "and not exists (select 1 from release_group_secondary_type_join j where j.release_group = r.id)\n" +
            ")\n" +
            "as tbl) as tbl2\n" +
            "order by year, month\n";

    private static final String TRACKLIST_BY_RELEASE_ID = "\n" +
            "select t.id as track_id, t.name as title, t.length as length, t.position as position, tbl2.disc_number as disc_number from (\n" +
            "select distinct on (release_group_id) \n" +
            "sum(track_count) over (partition by release_id) total_tracks, * from (\n" +
            "select m.track_count as track_count, m.id as medium_id, m.position as disc_number, r.id as release_group_id, rel.id as release_id from artist a\n" +
            "inner join artist_credit_name c on a.id = c.artist_credit\n" +
            "inner join release_group r on c.artist_credit = r.artist_credit\n" +
            "inner join release rel on rel.release_group = r.id\n" +
            "inner join medium m on m.release = rel.id\n" +
            "where r.id = ?) as tbl) as tbl2\n" +
            "inner join track t on t.medium = medium_id\n" +
            "order by disc_number, t.position ";

    private MusicbrainzDao() {
        try {
            dataSource = new PGSimpleDataSource();
            dataSource.setDatabaseName("musicbrainz_db");
            dataSource.setUrl("jdbc:postgresql://localhost:5432/musicbrainz_db");
            dataSource.setUser("musicbrainz");
            dataSource.setPassword("musicbrainz");
            dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("FATAL: Unable to connect to musicbrainz database.", e);
        }
    }

    public static List<Map<String, Object>> getReleasesByArtists(String artist, int page) {
        return getEntitiesFromResultSet(getResultSet(RELEASES_BY_ARTISTS, page, artist, artist).orNull());
    }

    public static Map<String, Object> getReleaseById(long id) {
        try {
            Optional<ResultSet> rs = getResultSet(RELEASE_BY_ID, 0, id);
            if (rs.isPresent()) {
                if (rs.get().next()) {
                    return getEntityFromResultSet(rs.get());
                } else {
                    return Maps.newHashMap();
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Error querying release by id", e);
        }
        return Maps.newHashMap();
    }

    public static List<Map<String, Object>> getReleasesByName(String name, int page) {
        return getEntitiesFromResultSet(getResultSet(RELEASE_BY_NAME, page, name, name).orNull());
    }

    public static List<Map<String, Object>> getReleasesByArtist(long artistId, int page) {
        return getEntitiesFromResultSet(getResultSet(RELEASE_BY_ARTIST, page, artistId).orNull());
    }

    public static List<Map<String, Object>> getTracklist(long releaseId, int page) {
        return getEntitiesFromResultSet(getResultSet(TRACKLIST_BY_RELEASE_ID, page, releaseId).orNull());
    }


    private static Optional<Connection> getConnection() {
        try {
            return Optional.of(INSTANCE.dataSource.getConnection());
        } catch (SQLException e) {
            LOGGER.error("Failed to connect to musicbrainz database.");
            return Optional.absent();
        }
    }

    private static Optional<ResultSet> getResultSet(String query, int page, Object... params) {
        Optional<Connection> optionalConn = getConnection();
        if (optionalConn.isPresent()) {
            Connection conn = optionalConn.get();
            try {
                PreparedStatement ps = conn.prepareStatement(query + " limit ? offset ? ");
                int index = 0;
                for (Object param : params) {
                    index++;
                    if (param instanceof String) {
                        ps.setString(index, (String) param);
                    }
                    if (param instanceof Integer) {
                        ps.setInt(index, (Integer) param);
                    }
                    if (param instanceof Long) {
                        ps.setLong(index, (Long) param);
                    }
                }
                ps.setInt(++index, PAGE_SIZE);
                ps.setInt(++index, PAGE_SIZE * page);
                return Optional.of(ps.executeQuery());
            } catch (SQLException e) {
                LOGGER.error("Error executing query", e);
                return Optional.absent();
            }
        } else {
            return Optional.absent();
        }
    }

    private static List<Map<String, Object>> getEntitiesFromResultSet(ResultSet resultSet) {
        ArrayList<Map<String, Object>> entities = new ArrayList<>();
        if (resultSet == null) {
            return entities;
        }
        try {
            while (resultSet.next()) {
                entities.add(getEntityFromResultSet(resultSet));
            }
        } catch (SQLException e) {
            LOGGER.error("Error processing resultSet", e);
        }
        return entities;
    }

    private static Map<String, Object> getEntityFromResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        Map<String, Object> resultsMap = new HashMap<>();
        for (int i = 1; i <= columnCount; ++i) {
            String columnName = metaData.getColumnName(i).toLowerCase();
            Object object = resultSet.getObject(i);
            resultsMap.put(columnName, object);
        }
        return resultsMap;
    }

}
