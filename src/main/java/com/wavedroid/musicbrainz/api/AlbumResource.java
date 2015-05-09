package com.wavedroid.musicbrainz.api;

import com.wavedroid.musicbrainz.dao.MusicbrainzDao;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;

/**
 * @author Dmitriy Khvatov (<i>dimax4@gmail.com</i>)
 * @version $Id$
 */
@Path("/release")
@Produces(MediaType.APPLICATION_JSON)
public class AlbumResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlbumResource.class);

    private static final ObjectMapper om = new ObjectMapper();

    @GET
    @Timed
    @Path("/artistName/{artist}")
    public String releasesByArtists(@PathParam("artist") String artistName, @QueryParam("page") Optional<Integer> page) {
        List<Map<String, Object>> map = MusicbrainzDao.getReleasesByArtists(decodeUrlParameter(artistName, "artist"), page.or(0));
        try {
            return om.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error processing JSON", e);
            return "";
        }
    }

    @GET
    @Timed
    @Path("/artistId/{artistId}")
    public String releasesByArtists(@PathParam("artistId") long artistId, @QueryParam("page") Optional<Integer> page) {
        List<Map<String, Object>> map = MusicbrainzDao.getReleasesByArtist(artistId, page.or(0));
        try {
            return om.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error processing JSON", e);
            return "";
        }
    }

    @GET
    @Timed
    @Path("/id/{id}")
    public String releaseById(@PathParam("id") long id) {
        Map<String, Object> releaseMap = MusicbrainzDao.getReleaseById(id);
        List<Map<String, Object>> tracklist = MusicbrainzDao.getTracklist(id, 0);
        Map<String, Object> map = Maps.newHashMap();
        map.put("release", releaseMap);
        map.put("tracklist", tracklist);
        try {
            return om.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error processing JSON", e);
            return "";
        }
    }

    @GET
    @Timed
    @Path("/mbid/{mbid}")
    public String releaseById(@PathParam("mbid") String mbid) {
        Map<String, Object> releaseMap = MusicbrainzDao.getReleaseByMbid(mbid);
        List<Map<String, Object>> tracklist = MusicbrainzDao.getTracklist(mbid, 0);
        Map<String, Object> map = Maps.newHashMap();
        map.put("release", releaseMap);
        map.put("tracklist", tracklist);
        try {
            return om.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error processing JSON", e);
            return "";
        }
    }

    @GET
    @Timed
    @Path("/name/{name}")
    public String releaseByName(@PathParam("name") String name) {
        List<Map<String, Object>> map = MusicbrainzDao.getReleasesByName(decodeUrlParameter(name, "name"), 0);
        try {
            return om.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error processing JSON", e);
            return "";
        }

    }

    private static String decodeUrlParameter(String paramValue, String paramName) {
        try {
            return URLDecoder.decode(paramValue, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            LOGGER.warn("error decoding url parameter " + paramName + "=" + paramValue);
            return paramValue;
        }
    }

}
