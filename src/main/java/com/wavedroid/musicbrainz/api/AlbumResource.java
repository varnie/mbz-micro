package com.wavedroid.musicbrainz.api;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.wavedroid.musicbrainz.dao.MusicbrainzDao;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

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
    public String releasesByArtists(@PathParam("artist") String artistName, @QueryParam("all") Optional<Boolean> all, @QueryParam("page") Optional<Integer> page) {
        List<Map<String, Object>> map = withTags(MusicbrainzDao.getReleasesByArtists(decodeUrlParameter(artistName, "artist"), all.or(false), page.or(0)));
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
    public String releasesByArtists(@PathParam("artistId") long artistId, @QueryParam("all") Optional<Boolean> all, @QueryParam("page") Optional<Integer> page) {
        List<Map<String, Object>> map = withTags(MusicbrainzDao.getReleasesByArtist(artistId, all.or(false), page.or(0)));
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
        Long releaseGroupId = getReleaseGroupId(releaseMap);
        Long releaseId = getReleaseId(releaseMap);
        releaseMap.put("image", getCover(mbid));
        releaseMap.put("genre", MusicbrainzDao.getTags(Lists.newArrayList(releaseGroupId), 1));
        List<Map<String, Object>> tracklist = MusicbrainzDao.getTracklist(releaseId, 0);
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
    public String releaseByName(@PathParam("name") String name, @QueryParam("all") Optional<Boolean> all) {
        List<Map<String, Object>> map = withTags(MusicbrainzDao.getReleasesByName(decodeUrlParameter(name, "name"), all.or(false), 0));
        try {
            return om.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error processing JSON", e);
            return "";
        }

    }

    private Long getReleaseGroupId(Map<String, Object> input) {
        return Long.parseLong(String.valueOf(input.get("release_group_id")));
    }

    private Long getReleaseId(Map<String, Object> input) {
        return Long.parseLong(String.valueOf(input.get("release_id")));
    }

    private static final String COVER_ART_URL = "http://coverartarchive.org/release-group/%s";

    private static String getCover(String mbid) {
        String url = String.format(COVER_ART_URL, mbid);

        HttpClient client = HttpClientBuilder.create().setRedirectStrategy(new LaxRedirectStrategy()).build();
        HttpGet httpGet = new HttpGet(url);
        HttpResponse httpResponse;
        try {
            httpResponse = client.execute(httpGet);
        } catch (IOException e) {
            return "";
        }
        HttpEntity entity = httpResponse.getEntity();
        InputStream is;
        try {
            is = entity.getContent();
        } catch (IOException e) {
            return "";
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();
        br.lines().forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                sb.append(s);
            }
        });
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        Map<String, Object> map;
        try {
            map = om.readValue(sb.toString(), typeRef);
        } catch (IOException e) {
            return "";
        }
        if (map == null || map.isEmpty()) {
            return "";
        }
        @SuppressWarnings("unchecked") List<Map<String, Object>> images = (List<Map<String, Object>>) map.get("images");
        @SuppressWarnings("Convert2Diamond") Map<String, Object> firstImage = Iterables.getFirst(images, new HashMap<String, Object>());
        @SuppressWarnings({"unchecked", "ConstantConditions"}) Map<String, Object> thumbnails = (Map<String, Object>) firstImage.get("thumbnails");
        if (thumbnails == null) {
            return "";
        }
        return (String) thumbnails.get("small");
    }

    private List<Map<String, Object>> withTags(List<Map<String, Object>> map) {
        List<Long> releaseGroupIds = Lists.transform(map, this::getReleaseGroupId);
        List<Map<String, Object>> tagsList = MusicbrainzDao.getTags(releaseGroupIds, 1);
        final Map<String, Map<String, Object>> tagsMap = Maps.newHashMap();
        for (Map<String, Object> m : tagsList) {
            tagsMap.put(String.valueOf(getReleaseGroupId(m)), m);
        }
        return Lists.newArrayList(Lists.transform(map, (Map<String, Object> input) -> {
            Map<String, Object> joined = Maps.newHashMap();
            joined.putAll(input);
            Map<String, Object> tag = tagsMap.get(String.valueOf(getReleaseGroupId(input)));
            if (tag != null) {
                joined.putAll(tag);
            }
            return joined;
        }));
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
