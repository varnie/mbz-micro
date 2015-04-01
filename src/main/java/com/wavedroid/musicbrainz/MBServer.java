package com.wavedroid.musicbrainz;

import com.wavedroid.musicbrainz.api.AlbumResource;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Environment;

/**
 * @author Dmitriy Khvatov (<i>dimax4@gmail.com</i>)
 * @version $Id$
 */
public class MBServer extends Application<Configuration> {

    public static void main(String[] args) throws Exception {
        new MBServer().run(args);
    }

    @Override
    public void run(Configuration configuration, Environment environment) throws Exception {
        environment.jersey().register(new AlbumResource());
    }
}
