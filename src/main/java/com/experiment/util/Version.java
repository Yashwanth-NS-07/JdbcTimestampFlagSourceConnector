package com.experiment.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Version {
    private static final Logger log = LoggerFactory.getLogger(Version.class);
    private static final String PATH = "/application.properties";
    private static String version = "unknown";
    static {
        try(InputStream stream = Version.class.getResourceAsStream(PATH)) {
            Properties properties = new Properties();
            properties.load(stream);
            version = properties.getProperty("version", version).trim();
        } catch (IOException e) {
            log.warn("Error while loading version: ", e);
        }
    }
    public static String getVersion() {return version;}
}
