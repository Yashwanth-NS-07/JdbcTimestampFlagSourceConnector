/*
 * Copyright 2025 Yashwanth Gowda N S
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
