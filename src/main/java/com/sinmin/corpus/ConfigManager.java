/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package com.sinmin.corpus;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by dimuthuupeksha on 2/12/15.
 */
public class ConfigManager {

    public static final String DATA_ROOT= "DATA_ROOT";
    public static final String ORACLE_DB_DRIVER = "ORACLE_DB_DRIVER";
    public static final String ORACLE_DB_CONNECTION = "ORACLE_DB_CONNECTION";
    public static final String ORACLE_DB_USER = "ORACLE_DB_USER";
    public static final String ORACLE_DB_PASSWORD = "ORACLE_DB_PASSWORD";
    public static final String CASSANDRA_SERVER_IP = "CASSANDRA_SERVER_IP";


    final static Logger logger = Logger.getLogger(ConfigManager.class);

    public static String getProperty(String propertyName){ //reads properties from config file

        InputStream inputStream = ConfigManager.class.getClassLoader().getResourceAsStream("config.properties");
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
            return properties.getProperty(propertyName);
        } catch (IOException e) {
            logger.error(e);
            return null;
        }
    }
}
