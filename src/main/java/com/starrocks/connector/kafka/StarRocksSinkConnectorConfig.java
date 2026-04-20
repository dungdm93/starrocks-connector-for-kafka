/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarRocksSinkConnectorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkConnectorConfig.class);
    public static final String GroupName = "StarRocks";

    /// FE HTTP Server connection address.
    /// The format is `<fe_host1>:<fe_http_port1>,<fe_host2>:<fe_http_port2>`,
    /// You can provide multiple addresses, separated by commas (`,`).
    /// For example, `192.168.xxx.xxx:8030,192.168.xxx.xxx:8030`
    public static final String STARROCKS_LOAD_URL = "starrocks.http.url";
    public static final String STARROCKS_USERNAME = "starrocks.username";
    public static final String STARROCKS_PASSWORD = "starrocks.password";

    /// The target database for loading data.
    public static final String STARROCKS_DATABASE_NAME = "starrocks.database.name";

    /// When the topic name does not match the SR table name,
    /// this configuration item provides a mapping in the following format:
    /// `<topic-1>:<table-1>,<topic-2>:<table-2>,...`
    public static final String STARROCKS_TOPIC2TABLE_MAP = "starrocks.topic2table.map";

    /// Stream Load parameters, which controls the load behavior.
    public static final String SINK_PROPERTIES_PREFIX = "sink.properties.";

    /// Data writing to StarRocks may fail due to a network fault or a short time StarRocks restart.
    /// For precommit, the connector detects if an error has occurred and writes the failed data to the SR again.
    /// This configuration controls the number of failed retries.
    /// The default value is `3`. `-1` indicates unlimited retry.
    public static final String SINK_MAXRETRIES = "sink.maxretries";

    /// The target database for loading data. The default is `JSON`.
    public static final String SINK_FORMAT = "sink.properties.format";

    /// This configuration is used to specify line delimiters when writing `CSV` data to starrocks.
    public static final String SINK_PROPERTIES_ROW_DELIMITER = "sink.properties.row_delimiter";

    /// The envelope format of incoming Kafka records.
    /// Supported values: `none` (default), `debezium`.
    /// When set to `debezium`, records are expected to be in Debezium CDC envelope format
    /// and are unwrapped before being written to StarRocks.
    public static final String ENVELOPE = "starrocks.envelope";

    /// Specifies whether the connector processes DELETE or tombstone events
    /// and removes the corresponding row from the database
    public static final String DELETE_ENABLE = "starrocks.delete.enabled";

    /// The maximum size of data that can be loaded into StarRocks at a time.
    /// Valid values: 64 MB to 10 GB.
    /// The SDK buffer may buffer data from multiple streams,
    /// and this threshold refers to the total size
    public static final String BUFFERFLUSH_MAXBYTES = "bufferflush.maxbytes";

    /// The interval at which data is flushed. Valid values: 1000 to 3600000.
    public static final String BUFFERFLUSH_INTERVALMS = "bufferflush.intervalms";

    /// The period of time after which the stream load times out. Valid values: 100 to 60000.
    public static final String CONNECT_TIMEOUTMS = "connect.timeoutms";

    public static final String[] mustRequiredConfigs = {
            STARROCKS_LOAD_URL,
            STARROCKS_DATABASE_NAME,
            STARROCKS_USERNAME,
            STARROCKS_PASSWORD
    };

    public static ConfigDef newConfigDef() {
        return new ConfigDef()
                .define(
                        STARROCKS_LOAD_URL, Type.STRING, null,
                        new ConfigDef.NonEmptyString(),
                        Importance.HIGH,
                        "StarRocks http url",
                        GroupName, 0,
                        Width.NONE,
                        STARROCKS_LOAD_URL
                ).define(
                        STARROCKS_USERNAME, Type.STRING, null,
                        new ConfigDef.NonEmptyString(),
                        Importance.HIGH,
                        "StarRocks username",
                        GroupName, 1,
                        Width.NONE, STARROCKS_USERNAME
                ).define(
                        STARROCKS_PASSWORD, Type.PASSWORD, null,
                        null,
                        Importance.HIGH,
                        "StarRocks password",
                        GroupName, 2,
                        Width.NONE, STARROCKS_PASSWORD
                ).define(
                        STARROCKS_DATABASE_NAME, Type.STRING, null,
                        new ConfigDef.NonEmptyString(),
                        Importance.HIGH,
                        "StarRocks database",
                        GroupName, 3,
                        Width.NONE, STARROCKS_DATABASE_NAME
                ).define(
                        STARROCKS_TOPIC2TABLE_MAP, Type.STRING, null,
                        new ConfigDef.NonEmptyString(),
                        Importance.LOW,
                        "a mapping between the topic name and the table name",
                        GroupName,
                        4,
                        Width.NONE, STARROCKS_TOPIC2TABLE_MAP
                ).define(
                        SINK_MAXRETRIES, Type.LONG, 3,
                        ConfigDef.Range.between(-1, Long.MAX_VALUE),
                        Importance.LOW,
                        "number of Stream Load retries after a stream load failure",
                        GroupName,
                        0,
                        Width.NONE, SINK_MAXRETRIES
                ).define(
                        SINK_FORMAT, Type.STRING, null,
                        new ConfigDef.NonEmptyString(),
                        Importance.MEDIUM,
                        "write to starrocks data format",
                        GroupName,
                        0,
                        Width.NONE, SINK_FORMAT
                ).define(
                        ENVELOPE, Type.STRING, "none",
                        ConfigDef.ValidString.in("none", "debezium"),
                        Importance.MEDIUM,
                        "envelope format of incoming records; 'none' (default) or 'debezium'",
                        GroupName,
                        0,
                        Width.NONE, ENVELOPE
                )
                .define(
                        DELETE_ENABLE, Type.BOOLEAN, false,
                        null,
                        Importance.MEDIUM,
                        "Specifies whether the connector processes DELETE or tombstone events and " +
                                "removes the corresponding row from the database. " +
                                "Enable option requires target table is PRIMARY KEY table.",
                        GroupName,
                        0,
                        Width.NONE, DELETE_ENABLE
                ).define(
                        BUFFERFLUSH_MAXBYTES, Type.LONG, 67108864,
                        ConfigDef.Range.between(67108864, 10737418240L),
                        Importance.LOW,
                        "the size of a batch of data",
                        GroupName,
                        0,
                        Width.NONE, BUFFERFLUSH_MAXBYTES
                ).define(
                        BUFFERFLUSH_INTERVALMS, Type.LONG, 1000,
                        ConfigDef.Range.between(1000, 3600000),
                        Importance.LOW,
                        "the interval at which data is sent in bulk to starrocks",
                        GroupName,
                        0,
                        Width.NONE, BUFFERFLUSH_INTERVALMS
                ).define(
                        CONNECT_TIMEOUTMS, Type.LONG, 100,
                        ConfigDef.Range.between(100, 60000),
                        Importance.LOW,
                        "timeout period for connecting to load-url",
                        GroupName,
                        0,
                        Width.NONE, CONNECT_TIMEOUTMS
                );
    }
}
