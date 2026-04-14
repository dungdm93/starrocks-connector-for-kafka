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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.starrocks.connector.kafka.StarRocksSinkConnectorConfig.STARROCKS_TOPIC2TABLE_MAP;

public class Util {
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);
    public static final String VERSION = "1.0.3";
    private static final Predicate<String> nameMatcher = Pattern
            .compile("^([_a-zA-Z][$a-zA-Z0-9]+\\.){0,2}[_a-zA-Z][_$a-zA-Z0-9]+$")
            .asMatchPredicate();

    public static Map<String, String> parseTopicToTableMap(String input) {
        var topic2Table = new HashMap<String, String>();
        var valid = true;
        for (var str : input.split(",")) {
            var tt = Arrays.stream(str.split(":"))
                    .map(String::trim)
                    .toArray(String[]::new);

            if (tt.length != 2 || tt[0].isEmpty() || tt[1].isEmpty()) {
                LOG.error("Invalid {} config format: {}", STARROCKS_TOPIC2TABLE_MAP, input);
                return null;
            }

            var topic = tt[0];
            var table = tt[1];

            if (!nameMatcher.test(table)) {
                LOG.error("table name {} should have at least 2 characters, start with _a-zA-Z, and only contains _$a-zA-z0-9", table);
                valid = false;
            }

            if (topic2Table.containsKey(topic)) {
                LOG.error("topic name {} is duplicated", topic);
                valid = false;
            }

            topic2Table.put(topic, table);
        }
        if (!valid) {
            var errMsg = String.format("Invalid %s config format: %s", STARROCKS_TOPIC2TABLE_MAP, input);
            throw new RuntimeException(errMsg);
        }
        return topic2Table;
    }
}
