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

import com.starrocks.connector.kafka.json.DecimalFormat;
import com.starrocks.connector.kafka.json.JsonConverter;
import com.starrocks.connector.kafka.json.JsonConverterConfig;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


//  Please reference to: https://docs.confluent.io/platform/7.4/connect/javadocs/javadoc/org/apache/kafka/connect/sink/SinkTask.html
//  SinkTask is a Task that takes records loaded from Kafka and sends them to another system.
//  Each task instance is assigned a set of partitions by the Connect framework and will handle
//  all records received from those partitions.

//  As records are fetched from Kafka, they will be passed to the sink task using the put(Collection) API,
//  which should either write them to the downstream system or batch them for later writing.
//  Periodically, Connect will call flush(Map) to ensure that batched records are actually
//  pushed to the downstream system. Below we describe the lifecycle of a SinkTask.
//    1. Initialization: SinkTasks are first initialized using initialize(SinkTaskContext) to prepare the task's context and start(Map) to accept configuration and start any services needed for processing.
//    2. Partition Assignment: After initialization, Connect will assign the task a set of partitions using open(Collection). These partitions are owned exclusively by this task until they have been closed with close(Collection).
//    3. Record Processing: Once partitions have been opened for writing, Connect will begin forwarding records from Kafka using the put(Collection) API. Periodically, Connect will ask the task to flush records using flush(Map) as described above.
//    4. Partition Rebalancing: Occasionally, Connect will need to change the assignment of this task. When this happens, the currently assigned partitions will be closed with close(Collection) and the new assignment will be opened using open(Collection).
//    5. Shutdown: When the task needs to be shutdown, Connect will close active partitions (if there are any) and stop the task using stop()

public class StarRocksSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkTask.class);
    private static final long KILO_BYTES_SCALE = 1024L;
    private static final long MEGA_BYTES_SCALE = KILO_BYTES_SCALE * KILO_BYTES_SCALE;
    private static final long GIGA_BYTES_SCALE = MEGA_BYTES_SCALE * KILO_BYTES_SCALE;

    public enum SinkType {
        CSV,
        JSON,
    }

    private SinkType sinkType;
    private StreamLoadManagerV2 loadManager;
    private Map<String, String> props;
    private StreamLoadProperties loadProperties;
    private String database;
    private Map<String, String> topic2Table;
    private final Map<String, String> streamLoadProps = new HashMap<>();
    private JsonConverter jsonConverter;
    private long maxRetryTimes;
    private long retryCount = 0;
    private Throwable sdkException;

    private long buffMaxBytes;
    private long bufferFlushInterval;
    private long currentBufferBytes = 0;
    private long lastFlushTime = 0;

    private StreamLoadManagerV2 buildLoadManager(StreamLoadProperties loadProperties) {
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(loadProperties, true);
        manager.init();
        return manager;
    }

    // Data chunk size in an http request for the stream load.
    // Flink connector does not open this configuration to users, so we use a fixed value here.
    private long getChunkLimit() {
        return 3 * GIGA_BYTES_SCALE;
    }

    // Timeout in millisecond to wait for 100-continue response for the http client.
    // Flink connector does not open this configuration to users, so we use a fixed value here.
    private int getWaitForContinueTimeout() {
        return 3000;
    }

    // Stream load thread count
    // An HTTP thread pool is used for communication between the SDK and SR. 
    // This configuration item is used to set the number of threads in the thread pool.
    private int getIoThreadCount() {
        return 2;
    }

    private long getScanFrequency() {
        return 50L;
    }

    private String getLabelPrefix() {
        return null;
    }

    private void parseSinkStreamLoadProperties() {
        props.keySet().stream()
                .filter(key -> key.startsWith(StarRocksSinkConnectorConfig.SINK_PROPERTIES_PREFIX))
                .forEach(key -> {
                    final String value = props.get(key);
                    final String subKey = key.substring((StarRocksSinkConnectorConfig.SINK_PROPERTIES_PREFIX).length()).toLowerCase();
                    streamLoadProps.put(subKey, value);
                });
    }

    private StreamLoadProperties buildLoadProperties() {
        var loadUrl = props.get(StarRocksSinkConnectorConfig.STARROCKS_LOAD_URL).split(",");
        database = props.get(StarRocksSinkConnectorConfig.STARROCKS_DATABASE_NAME);
        var format = props.getOrDefault(StarRocksSinkConnectorConfig.SINK_FORMAT, "json").toLowerCase();
        StreamLoadDataFormat dataFormat;
        switch (format) {
            case "csv" -> {
                dataFormat = new StreamLoadDataFormat.CSVFormat(StarRocksDelimiterParser
                        .parse(props.get(StarRocksSinkConnectorConfig.SINK_PROPERTIES_ROW_DELIMITER), "\n"));
                sinkType = SinkType.CSV;
            }
            case "json" -> {
                dataFormat = StreamLoadDataFormat.JSON;
                sinkType = SinkType.JSON;
            }
            default -> throw new RuntimeException("data format are not support");
        }
        // The default load format for the Starrocks Kafka Connector is JSON. If the format is not specified
        // in the configuration file, it needs to be added to the props.
        if (!props.containsKey(StarRocksSinkConnectorConfig.SINK_FORMAT)) {
            props.put(StarRocksSinkConnectorConfig.SINK_FORMAT, "json");
        }
        parseSinkStreamLoadProperties();
        LOG.info("Starrocks sink type is {}, stream load properties: {}", sinkType, streamLoadProps);
        // The Stream SDK must force the table name, which we set to _sr_default_table.
        // _sr_default_table will not be used.
        buffMaxBytes = Long.parseLong(props.getOrDefault(StarRocksSinkConnectorConfig.BUFFERFLUSH_MAXBYTES, "67108864"));
        var connectTimeoutms = Integer.parseInt(props.getOrDefault(StarRocksSinkConnectorConfig.CONNECT_TIMEOUTMS, "100"));
        var username = props.get(StarRocksSinkConnectorConfig.STARROCKS_USERNAME);
        var password = props.get(StarRocksSinkConnectorConfig.STARROCKS_PASSWORD);
        bufferFlushInterval = Long.parseLong(props.getOrDefault(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS, "1000"));
        return StreamLoadProperties.builder()
                .loadUrls(loadUrl)
                .username(username)
                .password(password)
                .defaultTableProperties(StreamLoadTableProperties.builder()
                        .database(database)
                        .table("_sr_default_table")
                        .streamLoadDataFormat(dataFormat)
                        .chunkLimit(getChunkLimit())
                        .addCommonProperties(streamLoadProps)
                        .build())
                .cacheMaxBytes(buffMaxBytes)
                .connectTimeout(connectTimeoutms)
                .waitForContinueTimeoutMs(getWaitForContinueTimeout())
                .ioThreadCount(getIoThreadCount())
                .scanningFrequency(getScanFrequency())
                .labelPrefix(getLabelPrefix())
                .expectDelayTime(bufferFlushInterval)
                .addHeaders(streamLoadProps)
                .enableTransaction()
                .build();
    }

    @Override
    public String version() {
        return Util.VERSION;
    }

    public static JsonConverter createJsonConverter() {
        var converter = new JsonConverter();
        converter.configure(Map.of(
                JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false,
                JsonConverterConfig.REPLACE_NULL_WITH_DEFAULT_CONFIG, false,
                JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name()
        ), false);
        return converter;
    }

    @Override
    public void start(Map<String, String> props) {
        LOG.info("Starrocks sink task starting. version is {}", Util.VERSION);
        this.props = props;
        loadProperties = buildLoadProperties();
        loadManager = buildLoadManager(loadProperties);
        topic2Table = getTopicToTableMap(props);
        jsonConverter = createJsonConverter();
        maxRetryTimes = Long.parseLong(props.getOrDefault(StarRocksSinkConnectorConfig.SINK_MAXRETRIES, "3"));
        LOG.info("Starrocks sink task started. version is {}", Util.VERSION);
    }

    static Map<String, String> getTopicToTableMap(Map<String, String> config) {
        if (config.containsKey(StarRocksSinkConnectorConfig.STARROCKS_TOPIC2TABLE_MAP)) {
            Map<String, String> result =
                    Util.parseTopicToTableMap(config.get(StarRocksSinkConnectorConfig.STARROCKS_TOPIC2TABLE_MAP));
            if (result != null) {
                return result;
            }
            LOG.error("Invalid Input, Topic2Table Map disabled");
        }
        return new HashMap<>();
    }

    private String getTableFromTopic(String topic) {
        return topic2Table.getOrDefault(topic, topic);
    }

    public void setJsonConverter(JsonConverter jsonConverter) {
        this.jsonConverter = jsonConverter;
    }

    public void setSinkType(SinkType sinkType) {
        this.sinkType = sinkType;
    }

    // This function is used to parse a SinkRecord and returns a String type row.
    // There are several scenarios to consider:
    // 1. If `sinkRecord` is null, return directly.
    // 2. If the `value` of `sinkRecord` is null, return directly.
    // 3. If the `valueSchema` of `sinkRecord` is null, it means that the received
    //    data does not have a defined schema. In this case, an attempt is made to
    //    parse it. If the parsing fails, a `DataException` exception will be thrown.
    public String getRecordFromSinkRecord(SinkRecord sinkRecord) {
        if (sinkRecord == null) {
            LOG.debug("Have got a null sink record");
            return null;
        }
        if (sinkRecord.value() == null) {
            LOG.debug("Sink record value is null, the record is {}", sinkRecord);
            return null;
        }
        if (sinkRecord.valueSchema() == null) {
            LOG.debug("Sink record value schema is null, the record is {}", sinkRecord);
        }

        if (sinkType == SinkType.CSV) {
            // When the sink Type is CSV, make sure that the SinkRecord.value type is String
            if (!(sinkRecord.value() instanceof String row)) {
                var msg = String.format("class %s cannot be cast to String", sinkRecord.value().getClass().getName());
                LOG.error(msg);
                throw new DataException(msg);
            }
            return row;
        } else {
            try {
                var b = jsonConverter.fromConnectData(sinkRecord.topic(), sinkRecord.valueSchema(), sinkRecord.value());
                return new String(b);
            } catch (DataException e) {
                LOG.error(e.getMessage());
                throw e;
            }
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        long start = System.currentTimeMillis();
        if (maxRetryTimes != -1) {
            if (retryCount > maxRetryTimes) {
                LOG.error("Starrocks Put failure {} times, which bigger than maxRetryTimes {}, sink task will be stopped", retryCount, maxRetryTimes);
                assert sdkException != null;
                LOG.error("Error message is ", sdkException);
                throw new RuntimeException(sdkException);
            }
        }
        boolean occurException = false;
        Exception e = null;
        SinkRecord record = null;
        SinkRecord firstRecord = null;
        for (var r : records) {
            record = r;
            if (firstRecord == null) {
                firstRecord = record;
            }
            LOG.debug("Received record: {}", record);

            var topic = record.topic();
            // The sdk does not provide the ability to clean up exceptions, that is to say, according to the current implementation of the SDK,
            // after an Exception occurs, the SDK must be re-initialized, which is based on flink:
            // 1. When an exception occurs, put will continue to fail, at which point we do nothing and let put move forward.
            // 2. Because the framework periodically calls the preCommit method, we can sense if an exception has occurred in
            //    this method. In the case of an exception, we initialize the new SDK and then throw an exception to the framework.
            //    In this case, the framework repulls the data from the commit point and then moves forward.
            String row = getRecordFromSinkRecord(record);
            LOG.debug("Parsed row: {}", row);
            if (row == null) continue;

            try {
                loadManager.write(null, database, getTableFromTopic(topic), row);
                currentBufferBytes += row.getBytes().length;
            } catch (Exception writeException) {
                LOG.error("Starrocks Put error: {} topic, partition, offset is {}, {}, {}", writeException.getMessage(), topic, record.kafkaPartition(), record.kafkaOffset());
                writeException.printStackTrace();
                occurException = true;
                e = writeException;
                break;
            }
        }

        if (occurException) {
            LOG.info("Starrocks Put occurs exception, Err {} currentBufferBytes {} recordRange [{}:{}-{}:{}] cost {}ms",
                    e.getMessage(), currentBufferBytes,
                    firstRecord.kafkaPartition(),
                    firstRecord.kafkaOffset(),
                    record.kafkaPartition(),
                    record.kafkaOffset(), System.currentTimeMillis() - start);
        } else {
            LOG.info("Starrocks Put success, currentBufferBytes {} recordRange [{}:{}-{}:{}] cost {}ms",
                    currentBufferBytes,
                    firstRecord == null ? null : firstRecord.kafkaPartition(),
                    firstRecord == null ? null : firstRecord.kafkaOffset(),
                    record == null ? null : record.kafkaPartition(),
                    record == null ? null : record.kafkaOffset(), System.currentTimeMillis() - start);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        long start = System.currentTimeMillis();
        // return previous offset when buffer size and flush interval are not reached
        if (currentBufferBytes < buffMaxBytes && System.currentTimeMillis() - lastFlushTime < bufferFlushInterval) {
            LOG.info("Starrocks skip preCommit currentBufferBytes {} less than buffMaxbytes {}"
                            + " or SinceLastFlushTime {} less than bufferFlushInterval {}",
                    currentBufferBytes, buffMaxBytes, System.currentTimeMillis() - lastFlushTime, bufferFlushInterval);
            return Collections.emptyMap();
        }
        Throwable flushException = null;
        try {
            LOG.info("Starrocks preCommit flush currentBufferBytes {} and SinceLastFlushTime {}",
                    currentBufferBytes, System.currentTimeMillis() - lastFlushTime);
            loadManager.flush();
        } catch (Exception e) {
            flushException = e;
        } finally {
            lastFlushTime = System.currentTimeMillis();
            currentBufferBytes = 0;
        }
        if (flushException == null) {
            flushException = loadManager.getException();
        }
        if (flushException != null) {
            // Update SDK exception
            sdkException = flushException;
            retryCount++;
            //  When an exception occurs, we re-initialize the SDK instance.
            if (loadManager != null) {
                loadManager.close();
            }
            loadManager = buildLoadManager(loadProperties);
            LOG.warn("Starrocks preCommit flush fail err {} retry times {} cost {}ms",
                    flushException.getMessage(), retryCount, System.currentTimeMillis() - start);
            //  Each time the SDK is checked for an exception, if an exception occurs, it is thrown, and the framework replays the data from the offset of the commit.
            throw new RuntimeException(flushException.getMessage());
        } else {
            retryCount = 0;
            sdkException = null;
            LOG.info("Starrocks preCommit flush success offsets {} cost {}ms", offsets, System.currentTimeMillis() - start);
        }
        return offsets;
    }

    @Override
    public void stop() {
        if (loadManager != null) {
            loadManager.close();
        }
        if (jsonConverter != null) {
            jsonConverter.close();
        }
        LOG.info("Starrocks sink task stopped. version is {}", Util.VERSION);
    }
}