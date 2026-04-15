/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starrocks.connector.kafka.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Converts Kafka Connect data to JSON bytes. Operates with fixed settings:
 * schemas disabled, null replacement disabled, and NUMERIC decimal format.
 */
public class JsonConverter {
    private static final JsonMapper JSON_MAPPER;
    private static final JsonNodeFactory JSON_NODE_FACTORY;

    static {
        JSON_MAPPER = new JsonMapper();
        JSON_NODE_FACTORY = new JsonNodeFactory(true);
        JSON_MAPPER.setNodeFactory(JSON_NODE_FACTORY);
    }

    private final HashMap<String, LogicalTypeConverter> converters;

    @FunctionalInterface
    private interface LogicalTypeConverter {
        JsonNode toJson(Schema schema, Object value);
    }

    public JsonConverter() {
        converters = new HashMap<>();
        converters.put(Decimal.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof java.math.BigDecimal decimal))
                throw new DataException("Invalid type for Decimal, expected java.math.BigDecimal but was " + value.getClass());
            return JSON_NODE_FACTORY.numberNode(decimal);
        });
        converters.put(Date.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof java.util.Date date))
                throw new DataException("Invalid type for Date, expected java.util.Date but was " + value.getClass());
            return JSON_NODE_FACTORY.numberNode(Date.fromLogical(schema, date));
        });
        converters.put(Time.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof java.util.Date date))
                throw new DataException("Invalid type for Time, expected java.util.Date but was " + value.getClass());
            return JSON_NODE_FACTORY.numberNode(Time.fromLogical(schema, date));
        });
        converters.put(Timestamp.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof java.util.Date date))
                throw new DataException("Invalid type for Timestamp, expected java.util.Date but was " + value.getClass());
            return JSON_NODE_FACTORY.numberNode(Timestamp.fromLogical(schema, date));
        });
    }

    public String fromConnectData(Schema schema, Object object) {
        if (schema == null && object == null) {
            return null;
        }
        try {
            var node = convertToJson(schema, object);
            return JSON_MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new DataException("Converting Kafka Connect data to String failed due to serialization error: ", e);
        }
    }

    public JsonNode convertToJson(Schema schema, Object object) {
        if (object == null) {
            if (schema == null)
                return null;
            if (schema.isOptional())
                return JSON_NODE_FACTORY.nullNode();
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }

        if (schema != null && schema.name() != null) {
            var logicalConverter = converters.get(schema.name());
            if (logicalConverter != null)
                return logicalConverter.toJson(schema, object);
        }

        try {
            final var schemaType = schema == null ? ConnectSchema.schemaType(object.getClass()) : schema.type();
            if (schemaType == null)
                throw new DataException("Java class " + object.getClass() + " does not have corresponding schema type.");

            return switch (schemaType) {
                case INT8 -> JSON_NODE_FACTORY.numberNode((Byte) object);
                case INT16 -> JSON_NODE_FACTORY.numberNode((Short) object);
                case INT32 -> JSON_NODE_FACTORY.numberNode((Integer) object);
                case INT64 -> JSON_NODE_FACTORY.numberNode((Long) object);
                case FLOAT32 -> JSON_NODE_FACTORY.numberNode((Float) object);
                case FLOAT64 -> JSON_NODE_FACTORY.numberNode((Double) object);
                case BOOLEAN -> JSON_NODE_FACTORY.booleanNode((Boolean) object);
                case STRING -> JSON_NODE_FACTORY.textNode(((CharSequence) object).toString());
                case BYTES -> {
                    if (object instanceof byte[] bytes)
                        yield JSON_NODE_FACTORY.binaryNode(bytes);
                    else if (object instanceof ByteBuffer buf)
                        yield JSON_NODE_FACTORY.binaryNode(buf.array());
                    else
                        throw new DataException("Invalid type for bytes type: " + object.getClass());
                }
                case ARRAY -> {
                    var collection = (Collection<?>) object;
                    var list = JSON_NODE_FACTORY.arrayNode();
                    var valueSchema = schema == null ? null : schema.valueSchema();
                    collection.forEach(elem -> list.add(convertToJson(valueSchema, elem)));
                    yield list;
                }
                case MAP -> {
                    var map = (Map<?, ?>) object;
                    var objectMode = schema != null
                            ? schema.keySchema().type() == Schema.Type.STRING
                            : map.keySet().stream().allMatch(k -> k instanceof String);
                    var keySchema = schema == null ? null : schema.keySchema();
                    var valueSchema = schema == null ? null : schema.valueSchema();
                    if (objectMode) {
                        var obj = JSON_NODE_FACTORY.objectNode();
                        map.forEach((k, v) -> {
                            var key = convertToJson(keySchema, k).asText();
                            var value = convertToJson(valueSchema, v);
                            obj.set(key, value);
                        });
                        yield obj;
                    } else {
                        var list = JSON_NODE_FACTORY.arrayNode();
                        map.forEach((k, v) -> {
                            var key = convertToJson(keySchema, k);
                            var value = convertToJson(valueSchema, v);
                            var elem = JSON_NODE_FACTORY.arrayNode()
                                    .add(key)
                                    .add(value);
                            list.add(elem);
                        });
                        yield list;
                    }
                }
                case STRUCT -> {
                    if (!(object instanceof Struct struct) || !struct.schema().equals(schema))
                        throw new DataException("Mismatching schema.");
                    var obj = JSON_NODE_FACTORY.objectNode();
                    schema.fields().forEach(field -> {
                        var value = convertToJson(field.schema(), struct.get(field));
                        obj.set(field.name(), value);
                    });
                    yield obj;
                }
            };
        } catch (ClassCastException e) {
            var schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
            throw new DataException("Invalid type for " + schemaTypeStr + ": " + object.getClass());
        }
    }
}
