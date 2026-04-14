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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StringConverterConfig;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;


/**
 * Implementation of {@link Converter} and {@link HeaderConverter} that uses JSON to store schemas and objects.
 * By default, this converter will serialize Connect keys, values, and headers with schemas, although this can be
 * disabled with the {@link JsonConverterConfig#SCHEMAS_ENABLE_CONFIG schemas.enable} configuration option.
 * <p>
 * This implementation currently does nothing with the topic names or header keys.
 */
public class JsonConverter implements Converter, HeaderConverter {

    private static final Map<Schema.Type, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS = new EnumMap<>(Schema.Type.class);

    static {
        TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, (schema, value, config) -> value.booleanValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, (schema, value, config) -> (byte) value.intValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, (schema, value, config) -> (short) value.intValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, (schema, value, config) -> value.intValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, (schema, value, config) -> value.longValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, (schema, value, config) -> value.floatValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, (schema, value, config) -> value.doubleValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, (schema, value, config) -> value.textValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, (schema, value, config) -> {
            try {
                return value.binaryValue();
            } catch (IOException e) {
                throw new DataException("Invalid bytes field", e);
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, (schema, value, config) -> {
            var elemSchema = schema == null ? null : schema.valueSchema();
            return value.valueStream()
                    .map(elem -> convertToConnect(elemSchema, elem, config))
                    .toList();
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, (schema, value, config) -> {
            var keySchema = schema == null ? null : schema.keySchema();
            var valueSchema = schema == null ? null : schema.valueSchema();

            // If the map uses strings for keys, it should be encoded in the natural JSON format. If it uses other
            // primitive types or a complex type as a key, it will be encoded as a list of pairs. If we don't have a
            // schema, we default to encoding in a Map.
            var result = new HashMap<>();
            if (schema == null || keySchema.type() == Schema.Type.STRING) {
                if (!value.isObject())
                    throw new DataException("Maps with string fields should be encoded as JSON objects, but found " + value.getNodeType());
                value.forEachEntry((k, v) ->
                        result.put(k, convertToConnect(valueSchema, v, config))
                );
            } else {
                if (!value.isArray())
                    throw new DataException("Maps with non-string fields should be encoded as JSON array of tuples, but found " + value.getNodeType());
                for (var entry : value) {
                    if (!entry.isArray())
                        throw new DataException("Found invalid map entry instead of array tuple: " + entry.getNodeType());
                    if (entry.size() != 2)
                        throw new DataException("Found invalid map entry, expected length 2 but found :" + entry.size());
                    result.put(convertToConnect(keySchema, entry.get(0), config),
                            convertToConnect(valueSchema, entry.get(1), config));
                }
            }
            return result;
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, (schema, value, config) -> {
            if (!value.isObject())
                throw new DataException("Structs should be encoded as JSON objects, but found " + value.getNodeType());

            // We only have ISchema here but need Schema, so we need to materialize the actual schema. Using ISchema
            // avoids having to materialize the schema for non-Struct types but it cannot be avoided for Structs since
            // they require a schema to be provided at construction. However, the schema is only a SchemaBuilder during
            // translation of schemas to JSON; during the more common translation of data to JSON, the call to schema.schema()
            // just returns the schema Object and has no overhead.
            var result = new Struct(schema.schema());
            schema.fields().forEach(field ->
                    result.put(field, convertToConnect(field.schema(), value.get(field.name()), config)));

            return result;
        });
    }

    // Convert values in Kafka Connect form into/from their logical types. These logical converters are discovered by logical type
    // names specified in the field
    private static final HashMap<String, LogicalTypeConverter> LOGICAL_CONVERTERS = new HashMap<>();

    private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);

    static {
        LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof BigDecimal decimal))
                    throw new DataException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());

                return switch (config.decimalFormat()) {
                    case NUMERIC -> JSON_NODE_FACTORY.numberNode(decimal);
                    case BASE64 -> JSON_NODE_FACTORY.binaryNode(Decimal.fromLogical(schema, decimal));
                };
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (value.isNumber()) return value.decimalValue();
                if (value.isBinary() || value.isTextual()) {
                    try {
                        return Decimal.toLogical(schema, value.binaryValue());
                    } catch (Exception e) {
                        throw new DataException("Invalid bytes for Decimal field", e);
                    }
                }

                throw new DataException("Invalid type for Decimal, underlying representation should be numeric or bytes but was " + value.getNodeType());
            }
        });

        LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof java.util.Date date))
                    throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
                return JSON_NODE_FACTORY.numberNode(Date.fromLogical(schema, date));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!(value.isInt()))
                    throw new DataException("Invalid type for Date, underlying representation should be integer but was " + value.getNodeType());
                return Date.toLogical(schema, value.intValue());
            }
        });

        LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof java.util.Date date))
                    throw new DataException("Invalid type for Time, expected Date but was " + value.getClass());
                return JSON_NODE_FACTORY.numberNode(Time.fromLogical(schema, date));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!(value.isInt()))
                    throw new DataException("Invalid type for Time, underlying representation should be integer but was " + value.getNodeType());
                return Time.toLogical(schema, value.intValue());
            }
        });

        LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof java.util.Date date))
                    throw new DataException("Invalid type for Timestamp, expected Date but was " + value.getClass());
                return JSON_NODE_FACTORY.numberNode(Timestamp.fromLogical(schema, date));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!(value.isIntegralNumber()))
                    throw new DataException("Invalid type for Timestamp, underlying representation should be integral but was " + value.getNodeType());
                return Timestamp.toLogical(schema, value.longValue());
            }
        });
    }

    private JsonConverterConfig config;
    private Cache<Schema, ObjectNode> fromConnectSchemaCache;
    private Cache<JsonNode, Schema> toConnectSchemaCache;


    public JsonSerializer getSerializer() {
        return serializer;
    }

    private final JsonSerializer serializer;
    private final JsonDeserializer deserializer;

    public JsonConverter() {
        serializer = new JsonSerializer(
                Collections.emptySet(),
                JSON_NODE_FACTORY
        );

        deserializer = new JsonDeserializer(
                // this ensures that the JsonDeserializer maintains full precision on
                // floating point numbers that cannot fit into float64
                Collections.singleton(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS),
                JSON_NODE_FACTORY
        );
    }

    // visible for testing
    long sizeOfFromConnectSchemaCache() {
        return fromConnectSchemaCache.size();
    }

    // visible for testing
    long sizeOfToConnectSchemaCache() {
        return toConnectSchemaCache.size();
    }

    @Override
    public ConfigDef config() {
        return JsonConverterConfig.configDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        config = new JsonConverterConfig(configs);

        serializer.configure(configs, config.type() == ConverterType.KEY);
        deserializer.configure(configs, config.type() == ConverterType.KEY);

        fromConnectSchemaCache = new SynchronizedCache<>(new LRUCache<>(config.schemaCacheSize()));
        toConnectSchemaCache = new SynchronizedCache<>(new LRUCache<>(config.schemaCacheSize()));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> conf = new HashMap<>(configs);
        conf.put(StringConverterConfig.TYPE_CONFIG, isKey ? ConverterType.KEY.getName() : ConverterType.VALUE.getName());
        configure(conf);
    }

    @Override
    public void close() {
        Utils.closeQuietly(this.serializer, "JSON converter serializer");
        Utils.closeQuietly(this.deserializer, "JSON converter deserializer");
    }

    @Override
    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        return fromConnectData(topic, schema, value);
    }

    @Override
    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        return toConnectData(topic, value);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema == null && value == null) {
            return null;
        }

        JsonNode jsonValue = config.schemasEnabled() ? convertToJsonWithEnvelope(schema, value) : convertToJsonWithoutEnvelope(schema, value);
        try {
            return serializer.serialize(topic, jsonValue);
        } catch (SerializationException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        JsonNode jsonValue;

        // This handles a tombstone message
        if (value == null) {
            return SchemaAndValue.NULL;
        }

        try {
            jsonValue = deserializer.deserialize(topic, value);
        } catch (SerializationException e) {
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }

        if (config.schemasEnabled() && (!jsonValue.isObject() || jsonValue.size() != 2 || !jsonValue.has(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME) || !jsonValue.has(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME)))
            throw new DataException("JsonConverter with schemas.enable requires \"schema\" and \"payload\" fields and may not contain additional fields." +
                    " If you are trying to deserialize plain JSON data, set schemas.enable=false in your converter configuration.");

        // The deserialized data should either be an envelope object containing the schema and the payload or the schema
        // was stripped during serialization and we need to fill in an all-encompassing schema.
        if (!config.schemasEnabled()) {
            ObjectNode envelope = JSON_NODE_FACTORY.objectNode();
            envelope.set(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME, null);
            envelope.set(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME, jsonValue);
            jsonValue = envelope;
        }

        Schema schema = asConnectSchema(jsonValue.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        return new SchemaAndValue(
                schema,
                convertToConnect(schema, jsonValue.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME), config)
        );
    }

    public ObjectNode asJsonSchema(Schema schema) {
        if (schema == null)
            return null;

        ObjectNode cached = fromConnectSchemaCache.get(schema);
        if (cached != null)
            return cached;

        final ObjectNode jsonSchema = switch (schema.type()) {
            case BOOLEAN -> JsonSchema.BOOLEAN_SCHEMA.deepCopy();
            case BYTES -> JsonSchema.BYTES_SCHEMA.deepCopy();
            case FLOAT64 -> JsonSchema.DOUBLE_SCHEMA.deepCopy();
            case FLOAT32 -> JsonSchema.FLOAT_SCHEMA.deepCopy();
            case INT8 -> JsonSchema.INT8_SCHEMA.deepCopy();
            case INT16 -> JsonSchema.INT16_SCHEMA.deepCopy();
            case INT32 -> JsonSchema.INT32_SCHEMA.deepCopy();
            case INT64 -> JsonSchema.INT64_SCHEMA.deepCopy();
            case STRING -> JsonSchema.STRING_SCHEMA.deepCopy();
            case ARRAY -> {
                var node = JSON_NODE_FACTORY.objectNode().put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.ARRAY_TYPE_NAME);
                node.set(JsonSchema.ARRAY_ITEMS_FIELD_NAME, asJsonSchema(schema.valueSchema()));
                yield node;
            }
            case MAP -> {
                var node = JSON_NODE_FACTORY.objectNode().put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.MAP_TYPE_NAME);
                node.set(JsonSchema.MAP_KEY_FIELD_NAME, asJsonSchema(schema.keySchema()));
                node.set(JsonSchema.MAP_VALUE_FIELD_NAME, asJsonSchema(schema.valueSchema()));
                yield node;
            }
            case STRUCT -> {
                var node = JSON_NODE_FACTORY.objectNode().put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.STRUCT_TYPE_NAME);
                var fields = JSON_NODE_FACTORY.arrayNode();
                for (var field : schema.fields()) {
                    var fieldJsonSchema = asJsonSchema(field.schema()).deepCopy();
                    fieldJsonSchema.put(JsonSchema.STRUCT_FIELD_NAME_FIELD_NAME, field.name());
                    fields.add(fieldJsonSchema);
                }
                node.set(JsonSchema.STRUCT_FIELDS_FIELD_NAME, fields);
                yield node;
            }
            default -> throw new DataException("Couldn't translate unsupported schema type " + schema + ".");
        };

        jsonSchema.put(JsonSchema.SCHEMA_OPTIONAL_FIELD_NAME, schema.isOptional());
        if (schema.name() != null)
            jsonSchema.put(JsonSchema.SCHEMA_NAME_FIELD_NAME, schema.name());
        if (schema.version() != null)
            jsonSchema.put(JsonSchema.SCHEMA_VERSION_FIELD_NAME, schema.version());
        if (schema.doc() != null)
            jsonSchema.put(JsonSchema.SCHEMA_DOC_FIELD_NAME, schema.doc());
        if (schema.parameters() != null) {
            var jsonSchemaParams = JSON_NODE_FACTORY.objectNode();
            schema.parameters().forEach(jsonSchemaParams::put);
            jsonSchema.set(JsonSchema.SCHEMA_PARAMETERS_FIELD_NAME, jsonSchemaParams);
        }
        if (schema.defaultValue() != null)
            jsonSchema.set(JsonSchema.SCHEMA_DEFAULT_FIELD_NAME, convertToJson(schema, schema.defaultValue()));

        fromConnectSchemaCache.put(schema, jsonSchema);
        return jsonSchema;
    }


    public Schema asConnectSchema(JsonNode jsonSchema) {
        if (jsonSchema.isNull())
            return null;

        Schema cached = toConnectSchemaCache.get(jsonSchema);
        if (cached != null)
            return cached;

        JsonNode schemaTypeNode = jsonSchema.get(JsonSchema.SCHEMA_TYPE_FIELD_NAME);
        if (schemaTypeNode == null || !schemaTypeNode.isTextual())
            throw new DataException("Schema must contain 'type' field");

        final SchemaBuilder builder = switch (schemaTypeNode.textValue()) {
            case JsonSchema.BOOLEAN_TYPE_NAME -> SchemaBuilder.bool();
            case JsonSchema.INT8_TYPE_NAME -> SchemaBuilder.int8();
            case JsonSchema.INT16_TYPE_NAME -> SchemaBuilder.int16();
            case JsonSchema.INT32_TYPE_NAME -> SchemaBuilder.int32();
            case JsonSchema.INT64_TYPE_NAME -> SchemaBuilder.int64();
            case JsonSchema.FLOAT_TYPE_NAME -> SchemaBuilder.float32();
            case JsonSchema.DOUBLE_TYPE_NAME -> SchemaBuilder.float64();
            case JsonSchema.BYTES_TYPE_NAME -> SchemaBuilder.bytes();
            case JsonSchema.STRING_TYPE_NAME -> SchemaBuilder.string();
            case JsonSchema.ARRAY_TYPE_NAME -> {
                var elemSchema = jsonSchema.get(JsonSchema.ARRAY_ITEMS_FIELD_NAME);
                if (elemSchema == null || elemSchema.isNull())
                    throw new DataException("Array schema did not specify the element type");
                yield SchemaBuilder.array(asConnectSchema(elemSchema));
            }
            case JsonSchema.MAP_TYPE_NAME -> {
                var keySchema = jsonSchema.get(JsonSchema.MAP_KEY_FIELD_NAME);
                if (keySchema == null)
                    throw new DataException("Map schema did not specify the key type");
                var valueSchema = jsonSchema.get(JsonSchema.MAP_VALUE_FIELD_NAME);
                if (valueSchema == null)
                    throw new DataException("Map schema did not specify the value type");
                yield SchemaBuilder.map(asConnectSchema(keySchema), asConnectSchema(valueSchema));
            }
            case JsonSchema.STRUCT_TYPE_NAME -> {
                var b = SchemaBuilder.struct();
                var fields = jsonSchema.get(JsonSchema.STRUCT_FIELDS_FIELD_NAME);
                if (fields == null || !fields.isArray())
                    throw new DataException("Struct schema's \"fields\" argument is not an array.");
                for (var field : fields) {
                    var jsonFieldName = field.get(JsonSchema.STRUCT_FIELD_NAME_FIELD_NAME);
                    if (jsonFieldName == null || !jsonFieldName.isTextual())
                        throw new DataException("Struct schema's field name not specified properly");
                    b.field(jsonFieldName.asText(), asConnectSchema(field));
                }
                yield b;
            }
            default -> throw new DataException("Unknown schema type: " + schemaTypeNode.textValue());
        };


        JsonNode schemaOptionalNode = jsonSchema.get(JsonSchema.SCHEMA_OPTIONAL_FIELD_NAME);
        if (schemaOptionalNode != null && schemaOptionalNode.isBoolean() && schemaOptionalNode.booleanValue())
            builder.optional();
        else
            builder.required();

        JsonNode schemaNameNode = jsonSchema.get(JsonSchema.SCHEMA_NAME_FIELD_NAME);
        if (schemaNameNode != null && schemaNameNode.isTextual())
            builder.name(schemaNameNode.textValue());

        JsonNode schemaVersionNode = jsonSchema.get(JsonSchema.SCHEMA_VERSION_FIELD_NAME);
        if (schemaVersionNode != null && schemaVersionNode.isIntegralNumber()) {
            builder.version(schemaVersionNode.intValue());
        }

        JsonNode schemaDocNode = jsonSchema.get(JsonSchema.SCHEMA_DOC_FIELD_NAME);
        if (schemaDocNode != null && schemaDocNode.isTextual())
            builder.doc(schemaDocNode.textValue());

        var schemaParamsNode = jsonSchema.get(JsonSchema.SCHEMA_PARAMETERS_FIELD_NAME);
        if (schemaParamsNode != null && schemaParamsNode.isObject()) {
            schemaParamsNode.fields().forEachRemaining(entry -> {
                if (!entry.getValue().isTextual())
                    throw new DataException("Schema parameters must have string values.");
                builder.parameter(entry.getKey(), entry.getValue().textValue());
            });
        }

        JsonNode schemaDefaultNode = jsonSchema.get(JsonSchema.SCHEMA_DEFAULT_FIELD_NAME);
        if (schemaDefaultNode != null)
            builder.defaultValue(convertToConnect(builder, schemaDefaultNode, config));

        Schema result = builder.build();
        toConnectSchemaCache.put(jsonSchema, result);
        return result;
    }


    /**
     * Convert this object, in the {@link org.apache.kafka.connect.data} format, into a JSON object with an envelope
     * object containing schema and payload fields.
     *
     * @param schema the schema for the data
     * @param value  the value
     * @return JsonNode-encoded version
     */
    private JsonNode convertToJsonWithEnvelope(Schema schema, Object value) {
        return new JsonSchema.Envelope(asJsonSchema(schema), convertToJson(schema, value)).toJsonNode();
    }

    private JsonNode convertToJsonWithoutEnvelope(Schema schema, Object value) {
        return convertToJson(schema, value);
    }

    /**
     * Convert this object, in the {@link org.apache.kafka.connect.data} format, into a JSON object, returning both the
     * schema and the converted object.
     */
    public JsonNode convertToJson(Schema schema, Object value) {
        if (value == null) {
            if (schema == null) // Any schema is valid and we don't have a default, so treat this as an optional schema
                return null;
            if (schema.defaultValue() != null && config.replaceNullWithDefault())
                return convertToJson(schema, schema.defaultValue());
            if (schema.isOptional())
                return JSON_NODE_FACTORY.nullNode();
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }

        if (schema != null && schema.name() != null) {
            LogicalTypeConverter logicalConverter = LOGICAL_CONVERTERS.get(schema.name());
            if (logicalConverter != null)
                return logicalConverter.toJson(schema, value, config);
        }

        try {
            final var schemaType = schema == null ? ConnectSchema.schemaType(value.getClass()) : schema.type();
            if (schemaType == null)
                throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");

            return switch (schemaType) {
                case INT8 -> JSON_NODE_FACTORY.numberNode((Byte) value);
                case INT16 -> JSON_NODE_FACTORY.numberNode((Short) value);
                case INT32 -> JSON_NODE_FACTORY.numberNode((Integer) value);
                case INT64 -> JSON_NODE_FACTORY.numberNode((Long) value);
                case FLOAT32 -> JSON_NODE_FACTORY.numberNode((Float) value);
                case FLOAT64 -> JSON_NODE_FACTORY.numberNode((Double) value);
                case BOOLEAN -> JSON_NODE_FACTORY.booleanNode((Boolean) value);
                case STRING -> JSON_NODE_FACTORY.textNode(((CharSequence) value).toString());
                case BYTES -> {
                    if (value instanceof byte[] bytes)
                        yield JSON_NODE_FACTORY.binaryNode(bytes);
                    else if (value instanceof ByteBuffer buf)
                        yield JSON_NODE_FACTORY.binaryNode(buf.array());
                    else
                        throw new DataException("Invalid type for bytes type: " + value.getClass());
                }
                case ARRAY -> {
                    var collection = (Collection<?>) value;
                    var list = JSON_NODE_FACTORY.arrayNode();
                    var valueSchema = schema == null ? null : schema.valueSchema();
                    collection.forEach(elem -> list.add(convertToJson(valueSchema, elem)));
                    yield list;
                }
                case MAP -> {
                    var map = (Map<?, ?>) value;
                    // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
                    var objectMode = schema != null
                            ? schema.keySchema().type() == Schema.Type.STRING
                            : map.keySet().stream().allMatch(k -> k instanceof String);
                    var keySchema = schema == null ? null : schema.keySchema();
                    var valueSchema = schema == null ? null : schema.valueSchema();
                    if (objectMode) {
                        var obj = JSON_NODE_FACTORY.objectNode();
                        map.forEach((k, v) -> obj.set(convertToJson(keySchema, k).asText(), convertToJson(valueSchema, v)));
                        yield obj;
                    } else {
                        var list = JSON_NODE_FACTORY.arrayNode();
                        map.forEach((k, v) -> list.add(JSON_NODE_FACTORY.arrayNode()
                                .add(convertToJson(keySchema, k))
                                .add(convertToJson(valueSchema, v))));
                        yield list;
                    }
                }
                case STRUCT -> {
                    if (!(value instanceof Struct struct) || !struct.schema().equals(schema))
                        throw new DataException("Mismatching schema.");
                    var obj = JSON_NODE_FACTORY.objectNode();
                    schema.fields().forEach(field -> obj.set(field.name(), convertToJson(field.schema(), struct.get(field))));
                    yield obj;
                }
            };
        } catch (ClassCastException e) {
            var schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
            throw new DataException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
        }
    }


    private static Object convertToConnect(Schema schema, JsonNode jsonValue, JsonConverterConfig config) {
        final Schema.Type schemaType;
        if (schema != null) {
            schemaType = schema.type();
            if (jsonValue == null || jsonValue.isNull()) {
                if (schema.defaultValue() != null && config.replaceNullWithDefault())
                    return schema.defaultValue(); // any logical type conversions should already have been applied
                if (schema.isOptional())
                    return null;
                throw new DataException("Invalid null value for required " + schemaType + " field");
            }
        } else {
            if (jsonValue.isNull() || jsonValue.isMissingNode()) {
                // Special case. With no schema
                return null;
            }
            schemaType = switch (jsonValue.getNodeType()) {
                case BOOLEAN -> Schema.Type.BOOLEAN;
                case NUMBER -> jsonValue.isIntegralNumber() ? Schema.Type.INT64 : Schema.Type.FLOAT64;
                case ARRAY -> Schema.Type.ARRAY;
                case OBJECT -> Schema.Type.MAP;
                case STRING -> Schema.Type.STRING;
                default -> null;
            };
        }

        final JsonToConnectTypeConverter typeConverter = TO_CONNECT_CONVERTERS.get(schemaType);
        if (typeConverter == null)
            throw new DataException("Unknown schema type: " + schemaType);

        if (schema != null && schema.name() != null) {
            LogicalTypeConverter logicalConverter = LOGICAL_CONVERTERS.get(schema.name());
            if (logicalConverter != null)
                return logicalConverter.toConnect(schema, jsonValue);
        }

        return typeConverter.convert(schema, jsonValue, config);
    }

    private interface JsonToConnectTypeConverter {
        Object convert(Schema schema, JsonNode value, JsonConverterConfig config);
    }

    private interface LogicalTypeConverter {
        JsonNode toJson(Schema schema, Object value, JsonConverterConfig config);

        Object toConnect(Schema schema, JsonNode value);
    }
}
