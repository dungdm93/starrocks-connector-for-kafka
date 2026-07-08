package com.starrocks.connector.kafka.json;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Collections;
import java.util.Map;

public class JsonConverterConfig extends AbstractConfig {
    public enum JsonHandlingMode {
        STRING,
        JSON,
    }

    public enum UuidHandlingMode {
        HEX,
        HEX_DASH,
        BINARY,
        LARGEINT,
    }

    public static final String JSON_HANDLING_MODE_CONFIG = "json.handling.mode";
    public static final JsonHandlingMode JSON_HANDLING_MODE_DEFAULT = JsonHandlingMode.STRING;
    private static final String JSON_HANDLING_MODE_DOC =
            "How to handle fields with a JSON logical type (``io.debezium.data.Json``). "
                    + "``string`` (default) keeps the raw JSON text as a string value. "
                    + "``json`` parses and embeds it as a native JSON node.";
    private static final String JSON_HANDLING_MODE_DISPLAY = "JSON Handling Mode";

    public static final String UUID_HANDLING_MODE_CONFIG = "uuid.handling.mode";
    public static final UuidHandlingMode UUID_HANDLING_MODE_DEFAULT = UuidHandlingMode.HEX_DASH;
    private static final String UUID_HANDLING_MODE_DOC =
            "How to handle fields with a UUID logical type (``io.debezium.data.Uuid``). "
                    + "``hex`` writes 32 hexadecimal characters without dashes. "
                    + "``hex_dash`` (default) writes the standard RFC-4122 string with dashes. "
                    + "``binary`` writes the 16-byte big-endian representation. "
                    + "``largeint`` writes the UUID as an signed 128-bit number, value in range [-2^127, 2^127 - 1].";
    private static final String UUID_HANDLING_MODE_DISPLAY = "UUID Handling Mode";

    private static final String GROUP = "Json Converter";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    JSON_HANDLING_MODE_CONFIG, Type.STRING, JSON_HANDLING_MODE_DEFAULT.name().toLowerCase(),
                    ConfigDef.ValidString.in("string", "json"),
                    Importance.MEDIUM,
                    JSON_HANDLING_MODE_DOC,
                    GROUP, 1,
                    Width.SHORT, JSON_HANDLING_MODE_DISPLAY
            ).define(
                    UUID_HANDLING_MODE_CONFIG, Type.STRING, UUID_HANDLING_MODE_DEFAULT.name().toLowerCase(),
                    ConfigDef.ValidString.in("hex", "hex_dash", "binary", "largeint"),
                    Importance.MEDIUM,
                    UUID_HANDLING_MODE_DOC,
                    GROUP, 1,
                    Width.SHORT, UUID_HANDLING_MODE_DISPLAY
            );

    public final JsonHandlingMode jsonHandlingMode;
    public final UuidHandlingMode uuidHandlingMode;

    public JsonConverterConfig() {
        this(Collections.EMPTY_MAP);
    }

    public JsonConverterConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        jsonHandlingMode = JsonHandlingMode.valueOf(getString(JSON_HANDLING_MODE_CONFIG).toUpperCase());
        uuidHandlingMode = UuidHandlingMode.valueOf(getString(UUID_HANDLING_MODE_CONFIG).toUpperCase());
    }
}
