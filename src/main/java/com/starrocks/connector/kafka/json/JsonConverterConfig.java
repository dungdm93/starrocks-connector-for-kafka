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

    public static final String JSON_HANDLING_MODE_CONFIG = "json.handling.mode";
    public static final JsonHandlingMode JSON_HANDLING_MODE_DEFAULT = JsonHandlingMode.STRING;
    private static final String JSON_HANDLING_MODE_DOC =
            "How to handle fields with a JSON logical type (``io.debezium.data.Json``). "
                    + "``string`` (default) keeps the raw JSON text as a string value. "
                    + "``json`` parses and embeds it as a native JSON node.";
    private static final String JSON_HANDLING_MODE_DISPLAY = "JSON Handling Mode";

    private static final String GROUP = "Json Converter";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    JSON_HANDLING_MODE_CONFIG, Type.STRING, JSON_HANDLING_MODE_DEFAULT.name().toLowerCase(),
                    ConfigDef.ValidString.in("string", "json"),
                    Importance.MEDIUM,
                    JSON_HANDLING_MODE_DOC,
                    GROUP, 1,
                    Width.SHORT, JSON_HANDLING_MODE_DISPLAY
            );

    public final JsonHandlingMode jsonHandlingMode;

    public JsonConverterConfig() {
        this(Collections.EMPTY_MAP);
    }

    public JsonConverterConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        jsonHandlingMode = JsonHandlingMode.valueOf(getString(JSON_HANDLING_MODE_CONFIG).toUpperCase());
    }
}
