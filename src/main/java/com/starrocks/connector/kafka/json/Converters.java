package com.starrocks.connector.kafka.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.starrocks.connector.kafka.json.JsonConverter.LogicalTypeConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import java.time.*;

import static java.time.format.DateTimeFormatter.*;

public class Converters {
    private static final Duration ONE_DAY = Duration.ofDays(1);
    public static final long NANO = 1;
    public static final long MICRO = 1_000;
    public static final long MILLI = 1_000_000;
    public static final long SECOND = 1_000_000_000;

    public static JsonNode convertDecimal(Schema schema, Object value, JsonConverter converter) {
        if (!(value instanceof java.math.BigDecimal decimal))
            throw new DataException("Decimal: expected BigDecimal, got " + value.getClass());
        return converter.nodeFactory().numberNode(decimal);
    }

    /// Convert date to ISO8601 string
    ///
    /// @see org.apache.kafka.connect.data.Date
    /// @see io.debezium.time.Conversions#toLocalDate(Object)
    public static LogicalTypeConverter forDate() {
        return (Schema schema, Object object, JsonConverter converter) -> {
            var date = toLocalDate(object);
            var s = ISO_LOCAL_DATE.format(date);
            return converter.nodeFactory().textNode(s);
        };
    }

    public static LocalDate toLocalDate(Object value) {
        return switch (value) {
            // Assume the value is the epoch day number
            case java.lang.Number n -> LocalDate.ofEpochDay(n.longValue());
            case java.time.LocalDate d -> d;
            // ignored time part
            case java.time.LocalDateTime dt -> dt.toLocalDate();
            case java.sql.Time t -> throw new DataException("Date: java.sql.Time not supported");
            // ignored time part
            case java.util.Date d -> LocalDate.ofEpochDay(d.getTime() / ONE_DAY.toMillis());
            default -> throw new DataException("Date: unsupported type " + value.getClass());
        };
    }

    /// Convert time to ISO8601 string
    ///
    /// @see org.apache.kafka.connect.data.Time
    /// @see io.debezium.time.Conversions#toLocalTime(Object)
    public static LogicalTypeConverter forTime(long precision) {
        return (Schema schema, Object object, JsonConverter converter) -> {
            var time = toLocalTime(object, precision);
            var s = ISO_LOCAL_TIME.format(time);
            return converter.nodeFactory().textNode(s);
        };
    }

    @SuppressWarnings("deprecation")
    public static LocalTime toLocalTime(Object value, long precision) {
        return switch (value) {
            case java.lang.Number n -> LocalTime.ofNanoOfDay(n.longValue() * precision);
            case java.time.LocalTime t -> t;
            // ignored date part
            case java.time.LocalDateTime dt -> dt.toLocalTime();
            case java.sql.Date d -> throw new DataException("Time: java.sql.Date not supported");
            // separate java.sql.Timestamp from java.util.Date to preserve nanoseconds
            case java.sql.Timestamp ts -> LocalTime.of(
                    ts.getHours(), ts.getMinutes(), ts.getSeconds(),
                    ts.getNanos()
            );
            // ignored date part
            case java.util.Date d -> LocalTime.of(
                    d.getHours(), d.getMinutes(), d.getSeconds(),
                    (int) (d.getTime() % 1_000L * MILLI)
            );
            case java.time.Duration d -> LocalTime.ofNanoOfDay(d.toNanos());
            default -> throw new DataException("Time: unsupported type " + value.getClass());
        };
    }

    /// Convert timestamp/instant to ISO8601 string
    ///
    /// @see org.apache.kafka.connect.data.Timestamp
    /// @see io.debezium.time.Conversions#toLocalDateTime(Object)
    public static LogicalTypeConverter forTimestamp(long precision) {
        return (Schema schema, Object object, JsonConverter converter) -> {
            var datetime = toLocalDateTime(object, precision);
            var s = ISO_LOCAL_DATE_TIME.format(datetime);
            return converter.nodeFactory().textNode(s);
        };
    }

    public static LocalDateTime toLocalDateTime(Object value, long precision) {
        return switch (value) {
            case java.lang.Number n -> {
                var mod = SECOND / precision;
                var seconds = n.longValue() / mod;
                var nanos = (n.longValue() % mod) * precision;
                yield LocalDateTime.ofEpochSecond(seconds, (int) nanos, ZoneOffset.UTC);
            }
            case java.time.Instant i -> i.atZone(ZoneOffset.UTC).toLocalDateTime();
            case java.time.LocalDate d -> d.atStartOfDay();
            case java.time.LocalTime t -> t.atDate(LocalDate.EPOCH);
            case java.time.LocalDateTime dt -> dt;
            case java.time.OffsetDateTime dt -> dt.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
            case java.sql.Timestamp ts -> ts.toLocalDateTime();
            case java.util.Date d -> LocalDateTime.ofInstant(d.toInstant(), ZoneOffset.UTC);
            default -> throw new DataException("Timestamp: unsupported type " + value.getClass());
        };
    }
}
