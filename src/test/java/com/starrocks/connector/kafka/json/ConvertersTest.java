package com.starrocks.connector.kafka.json;

import org.apache.kafka.connect.errors.DataException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.math.BigDecimal;
import java.time.*;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import static com.starrocks.connector.kafka.json.Converters.*;
import static java.time.temporal.ChronoUnit.MICROS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ConvertersTest {
    private static final LocalDate DATE = LocalDate.of(2024, 6, 15);
    private static final LocalTime TIME = LocalTime.of(1, 23, 45, 192837465);
    private static final LocalDateTime DATE_TIME = LocalDateTime.of(2024, 6, 15, 1, 23, 45, 192837465);

    private static final LocalDate DATE_PRE_EPOCH = LocalDate.of(1969, 12, 31);
    private static final LocalDateTime DATE_TIME_PRE_EPOCH = LocalDateTime.of(1969, 12, 31, 1, 23, 45, 192837465);

    @Parameters(name = "{0}")
    public static Collection<Object[]> timezones() {
        return List.<Object[]>of(
                new Object[]{"UTC"},
                new Object[]{"Asia/Ho_Chi_Minh"}
        );
    }

    private final TimeZone timezone;
    private TimeZone savedTimeZone;
    private final JsonConverter converter = new JsonConverter();

    public ConvertersTest(String tzname) {
        this.timezone = TimeZone.getTimeZone(tzname);
    }

    @Before
    public void setTimeZone() {
        savedTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(timezone);
    }

    @After
    public void restoreTimeZone() {
        TimeZone.setDefault(savedTimeZone);
    }

    // --- convertDecimal ---

    @Test
    public void convertDecimal_bigDecimal_returnsNumberNode() {
        var value = new BigDecimal("123.456");
        var node = Converters.convertDecimal(null, value, converter);
        assertEquals(new BigDecimal("123.456"), node.decimalValue());
    }

    @Test(expected = DataException.class)
    public void convertDecimal_nonBigDecimal_throws() {
        Converters.convertDecimal(null, "not-a-decimal", converter);
    }

    // --- toLocalDate ---

    @Test
    public void toLocalDate_epochDayNumber() {
        assertEquals(LocalDate.EPOCH, toLocalDate(0));
        assertEquals(DATE, toLocalDate(DATE.toEpochDay()));
        assertEquals(DATE_PRE_EPOCH, toLocalDate(DATE_PRE_EPOCH.toEpochDay()));
    }

    @Test(expected = DataException.class)
    public void toLocalDate_sqlTime_throws() {
        toLocalDate(new java.sql.Time(0));
    }


    @Test
    public void toLocalDate_utilDate() {
        // ignore time part
        var date = new java.util.Date(DATE_TIME.toInstant(ZoneOffset.UTC).toEpochMilli());
        assertEquals(DATE, toLocalDate(date));

        // Non-midnight time to expose floor-division bug: 1969-12-31T01:23:45Z
        date = new java.util.Date(DATE_TIME_PRE_EPOCH.toInstant(ZoneOffset.UTC).toEpochMilli());
        assertEquals(DATE_PRE_EPOCH, toLocalDate(date));
    }

    @Test
    public void toLocalDate_localDate() {
        assertEquals(DATE, toLocalDate(DATE));
    }

    @Test
    public void toLocalDate_localDateTime_ignoresTime() {
        assertEquals(DATE, toLocalDate(DATE_TIME));
    }

    @Test
    public void toLocalDate_epochDayNumber_preEpoch() {
        assertEquals(DATE_PRE_EPOCH, toLocalDate(DATE_PRE_EPOCH.toEpochDay()));
    }

    @Test(expected = DataException.class)
    public void toLocalDate_unsupportedType_throws() {
        toLocalDate("unsupported");
    }

    // --- toLocalTime ---

    @Test
    public void toLocalTime_number() {
        {
            var nano = TIME.toNanoOfDay();
            assertEquals(TIME, toLocalTime(nano, NANO));
            assertEquals(TIME.truncatedTo(MICROS), toLocalTime(nano / MICRO, MICRO));
            assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(nano / MILLI, MILLI));
        }

        {
            var dt = DATE_TIME.withYear(1970).withMonth(1).withDayOfMonth(2);
            var nano = dt.toEpochSecond(ZoneOffset.UTC) * SECOND + dt.getNano();
            assertEquals(TIME, toLocalTime(nano, NANO));
            assertEquals(TIME.truncatedTo(MICROS), toLocalTime(nano / MICRO, MICRO));
            assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(nano / MILLI, MILLI));
        }

        {
            var dt = DATE_TIME_PRE_EPOCH;
            var nano = dt.toEpochSecond(ZoneOffset.UTC) * SECOND + dt.getNano();
            assertEquals(TIME, toLocalTime(nano, NANO));
            assertEquals(TIME.truncatedTo(MICROS), toLocalTime(Math.floorDiv(nano, MICRO), MICRO));
            assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(Math.floorDiv(nano, MILLI), MILLI));
        }
    }

    @Test(expected = DataException.class)
    public void toLocalTime_sqlDate_throws() {
        toLocalTime(new java.sql.Date(0), MILLI);
    }

    @Test
    public void toLocalTime_sqlTime() {
        var time = new java.sql.Time(TIME.toNanoOfDay() / MILLI);
        assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(time, MILLI));
    }

    @Test
    public void toLocalTime_sqlTimestamp() {
        {
            var ts = new java.sql.Timestamp(TIME.toNanoOfDay() / MILLI);
            ts.setNanos(TIME.getNano());
            assertEquals(TIME, toLocalTime(ts, MILLI));
        }

        {
            var i = DATE_TIME.toInstant(ZoneOffset.UTC);
            var ts = new java.sql.Timestamp(i.toEpochMilli());
            ts.setNanos(i.getNano());
            assertEquals(TIME, toLocalTime(ts, MILLI));
        }

        {
            var i = DATE_TIME_PRE_EPOCH.toInstant(ZoneOffset.UTC);
            var ts = new java.sql.Timestamp(i.toEpochMilli());
            ts.setNanos(i.getNano());
            assertEquals(TIME, toLocalTime(ts, MILLI));
        }
    }

    @Test
    public void toLocalTime_utilDate() {
        {
            var ts = new java.util.Date(TIME.toNanoOfDay() / MILLI);
            assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(ts, MILLI));
        }

        {
            var i = DATE_TIME.toInstant(ZoneOffset.UTC);
            var ts = new java.util.Date(i.toEpochMilli());
            assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(ts, MILLI));
        }

        {
            var i = DATE_TIME_PRE_EPOCH.toInstant(ZoneOffset.UTC);
            var ts = new java.util.Date(i.toEpochMilli());
            assertEquals(TIME.truncatedTo(MILLIS), toLocalTime(ts, MILLI));
        }
    }

    @Test
    public void toLocalTime_localTime() {
        assertEquals(TIME, toLocalTime(TIME, MILLI));
    }

    @Test
    public void toLocalTime_localDateTime() {
        assertEquals(TIME, toLocalTime(DATE_TIME, MILLI));
    }

    @Test
    public void toLocalTime_duration() {
        var duration = Duration
                .ofHours(1).plusMinutes(23).plusSeconds(45)
                .plusNanos(192837465);
        assertEquals(TIME, toLocalTime(duration, MILLI));
    }

    @Test(expected = DataException.class)
    public void toLocalTime_unsupportedType_throws() {
        toLocalTime("unsupported", MILLI);
    }

    // --- toLocalDateTime ---

    @Test
    public void toLocalDateTime_number() {
        var i = DATE_TIME.toInstant(ZoneOffset.UTC);
        long millis = i.toEpochMilli();
        assertEquals(DATE_TIME.truncatedTo(MILLIS), toLocalDateTime(millis, MILLI));

        long micros = i.getEpochSecond() * (SECOND / MICRO) + DATE_TIME.getNano() / MICRO;
        assertEquals(DATE_TIME.truncatedTo(MICROS), toLocalDateTime(micros, MICRO));

        long nanos = i.getEpochSecond() * SECOND + DATE_TIME.getNano();
        assertEquals(DATE_TIME, toLocalDateTime(nanos, NANO));

        // pre-epoch
        i = DATE_TIME_PRE_EPOCH.toInstant(ZoneOffset.UTC);
        millis = i.toEpochMilli();
        assertEquals(DATE_TIME_PRE_EPOCH.truncatedTo(MILLIS), toLocalDateTime(millis, MILLI));

        micros = i.getEpochSecond() * (SECOND / MICRO) + DATE_TIME_PRE_EPOCH.getNano() / MICRO;
        assertEquals(DATE_TIME_PRE_EPOCH.truncatedTo(MICROS), toLocalDateTime(micros, MICRO));

        nanos = i.getEpochSecond() * SECOND + DATE_TIME_PRE_EPOCH.getNano();
        assertEquals(DATE_TIME_PRE_EPOCH, toLocalDateTime(nanos, NANO));
    }

    @Test
    public void toLocalDateTime_instant() {
        assertEquals(DATE_TIME, toLocalDateTime(DATE_TIME.toInstant(ZoneOffset.UTC), MILLI));
    }

    @Test
    public void toLocalDateTime_localDate_startOfDay() {
        assertEquals(DATE.atStartOfDay(), toLocalDateTime(DATE, MILLI));
    }

    @Test
    public void toLocalDateTime_localTime_epochDate() {
        assertEquals(LocalDateTime.of(LocalDate.EPOCH, TIME), toLocalDateTime(TIME, MILLI));
    }

    @Test
    public void toLocalDateTime_localDateTime() {
        assertEquals(DATE_TIME, toLocalDateTime(DATE_TIME, MILLI));
    }

    @Test
    public void toLocalDateTime_offsetDateTime_normalizesToUtc() {
        var offsetDt = OffsetDateTime.of(DATE_TIME, ZoneOffset.ofHours(7));
        assertEquals(DATE_TIME.minusHours(7), toLocalDateTime(offsetDt, MILLI));
    }

    @Test
    public void toLocalDateTime_utilDate() {
        var date = new java.util.Date(DATE_TIME.toInstant(ZoneOffset.UTC).toEpochMilli());
        assertEquals(DATE_TIME.truncatedTo(MILLIS), toLocalDateTime(date, MILLI));

        // pre-epoch
        date = new java.util.Date(DATE_TIME_PRE_EPOCH.toInstant(ZoneOffset.UTC).toEpochMilli());
        assertEquals(DATE_TIME_PRE_EPOCH.truncatedTo(MILLIS), toLocalDateTime(date, MILLI));
    }

    @Test
    public void toLocalDateTime_sqlTimestamp() {
        var ts = new java.sql.Timestamp(DATE_TIME.toInstant(ZoneOffset.UTC).toEpochMilli());
        ts.setNanos(DATE_TIME.getNano());
        assertEquals(DATE_TIME, toLocalDateTime(ts, MILLI));

        // pre-epoch
        ts = new java.sql.Timestamp(DATE_TIME_PRE_EPOCH.toInstant(ZoneOffset.UTC).toEpochMilli());
        ts.setNanos(DATE_TIME_PRE_EPOCH.getNano());
        assertEquals(DATE_TIME_PRE_EPOCH, toLocalDateTime(ts, MILLI));
    }

    @Test(expected = DataException.class)
    public void toLocalDateTime_unsupportedType_throws() {
        toLocalDateTime("unsupported", MILLI);
    }
}
