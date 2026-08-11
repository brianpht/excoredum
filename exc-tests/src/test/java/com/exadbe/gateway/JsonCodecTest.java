package com.exadbe.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.gateway.codec.JsonReader;
import com.exadbe.gateway.codec.JsonWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Round-trip and failure coverage for the gateway's minimal JSON codec pair. */
class JsonCodecTest {

    private static String written(final JsonWriter writer) {
        return new String(writer.buffer(), 0, writer.length(), StandardCharsets.US_ASCII);
    }

    @Test
    void writesNestedStructures() {
        final JsonWriter writer = new JsonWriter(64);
        writer.beginObject()
                .name("ticket")
                .valueLong(0L)
                .name("data")
                .beginObject()
                .name("levels")
                .beginArray()
                .valueLong(1L)
                .valueDecimal(12345L, 2)
                .valueString("bid")
                .endArray()
                .name("ok")
                .valueBoolean(true)
                .name("nothing")
                .valueNull()
                .endObject()
                .name("empty")
                .beginArray()
                .endArray()
                .endObject();

        assertEquals(
                "{\"ticket\":0,\"data\":{\"levels\":[1,123.45,\"bid\"],\"ok\":true,\"nothing\":null},\"empty\":[]}",
                written(writer));
    }

    @Test
    void escapesStrings() {
        final JsonWriter writer = new JsonWriter(64);
        writer.beginObject().name("k").valueString("a\"b\\c\nd").endObject();
        assertEquals("{\"k\":\"a\\\"b\\\\c\\u000ad\"}", written(writer));
    }

    @Test
    void growsBufferOnDemand() {
        final JsonWriter writer = new JsonWriter(64);
        writer.beginArray();
        for (int i = 0; i < 1000; i++) {
            writer.valueLong(i);
        }
        writer.endArray();
        assertTrue(written(writer).startsWith("[0,1,2,"));
        assertTrue(written(writer).endsWith(",999]"));
    }

    @Test
    void readsFlatObjectWithScalarsAndSkips() {
        final String body = "{\"uid\":42,\"price\":\"123.45\",\"note\":\"he\\u006Clo\","
                + "\"nested\":{\"deep\":[1,2,{\"x\":\"y\"}]},\"list\":[true,null],\"size\":7}";
        final JsonReader reader = new JsonReader();
        reader.wrap(body.getBytes(StandardCharsets.US_ASCII), body.length());

        final Map<String, String> fields = new HashMap<>();
        assertTrue(reader.beginObject());
        while (reader.hasNextField()) {
            final String name = reader.fieldName();
            switch (name) {
                case "uid":
                    fields.put(name, Long.toString(reader.nextLong()));
                    break;
                case "price":
                    fields.put(name, reader.nextToken());
                    break;
                case "note":
                    fields.put(name, reader.nextString());
                    break;
                case "size":
                    fields.put(name, Long.toString(reader.nextLong()));
                    break;
                default:
                    reader.skipValue();
                    break;
            }
        }
        assertFalse(reader.failed());
        assertEquals("42", fields.get("uid"));
        assertEquals("123.45", fields.get("price"));
        assertEquals("hello", fields.get("note"));
        assertEquals("7", fields.get("size"));
    }

    @Test
    void tokenAcceptsBareNumbers() {
        final String body = "{\"amount\":0.05}";
        final JsonReader reader = new JsonReader();
        reader.wrap(body.getBytes(StandardCharsets.US_ASCII), body.length());
        assertTrue(reader.beginObject());
        assertTrue(reader.hasNextField());
        assertEquals("amount", reader.fieldName());
        assertEquals("0.05", reader.nextToken());
        assertFalse(reader.hasNextField());
        assertFalse(reader.failed());
    }

    @Test
    void malformedInputSetsFailedFlag() {
        final String[] bad = {
            "{\"uid\":", "{\"uid\" 42}", "{uid:42}", "[1,2]", "{\"a\":1", "{\"a\":1]}", "{\"a\":1,\"b\"",
        };
        for (final String sample : bad) {
            final JsonReader reader = new JsonReader();
            reader.wrap(sample.getBytes(StandardCharsets.US_ASCII), sample.length());
            if (reader.beginObject()) {
                while (reader.hasNextField()) {
                    reader.fieldName();
                    reader.skipValue();
                }
            }
            assertTrue(reader.failed(), sample);
        }
    }

    @Test
    void rejectsUnterminatedString() {
        final String body = "{\"a\":\"abc}";
        final JsonReader reader = new JsonReader();
        reader.wrap(body.getBytes(StandardCharsets.US_ASCII), body.length());
        assertTrue(reader.beginObject());
        assertTrue(reader.hasNextField());
        assertEquals("a", reader.fieldName());
        reader.nextString();
        assertTrue(reader.failed());
    }
}
