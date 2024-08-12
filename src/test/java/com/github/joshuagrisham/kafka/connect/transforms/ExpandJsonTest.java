package com.github.joshuagrisham.kafka.connect.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.github.joshuagrisham.kafka.connect.transforms.ExpandJson.ConfigName;

public class ExpandJsonTest {

    private final ExpandJson<SourceRecord> xformKey = new ExpandJson.Key<>();
    private final ExpandJson<SourceRecord> xformValue = new ExpandJson.Value<>();

    private final String SIMPLE_JSON = "{\"stringValue\": \"String value\", \"numberValue\": 42, \"booleanValue\": true}";
    private final Schema SIMPLE_JSON_SCHEMA = SchemaBuilder.struct()
        .field("stringValue", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("numberValue", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .field("booleanValue", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
        .optional()
        .build();
    private final Struct SIMPLE_JSON_VALUE = new Struct(SIMPLE_JSON_SCHEMA)
        .put("stringValue", "String value")
        .put("numberValue", 42)
        .put("booleanValue", true);

    private final Schema STRUCT_WITH_JSON_STRING_SCHEMA = SchemaBuilder.struct()
        .field("jsonValue", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("numberValue", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .field("booleanValue", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
        .optional()
        .build();
    private final Struct STRUCT_WITH_JSON_STRING_VALUE = new Struct(STRUCT_WITH_JSON_STRING_SCHEMA)
        .put("jsonValue", SIMPLE_JSON)
        .put("numberValue", 42)
        .put("booleanValue", true);

    private final Schema STRUCT_WITH_JSON_STRUCT_SCHEMA = SchemaBuilder.struct()
        .field("jsonValue", SIMPLE_JSON_SCHEMA)
        .field("numberValue", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .field("booleanValue", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
        .optional()
        .build();
    private final Struct STRUCT_WITH_JSON_STRUCT_VALUE = new Struct(STRUCT_WITH_JSON_STRUCT_SCHEMA)
        .put("jsonValue", SIMPLE_JSON_VALUE)
        .put("numberValue", 42)
        .put("booleanValue", true);

    private final String NESTED_JSON = "{\"numberValue\": 42, \"level1\": {\"level2\": {\"level3Number\": 24, \"level3String\": \"foo\"}}}";
    private final Schema STRUCT_WITH_NESTED_JSON_STRING_SCHEMA = SchemaBuilder.struct()
        .field("jsonValue", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("numberValue", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .field("booleanValue", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
        .optional()
        .build();
    private final Struct STRUCT_WITH_NESTED_JSON_STRING_VALUE = new Struct(STRUCT_WITH_NESTED_JSON_STRING_SCHEMA)
        .put("jsonValue", NESTED_JSON)
        .put("numberValue", 42)
        .put("booleanValue", true);
    

    @AfterEach
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }


    @Test
    public void wholeRecordKeySchemaless() {
        xformKey.configure(Collections.emptyMap());
        SourceRecord source = new SourceRecord(null, null, "topic", 0,
                null, SIMPLE_JSON, null, SIMPLE_JSON);
        SourceRecord transformed = xformKey.apply(source);

        assertNull(transformed.valueSchema());
        assertEquals(source.value(), transformed.value());
        assertEquals(SIMPLE_JSON_SCHEMA, transformed.keySchema());
        assertEquals(SIMPLE_JSON_VALUE, transformed.key());
    }

    @Test
    public void wholeRecordValueSchemaless() {
        xformValue.configure(Collections.emptyMap());
        SourceRecord source = new SourceRecord(null, null, "topic", 0,
                null, SIMPLE_JSON, null, SIMPLE_JSON);
        SourceRecord transformed = xformValue.apply(source);

        assertNull(transformed.keySchema());
        assertEquals(source.key(), transformed.key());
        assertEquals(SIMPLE_JSON_SCHEMA, transformed.valueSchema());
        assertEquals(SIMPLE_JSON_VALUE, transformed.value());
    }

    @Test
    public void primitiveStringKeySchemaless() {
        xformKey.configure(Collections.emptyMap());
        SourceRecord source = new SourceRecord(null, null, "topic", 0,
                null, "\"key\"", null, "\"value\"");
        SourceRecord transformed = xformKey.apply(source);

        assertNull(transformed.valueSchema());
        assertEquals(source.value(), transformed.value());
        assertEquals(SchemaBuilder.OPTIONAL_STRING_SCHEMA, transformed.keySchema());
        assertEquals("key", transformed.key());
    }

    @Test
    public void primitiveStringValueSchemaless() {
        xformValue.configure(Collections.emptyMap());
        SourceRecord source = new SourceRecord(null, null, "topic", 0,
                null, "\"key\"", null, "\"value\"");
        SourceRecord transformed = xformValue.apply(source);

        assertNull(transformed.keySchema());
        assertEquals(source.key(), transformed.key());
        assertEquals(SchemaBuilder.OPTIONAL_STRING_SCHEMA, transformed.valueSchema());
        assertEquals("value", transformed.value());
    }

    @Test
    public void primitiveIntKeySchemaless() {
        xformKey.configure(Collections.emptyMap());
        SourceRecord source = new SourceRecord(null, null, "topic", 0,
                null, 42, null, 24);
        SourceRecord transformed = xformKey.apply(source);

        assertNull(transformed.valueSchema());
        assertEquals(source.value(), transformed.value());
        assertEquals(SchemaBuilder.OPTIONAL_INT32_SCHEMA, transformed.keySchema());
        assertEquals(42, transformed.key());
    }

    @Test
    public void primitiveIntValueSchemaless() {
        xformValue.configure(Collections.emptyMap());
        SourceRecord source = new SourceRecord(null, null, "topic", 0,
                null, 42L, null, 24);
        SourceRecord transformed = xformValue.apply(source);

        assertNull(transformed.keySchema());
        assertEquals(source.key(), transformed.key());
        assertEquals(SchemaBuilder.OPTIONAL_INT32_SCHEMA, transformed.valueSchema());
        assertEquals(24, transformed.value());
    }

    @Test
    public void wholeRecordKeySchemalessFailsFieldsConfig() {
        xformKey.configure(Map.of(ConfigName.FIELDS, "foo"));
        SourceRecord source = new SourceRecord(null, null, "topic", 0,
                null, SIMPLE_JSON, null, SIMPLE_JSON);
        assertThrows(DataException.class, () -> xformKey.apply(source));
    }

    @Test
    public void keySchemaField() {
        xformKey.configure(Map.of(ConfigName.FIELDS, "jsonValue"));
        SourceRecord source = new SourceRecord(null, null, "topic", 0,
            STRUCT_WITH_JSON_STRING_SCHEMA, STRUCT_WITH_JSON_STRING_VALUE, STRUCT_WITH_JSON_STRING_SCHEMA, STRUCT_WITH_JSON_STRING_VALUE);
        SourceRecord transformed = xformKey.apply(source);

        assertEquals(STRUCT_WITH_JSON_STRING_SCHEMA, transformed.valueSchema());
        assertEquals(source.value(), transformed.value());
        assertEquals(STRUCT_WITH_JSON_STRUCT_SCHEMA, transformed.keySchema());
        assertEquals(STRUCT_WITH_JSON_STRUCT_VALUE, transformed.key());
    }

    @Test
    public void valueSchemaField() {
        xformValue.configure(Map.of(ConfigName.FIELDS, "jsonValue"));
        SourceRecord source = new SourceRecord(null, null, "topic", 0,
            STRUCT_WITH_JSON_STRING_SCHEMA, STRUCT_WITH_JSON_STRING_VALUE, STRUCT_WITH_JSON_STRING_SCHEMA, STRUCT_WITH_JSON_STRING_VALUE);
        SourceRecord transformed = xformValue.apply(source);

        assertEquals(STRUCT_WITH_JSON_STRING_SCHEMA, transformed.keySchema());
        assertEquals(source.key(), transformed.key());
        assertEquals(STRUCT_WITH_JSON_STRUCT_SCHEMA, transformed.valueSchema());
        assertEquals(STRUCT_WITH_JSON_STRUCT_VALUE, transformed.value());

    }

    @Test
    public void testSchemaNamePrefix() {
        xformValue.configure(Map.of(ConfigName.FIELDS, "jsonValue", ConfigName.SCHEMA_NAME_PREFIX, "my.prefix.MyConnectRecord"));
        SourceRecord source = new SourceRecord(null, null, "topic", 0,
                null, 42, STRUCT_WITH_NESTED_JSON_STRING_SCHEMA, STRUCT_WITH_NESTED_JSON_STRING_VALUE);
        SourceRecord transformed = xformValue.apply(source);

        //TODO flesh this out with a good test... 
    }

}
