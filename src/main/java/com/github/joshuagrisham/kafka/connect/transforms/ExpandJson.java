package com.github.joshuagrisham.kafka.connect.transforms;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonSchema;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ExpandJson<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Parses the specified fields as JSON objects and converts them to nested structures with appropriate Connect "
                    + "schema types. All inferred fields will be assumed to be optional."
                    + "<p/>When not using a schema, only parsing the entire value is supported at this time."
                    + "<p/>When using a schema, only root-level fields are supported at this time."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName()
                    + "</code>) or value (<code>" + Value.class.getName() + "</code>).";

    // TODO: Ideally the field names could support nested fields; this can be tricky in practice, so for now this only supports root-level fields
    // Otherwise docstring could have something like this in it:
    //  + "<p/>When using a schema, dot-based notation can be used for addressing nested fields (e.g. level1.level2); use backslash to escape dots which exist as part of the field name itself."


    public interface ConfigName {
        String FIELDS = "fields";
        String SCHEMA_NAME_PREFIX = "schema.name.prefix";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELDS, ConfigDef.Type.LIST, null, ConfigDef.Importance.MEDIUM,
                    "Names of fields to be parsed as JSON objects, or empty to parse the entire value.")
            .define(ConfigName.SCHEMA_NAME_PREFIX, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, 
                    "Fully-qualified (namespace and local name) schema name prefix to use for any newly-generated nested "
                            + "struct schemas. If only one nested struct schema is generated, the "
                            + ConfigName.SCHEMA_NAME_PREFIX + " will be used as-is. For each additional generated schema, "
                            + "a number will be appended to the given prefix starting from the bottom up.");

    private static final String PURPOSE = "expand JSON fields";

    private final ObjectMapper MAPPER = new ObjectMapper();
    private JsonConverter JSONCONVERTER = new JsonConverter();

    private List<String> fields;
    private String schemaNamePrefix;

    // When using SCHEMA_NAME_PREFIX, schema names generated will be unique per unique List<Field> of the schema itself
    private Map<List<Field>, String> generatedSchemaNames;
    private int schemaNameIndex = 0;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fields = config.getList(ConfigName.FIELDS);
        schemaNamePrefix = config.getString(ConfigName.SCHEMA_NAME_PREFIX);
        generatedSchemaNames = new LinkedHashMap<>();
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
        JSONCONVERTER.configure(Map.of("schemas.enable", true, "converter.type", recordType()));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        if (fields != null && !fields.isEmpty()) {
            throw new DataException("Expanding specific fields is not supported for schemaless records. Try transforming the entire "
                    + "value or make the necessary adjustments so that your records will have a schema.");
        }

        SchemaAndValue schemaAndValue = JSONCONVERTER.toConnectData(record.topic(),
            getInferredEnvelopeValue(operatingValue(record))); // inferring envelope schema+value here as KIP-301 is not yet implemented

        return newRecord(record, schemaAndValue.schema(), schemaAndValue.value());
    }

    private R applyWithSchema(R record) {
        if (fields == null || fields.isEmpty()) {
            throw new DataException("Expanding the entire value is not supported for records with a schema. Try transforming one or "
                    + "more specific fields or make the necessary adjustments so that your records will not have a schema.");
        }

        final Schema currentSchema = operatingSchema(record);
        final Struct currentValue = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = getOrBuildSchema(record.topic(), currentSchema, currentValue);

        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : updatedSchema.schema().fields()) {
            if (fields.contains(field.name())) {
                SchemaAndValue newSchemaAndValue = JSONCONVERTER.toConnectData(record.topic(),
                    getInferredEnvelopeValue(currentValue.get(field.name()))); // inferring envelope schema+value here as KIP-301 is not yet implemented
                updatedValue.put(field, newSchemaAndValue.value());
            } else {
                updatedValue.put(field, currentValue.get(field));
            }
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema getOrBuildSchema(String topic, Schema currentSchema, Struct currentValue) {
        Schema updatedSchema = schemaUpdateCache.get(currentSchema);
        if (updatedSchema != null)
            return updatedSchema;

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(currentSchema, SchemaBuilder.struct());
        for (Field field : currentSchema.fields()) {
            if (fields.contains(field.name())) {
                SchemaAndValue newSchemaAndValue = JSONCONVERTER.toConnectData(topic,
                    getInferredEnvelopeValue(currentValue.get(field.name()))); // inferring envelope schema+value here as KIP-301 is not yet implemented
                builder.field(field.name(), newSchemaAndValue.schema());
            } else {
                builder.field(field.name(), field.schema());
            }
        }

        if (currentSchema.isOptional())
            builder.optional();

        updatedSchema = builder.build();
        schemaUpdateCache.put(currentSchema, updatedSchema);
        return updatedSchema;
    }

    // Since KIP-301 (Schema Inferencing for JsonConverter) is still not implemented,
    // then we will pull in some of the proposal here
    // References:
    //  https://cwiki.apache.org/confluence/display/KAFKA/KIP-301%3A+Schema+Inferencing+for+JsonConverter
    //  https://github.com/apache/kafka/pull/5001

    // Ideally we could just the JsonConverter with inferred schema support directly (after KIP-301), but for now the current
    // JsonConverter requires an "enveloped" payload (schema+payload); we will create one using a version of the inferSchema()
    // from the KIP-301 proposal
    private byte[] getInferredEnvelopeValue(Object value) {

        JsonNode valueJson;
        try {
            valueJson = MAPPER.readTree(value.toString());
        } catch (JsonProcessingException e) {
            throw new DataException("Exception when attempting to parse value as JSON", e);
        }

        Schema inferredSchema = inferSchema(valueJson);
        ObjectNode inferredSchemaJson = JSONCONVERTER.asJsonSchema(inferredSchema);
        byte[] envelopedValue;
        try {
            envelopedValue = MAPPER.writeValueAsBytes(JsonSchema.envelope(inferredSchemaJson, valueJson));
        } catch (JsonProcessingException e) {
            throw new DataException("Exception when creating schema-value envelope", e);
        }

        // Now envelopedValue is a proper JsonSchema-formatted payload that the current implementation of JsonConverter can use
        return envelopedValue;

    }

    // copy of inferSchema() from the KIP-301 proposal but slightly adjusted as follows:
    // - assume that fields should always be optional (as it seems almost impossible to determine this beforehand)
    // - handle Schema Name for new Structs if desired via SCHEMA_NAME_PREFIX config
    // - more discreet handling of NUMBER types (instead of only picking INT64 vs FLOAT64)
    private Schema inferSchema(JsonNode jsonValue) {
        switch (jsonValue.getNodeType()) {
            case NULL:
                return Schema.OPTIONAL_STRING_SCHEMA;

            case BOOLEAN:
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;

            case NUMBER:
                if (jsonValue.isIntegralNumber()) {
                    if (jsonValue.isBigInteger()) // use String for BigIntegers
                        return Schema.OPTIONAL_STRING_SCHEMA;
                    if (jsonValue.isInt()) // prefer Integer if assumed int
                        return Schema.OPTIONAL_INT32_SCHEMA;
                    if (jsonValue.isShort()) // prefer Integer if assumed short
                        return Schema.OPTIONAL_INT32_SCHEMA;

                    // otherwise use Long
                    return Schema.OPTIONAL_INT64_SCHEMA;
                }
                else {
                    if (jsonValue.isBigDecimal()) // use String for BigDecimals
                        return Schema.OPTIONAL_STRING_SCHEMA;

                    // otherwise use Float
                    return Schema.OPTIONAL_FLOAT64_SCHEMA;
                }

            case ARRAY:
                SchemaBuilder arrayBuilder = SchemaBuilder.array(jsonValue.elements().hasNext() ? inferSchema(jsonValue.elements().next()) : Schema.OPTIONAL_STRING_SCHEMA);
                arrayBuilder.optional();
                return arrayBuilder.build();

            case OBJECT:
                SchemaBuilder structBuilder = SchemaBuilder.struct();
                Iterator<Map.Entry<String, JsonNode>> it = jsonValue.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> entry = it.next();
                    structBuilder.field(entry.getKey(), inferSchema(entry.getValue()));
                }
                structBuilder.optional();
                
                // Handle generating and setting schema name if configured to do so
                if (schemaNamePrefix != null && !schemaNamePrefix.isBlank()) {
                    if (generatedSchemaNames.get(structBuilder.fields()) == null) {
                        // Use roughly the same logic as Confluent used in AvroData to increment the schema name
                        // see: https://github.com/confluentinc/schema-registry/blob/6b44e90c8f2295af24e02a0a67e5e7fd29c6af53/avro-data/src/main/java/io/confluent/connect/avro/AvroData.java#L1105
                        int nameIndex = ++schemaNameIndex;
                        String name = schemaNamePrefix + (nameIndex > 1 ? nameIndex : "");
                        generatedSchemaNames.put(structBuilder.fields(), name);
                        structBuilder.name(name);
                    } else {
                        structBuilder.name(generatedSchemaNames.get(structBuilder.fields()));
                    }
                }

                return structBuilder.build();

            case STRING:
                return Schema.OPTIONAL_STRING_SCHEMA;

            case BINARY:
            case MISSING:
            case POJO:
            default:
                return null;
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    protected abstract String recordType();

    public static final class Key<R extends ConnectRecord<R>> extends ExpandJson<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

        @Override
        protected String recordType() {
            return "key";
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends ExpandJson<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

        @Override
        protected String recordType() {
            return "value";
        }
    }

}
