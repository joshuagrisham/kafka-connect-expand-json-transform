# Kafka Connect Expand JSON Transform (SMT)

Kafka Connect Single Message Transform (SMT) to parse JSON objects from given source fields and expand them into appropriate Connect API structures.

## Background

Inspired by [RedHatInsights ExpandJSON](https://github.com/RedHatInsights/expandjsonsmt), but aims to overcome the following issues:

- Reliance on several additional dependencies including BSON for handling parsing of the actual JSON
- Arrays always receive a hard-coded additional Struct layer with a single field with the name `array` in them and there is no way to configure or remove this from the result.
- Schema name and namespace of the generated Structs are empty and cannot be controlled easily for nested structures by other mainstream SMTs (namely [SetSchemaMetadata](https://github.com/apache/kafka/blob/trunk/connect/transforms/src/main/java/org/apache/kafka/connect/transforms/SetSchemaMetadata.java))
- `ExpandJSON` only supports Schema records and only in the Value; schemaless records and keys are not supported.

This new `ExpandJson` SMT tries address the above as follows:

- Use of no additional dependencies outside of what will already be present in the Connect runtime (namely, using only the `org.apache.kafka` artifacts `connect-api`, `connect-transforms`, and `connect-json`).
- Uses existing functionality to parse and convert JSON data via [JsonConverter](https://github.com/apache/kafka/blob/trunk/connect/json/src/main/java/org/apache/kafka/connect/json/JsonConverter.java), and aims to infer the schema in a "standardized" way by implementing roughly the same logic that is suggested in [KIP-301: Schema Inferencing for JsonConverter](https://cwiki.apache.org/confluence/display/KAFKA/KIP-301%3A+Schema+Inferencing+for+JsonConverter); it should be quite trivial to switch to using this feature directly if/once it is implemented.
- No hard-coded struct layer with the field name `array` for arrays (again matching the proposal from KIP-301).
- Ability to set a `schema.name.prefix` in order to control the namespace and local name of the inferred nested schemas.
- Support for expanding JSON in Key as well as value.
- Support for expanding entire key or value content in case of schemaless records.

## Potential improvements

- Support to address nested fields, potentially using a dot-based notation (currently only root-level fields are supported).
- Ability to support specific Map or potentially other fields for schemaless records instead of only whole-record parsing and expansion.
- Possible ability to control which data types are chosen for each inferred type (e.g. should inferred integers be stored as long instead? etc)
- Possible ability to control if fields should be inferred as optional vs required

## Installation

I have not yet really deployed this anywhere, but as this is a single and very tiny JAR file with no other non-Kafka dependencies, then it is quite easy to just take the JAR file from latest release here and put it into your Kafka Connect plugins folder.

You can also fetch it via <https://jitpack.io> using Maven, Gradle, etc if desired.

## Example usage

```sh
"transforms": "ExpandJson",
"transforms.ExpandJson.type": "com.github.joshuagrisham.kafka.connect.transforms.ExpandJson$Value",
"transforms.ExpandJson.fields": "someJsonTextField,anotherJsonTextField"
"transforms.ExpandJson.schema.name.prefix": "com.github.joshuagrisham.kafka.test.MyJsonRecord"
```
