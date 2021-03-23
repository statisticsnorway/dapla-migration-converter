package no.ssb.rawdata.converter.app.migration.json;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.avro.convert.core.FieldDescriptor;

import java.util.function.Function;

class ColumnMapper {
    final String name;
    final Function<JsonNode, Object> jsonToAvroConverter;
    final Function<String, JsonNode> stringToJsonConverter;
    final FieldDescriptor fieldDescriptor;

    ColumnMapper(String name, Function<JsonNode, Object> jsonToAvroConverter, Function<String, JsonNode> stringToJsonConverter, FieldDescriptor fieldDescriptor) {
        this.name = name;
        this.jsonToAvroConverter = jsonToAvroConverter;
        this.stringToJsonConverter = stringToJsonConverter;
        this.fieldDescriptor = fieldDescriptor;
    }
}
