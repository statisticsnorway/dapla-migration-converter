package no.ssb.rawdata.converter.app.migration.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.TextNode;
import no.ssb.avro.convert.core.FieldDescriptor;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMetadataClient;
import no.ssb.rawdata.converter.app.migration.MigrationConverter;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import static java.util.Optional.ofNullable;

public class JsonKostraConverter implements MigrationConverter {

    static final ObjectMapper mapper = new ObjectMapper();

    final ValueInterceptorChain valueInterceptorChain;
    final String documentId;
    final byte[] schemaBytes;
    Schema avroSchema;
    ColumnMapper[] columnMappers;

    public JsonKostraConverter(ValueInterceptorChain valueInterceptorChain, String documentId, byte[] schemaBytes) {
        this.valueInterceptorChain = valueInterceptorChain;
        this.documentId = documentId;
        this.schemaBytes = schemaBytes;
    }

    static class ColumnMapper {
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

    @Override
    public Schema init(RawdataMetadataClient metadataClient) {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(documentId).fields();

        ArrayNode arrayNode;
        try {
            arrayNode = mapper.readValue(new StringReader(new String(schemaBytes, StandardCharsets.UTF_8)), ArrayNode.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        columnMappers = new ColumnMapper[arrayNode.size()];

        int i = 0;
        for (JsonNode node : arrayNode) {
            String name = ofNullable(node.get("name")).map(JsonNode::textValue).orElseThrow();
            String type = ofNullable(node.get("type")).map(JsonNode::textValue).orElseThrow();

            if ("STRING".contains(type)) {
                fields.optionalString(name);
                columnMappers[i] = new ColumnMapper(name, JsonNode::textValue, TextNode::valueOf, new FieldDescriptor(name));

            } else if ("BOOLEAN".equals(type)) {
                fields.optionalBoolean(name);
                columnMappers[i] = new ColumnMapper(name, JsonNode::booleanValue, str -> BooleanNode.valueOf(Boolean.parseBoolean(str)), new FieldDescriptor(name));

            } else if ("LONG".equals(type)) {
                fields.optionalLong(name);
                columnMappers[i] = new ColumnMapper(name, JsonNode::longValue, str -> LongNode.valueOf(Long.parseLong(str)), new FieldDescriptor(name));

            } else if ("INTEGER".equals(type)) {
                fields.optionalInt(name);
                columnMappers[i] = new ColumnMapper(name, JsonNode::intValue, str -> IntNode.valueOf(Integer.parseInt(str)), new FieldDescriptor(name));

            } else if ("FLOAT".equals(type)) {
                fields.optionalFloat(name);
                columnMappers[i] = new ColumnMapper(name, JsonNode::floatValue, str -> FloatNode.valueOf(Float.parseFloat(str)), new FieldDescriptor(name));

            } else if ("NUMERIC".contains(type)) {
                fields.optionalDouble(name);
                columnMappers[i] = new ColumnMapper(name, JsonNode::doubleValue, str -> DoubleNode.valueOf(Double.parseDouble(str)), new FieldDescriptor(name));

            } else {
                throw new RuntimeException("Type not supported: " + type);
            }

            i++;
        }

        avroSchema = fields.endRecord();

        return avroSchema;
    }

    @Override
    public boolean isConvertible(RawdataMessage rawdataMessage) {
        return true;
    }

    @Override
    public GenericRecord convert(RawdataMessage rawdataMessage) {
        byte[] data = rawdataMessage.get(documentId);

        ArrayNode arrayNode;
        try {
            arrayNode = (ArrayNode) mapper.readTree(data);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);

        for (int i = 0; i < columnMappers.length; i++) {
            ColumnMapper columnMapper = columnMappers[i];
            ofNullable(arrayNode.get(i))
                    .map(jsonNode -> valueInterceptorChain.intercept(columnMapper.fieldDescriptor, asText(jsonNode)))
                    .map(columnMapper.stringToJsonConverter)
                    .map(columnMapper.jsonToAvroConverter)
                    .ifPresent(value -> recordBuilder.set(columnMapper.name, value));
        }

        return recordBuilder.build();
    }

    static String asText(JsonNode node) {
        if (node.isNull()) {
            return null;
        }
        return node.asText();
    }
}
