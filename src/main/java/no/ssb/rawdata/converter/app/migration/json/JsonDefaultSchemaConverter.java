package no.ssb.rawdata.converter.app.migration.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
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

import static java.util.Optional.ofNullable;

public class JsonDefaultSchemaConverter implements MigrationConverter {

    static final ObjectMapper mapper = new ObjectMapper();

    final ValueInterceptorChain valueInterceptorChain;
    final String documentId;
    final byte[] schemaBytes;
    Schema avroSchema;
    ColumnMapper[] columnMappers;

    public JsonDefaultSchemaConverter(ValueInterceptorChain valueInterceptorChain, String documentId, byte[] schemaBytes) {
        this.valueInterceptorChain = valueInterceptorChain;
        this.documentId = documentId;
        this.schemaBytes = schemaBytes;
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

            switch (JsonNodeType.valueOf(type)) {
                case STRING -> {
                    fields.optionalString(name);
                    columnMappers[i] = new ColumnMapper(name, JsonNode::textValue, TextNode::valueOf, new FieldDescriptor(name));
                    break;
                }
                case BOOLEAN -> {
                    fields.optionalBoolean(name);
                    columnMappers[i] = new ColumnMapper(name, JsonNode::booleanValue, str -> BooleanNode.valueOf(Boolean.parseBoolean(str)), new FieldDescriptor(name));
                    break;
                }
                case NUMBER -> {
                    try {
                        columnMappers[i] = new ColumnMapper(name, JsonNode::longValue, str -> LongNode.valueOf(Long.parseLong(str)), new FieldDescriptor(name));
                        fields.optionalLong(name);
                    } catch (NumberFormatException e) {
                        columnMappers[i] = new ColumnMapper(name, JsonNode::doubleValue, str -> DoubleNode.valueOf(Double.parseDouble(str)), new FieldDescriptor(name));
                        fields.optionalDouble(name);
                    }
                    break;
                }
                case BINARY -> {
                    fields.optionalBytes(name);
                    columnMappers[i] = new ColumnMapper(name, jsonNode -> {
                        try {
                            return jsonNode.binaryValue();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }, s -> BinaryNode.valueOf(s.getBytes(StandardCharsets.UTF_8)), new FieldDescriptor(name));
                    break;
                }
                case OBJECT, ARRAY, POJO, MISSING, NULL -> {
                    throw new UnsupportedOperationException();
                }
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

        JsonNode arrayNode;
        try {
            arrayNode = mapper.readTree(data);
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
