package no.ssb.rawdata.converter.app.migration.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMetadataClient;
import no.ssb.rawdata.converter.app.migration.MigrationConverter;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Optional.ofNullable;
import static no.ssb.rawdata.converter.util.RawdataMessageAdapter.posAndIdOf;

public class JsonOracleConverter implements MigrationConverter {

    static final ObjectMapper mapper = new ObjectMapper();

    final ValueInterceptorChain valueInterceptorChain;
    final String documentId;
    final byte[] schemaBytes;
    String[] columnNames;
    Schema avroSchema;

    public JsonOracleConverter(ValueInterceptorChain valueInterceptorChain, String documentId, byte[] schemaBytes) {
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

        List<String> columns = new ArrayList<>();
        for (JsonNode node : arrayNode) {
            String name = ofNullable(node.get("name")).map(JsonNode::textValue).orElseThrow();
            String type = ofNullable(node.get("type")).map(JsonNode::textValue).orElseThrow();

            columns.add(name);

            // https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm

            if (Set.of("CHAR", "VARCHAR", "VARCHAR2", "NCHAR", "NVARCHAR2", "DATE").contains(type)) {
                fields.optionalString(name);

            } else if ("BOOLEAN".equals(type)) {
                fields.optionalBoolean(name);

            } else if ("LONG".equals(type)) {
                fields.optionalLong(name);

                // TODO does Toad export with precision. A NUMBER(38) = INTEGER
            } else if ("INTEGER".equals(type)) {
                fields.optionalInt(name);

            } else if ("FLOAT".equals(type)) {
                fields.optionalFloat(name);

            } else if (Set.of("DOUBLE", "NUMBER").contains(type)) {
                fields.optionalDouble(name);

            } else {
                throw new RuntimeException("Type not supported: " + type);
            }
        }
        avroSchema = fields.endRecord();

        columnNames = columns.toArray(String[]::new);

        return avroSchema;
    }

    @Override
    public boolean isConvertible(RawdataMessage rawdataMessage) {
        return true;
    }

    @Override
    public GenericRecord convert(RawdataMessage rawdataMessage) {
        byte[] data = rawdataMessage.get(documentId);

        try (JsonToRecords records = new JsonToRecords(new ByteArrayInputStream(data), columnNames, avroSchema)
                .withValueInterceptor(valueInterceptorChain::intercept)) {

            List<GenericRecord> dataItems = new ArrayList<>();
            records.forEach(dataItems::add);

            return dataItems.get(0);
        } catch (IOException e) {
            throw new JsonConverterException("Error converting CSV data at " + posAndIdOf(rawdataMessage), e);
        }
    }

    public static class JsonConverterException extends RawdataConverterException {
        public JsonConverterException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
