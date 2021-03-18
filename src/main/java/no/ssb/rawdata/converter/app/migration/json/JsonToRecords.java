package no.ssb.rawdata.converter.app.migration.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaAwareElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import no.ssb.avro.convert.core.ValueInterceptor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class JsonToRecords implements AutoCloseable, Iterable<GenericRecord> {

    static final ObjectMapper mapper = new ObjectMapper();

    private final JsonNode rootNode;
    private final String[] columnNames;
    private final SchemaBuddy schemaBuddy;
    private ValueInterceptor valueInterceptor;
    private JsonToRecords.Callback callBack; // this is never called. Used for debug purposes? Ref CsvToRecords.withCallBack

    public JsonToRecords(InputStream inputStream, String[] columnNames, Schema schema) throws IOException {
        this.columnNames = columnNames;
        final InputStreamReader source = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        source.mark(1);
        char[] firstChar = new char[1];
        if (source.read(firstChar, 0, 1) == -1) {
            throw new IllegalStateException("Content is empty or illegal!");
        }
        source.reset(); // costly operation but convenient

        Class<? extends JsonNode> rootNodeClass = '{' == firstChar[0] ? ObjectNode.class : ArrayNode.class;
        rootNode = mapper.readValue(source, rootNodeClass);
        this.schemaBuddy = SchemaBuddy.parse(schema);
    }

    public JsonToRecords withValueInterceptor(ValueInterceptor valueInterceptor) {
        this.valueInterceptor = valueInterceptor;
        return this;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public Iterator<GenericRecord> iterator() {
        return new Iterator<>() {
            final Iterator<JsonNode> it = rootNode.iterator();
            int index = 0;

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public GenericRecord next() {
                // See CsvParser.iterator.next()
                DataElement dataElement = new DataElement(columnNames[index++]).withValueInterceptor(valueInterceptor);
                dataElement.setValue(it.next().textValue());
                if (JsonToRecords.this.callBack != null) {
                    JsonToRecords.this.callBack.onElement(dataElement);
                }
                return SchemaAwareElement.toRecord(dataElement, JsonToRecords.this.schemaBuddy);
            }
        };
    }

    public interface Callback {
        void onElement(DataElement dataElement);
    }

}
