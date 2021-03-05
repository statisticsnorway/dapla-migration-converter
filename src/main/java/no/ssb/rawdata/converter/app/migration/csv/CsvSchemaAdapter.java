package no.ssb.rawdata.converter.app.migration.csv;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.converter.app.migration.MigrationRawdataConverter;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import no.ssb.rawdata.converter.util.RawdataMessageAdapter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Wraps information about a Avro Schema specific for holding a set of lines from a csv file.
 *
 * <p>Two different record types are supported: 'single' and 'collection'.</p>
 *
 * <p>In some cases a csv file will always only contain one (1) line/record per rawdata message. In these
 * cases, the avro schema would look something like this:
 * <pre>
 * {
 * "type": "record",
 * "name": "<item field name>",
 * "fields": [
 * {
 * "name": "col1",
 * "type": [ "null", "string" ]
 * },
 * (...)
 * ]
 * }
 * </pre></p>
 *
 * <p>In case a rawdata message contains multiple csv lines/records (record-type=collection), the avro schema
 * will wrap the data elements in an array, like so:
 * <pre>
 * {
 * "type": "record",
 * "name": "root",
 * "fields": [
 * {
 * "name": "<collection field name>",
 * "type": {
 * "type": "array",
 * "items": {
 * "type": "record",
 * "name": "<item field name>",
 * "fields": [
 * {
 * "name": "col1",
 * "type": [ "null", "string" ]
 * },
 * , (...)
 * ]
 * }
 * }
 * }
 * ]
 * }
 * </pre></p>
 */
@Data
@RequiredArgsConstructor
public class CsvSchemaAdapter {
    private static final String FIELDNAME_ITEM = "item";
    private static final String FIELDNAME_COLLECTION = "elements";

    private List<FieldInfo> fields = new ArrayList<>();
    private final Schema itemSchema;
    private final Schema collectionSchema;
    private final RecordType recordType;

    public List<String> getHeaders() {
        return getItemSchema().getFields().stream()
                .map(f -> f.name())
                .collect(Collectors.toList());
    }

    public Schema getTargetSchema() {
        return recordType == RecordType.SINGLE ? itemSchema : collectionSchema;
    }

    public GenericRecord toTargetRecord(Collection<GenericRecord> dataItems) {
        if (dataItems.isEmpty()) {
            throw new CsvSchemaException("Encountered empty CSV data");
        }

        if (recordType == RecordType.SINGLE) {
            if (dataItems.size() > 1) {
                throw new CsvSchemaException("Encountered multi-line CSV data, but schema only supports a single record (record-type=single)");
            }

            return dataItems.stream().findFirst().get();
        } else {
            return new GenericRecordBuilder(collectionSchema).set(FIELDNAME_COLLECTION, dataItems).build();

        }
    }

    public static CsvSchemaAdapter of(RawdataMessage rawdataMessage, String csvItemName) {
        // Get hold of data collector schema metadata
        Map<String, Object> schema = new RawdataMessageAdapter(rawdataMessage)
                .findItemMetadata(csvItemName)
                .orElseThrow(() -> new MigrationRawdataConverter.CsvRawdataConverterException("No item metadata found for '" + csvItemName + "' sample item. Unable to determine target avro schema.")
                ).getSchemaMap();

        // Transform schema to field info
        List<FieldInfo> fields = ((List<Map<String, Object>>) schema.getOrDefault("fields", List.of()))
                .stream().map(f ->
                        FieldInfo.builder()
                                .name((String) f.get("mapped-name"))
                                .dataType(DataType.from((String) f.get("data-type")))
                                .optional(true)
                                .build()
                )
                .collect(Collectors.toList());

        if (fields.isEmpty()) {
            new CsvSchemaException("No fields schema metadata found in sample item. Unable to determine target avro schema.");
        }

        Schema itemSchema = itemSchemaOf(fields);
        Schema collectionSchema = collectionSchemaOf(itemSchema);
        RecordType recordType = RecordType.from((String) schema.get("record-type")).orElse(RecordType.COLLECTION);
        return new CsvSchemaAdapter(itemSchema, collectionSchema, recordType);

    }

    private static Schema itemSchemaOf(List<FieldInfo> fields) {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(FIELDNAME_ITEM).fields();
        for (FieldInfo fieldInfo : fields) {
            String name = fieldInfo.getName();
            switch (fieldInfo.getDataType()) {
                case INT:
                    fieldAssembler.optionalInt(name);
                    break;
                case LONG:
                    fieldAssembler.optionalLong(name);
                    break;
                case BOOLEAN:
                    fieldAssembler.optionalBoolean(name);
                    break;
                default:
                    fieldAssembler.optionalString(name);
            }
        }

        return fieldAssembler.endRecord();
    }

    private static Schema collectionSchemaOf(Schema itemSchema) {
        return SchemaBuilder.record("root")
                .fields()
                .name(FIELDNAME_COLLECTION).type().array().items(itemSchema).noDefault()
                .endRecord();
    }

    public enum RecordType {
        COLLECTION, SINGLE("entry");

        RecordType(String... aliases) {
            this.aliases.addAll(Arrays.asList(aliases));
        }

        private Set<String> aliases = new HashSet<>();

        public static Optional<RecordType> from(String s) {
            return Arrays.stream(RecordType.values())
                    .filter(t -> t.name().equalsIgnoreCase(s) || t.aliases.contains(s))
                    .findFirst();
        }
    }

    public static class CsvSchemaException extends RawdataConverterException {
        public CsvSchemaException(String msg) {
            super(msg);
        }

        public CsvSchemaException(String message, Throwable cause) {
            super(message, cause);
        }
    }


}