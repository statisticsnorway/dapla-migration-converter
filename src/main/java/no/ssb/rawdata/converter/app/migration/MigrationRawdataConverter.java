package no.ssb.rawdata.converter.app.migration;

import lombok.extern.slf4j.Slf4j;
import no.ssb.dapla.ingest.rawdata.metadata.RawdataMetadata;
import no.ssb.dapla.ingest.rawdata.metadata.RawdataStructure;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMetadataClient;
import no.ssb.rawdata.converter.core.convert.ConversionResult;
import no.ssb.rawdata.converter.core.convert.ConversionResult.ConversionResultBuilder;
import no.ssb.rawdata.converter.core.convert.RawdataConverter;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import no.ssb.rawdata.converter.core.schema.AggregateSchemaBuilder;
import no.ssb.rawdata.converter.core.schema.DcManifestSchemaAdapter;
import no.ssb.rawdata.converter.metrics.MetricName;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static no.ssb.rawdata.converter.util.RawdataMessageAdapter.posAndIdOf;

@Slf4j
public class MigrationRawdataConverter implements RawdataConverter {

    private static final String FIELDNAME_MANIFEST = "manifest";
    private static final String FIELDNAME_COLLECTOR = "collector";

    private final MigrationRawdataConverterConfig converterConfig;
    private final ValueInterceptorChain valueInterceptorChain;

    private DcManifestSchemaAdapter dcManifestSchemaAdapter;
    private Schema manifestSchema;
    private Schema targetAvroSchema;
    private Schema dataSchema;

    private RawdataMetadata rawdataMetadata;
    private RawdataStructure rawdataStructure;

    private Map<String, DocumentMappings> documentMappingsById = new LinkedHashMap<>();

    static class DocumentMappings {
        final CSVFormat csvFormat;
        final String[] csvHeader;
        final String[] avroHeader;
        final Function<String, Object>[] csvToAvroMapping;

        DocumentMappings(CSVFormat csvFormat, String[] csvHeader, String[] avroHeader, Function<String, Object>[] csvToAvroMapping) {
            this.csvFormat = csvFormat;
            this.csvHeader = csvHeader;
            this.avroHeader = avroHeader;
            this.csvToAvroMapping = csvToAvroMapping;
        }
    }

    public MigrationRawdataConverter(MigrationRawdataConverterConfig converterConfig, ValueInterceptorChain valueInterceptorChain) {
        this.converterConfig = converterConfig;
        this.valueInterceptorChain = valueInterceptorChain;
    }

    @Override
    public void init(RawdataMetadataClient metadataClient) {
        log.info("Determine target avro schema from metadata of topic: {}", metadataClient.topic());

        Set<String> keys = metadataClient.keys();
        if (!keys.contains("metadata.json")) {
            throw new IllegalStateException("Missing 'metadata.json' from metadata");
        }
        if (!keys.contains("structure.json")) {
            throw new IllegalStateException("Missing 'structure.json' from metadata");
        }

        rawdataMetadata = RawdataMetadata.of(metadataClient.get("metadata.json")).build();
        rawdataStructure = RawdataStructure.of(metadataClient.get("structure.json")).build();

        manifestSchema = new AggregateSchemaBuilder("dapla.rawdata.manifest")
                .schema(FIELDNAME_COLLECTOR, SchemaBuilder.record(FIELDNAME_COLLECTOR)
                        .fields()
                        .requiredString("ulid")
                        .requiredString("position")
                        .requiredString("timestamp")
                        .endRecord())
                .build();

        String targetNamespace = "dapla.rawdata.migration." + metadataClient.topic();
        AggregateSchemaBuilder targetAvroAggregateSchemaBuilder = new AggregateSchemaBuilder(targetNamespace)
                .schema(FIELDNAME_MANIFEST, manifestSchema);


        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("item").fields();
        Map<String, RawdataStructure.Document> documents = rawdataStructure.documents();

        for (Map.Entry<String, RawdataStructure.Document> entry : documents.entrySet()) {
            String documentName = entry.getKey();
            RawdataStructure.Document document = entry.getValue();
            RawdataStructure.Document.Structure structure = document.structure();
            List<RawdataStructure.Document.Structure.Column> columns = structure.columns();
            String[] csvHeader = columns.stream()
                    .map(RawdataStructure.Document.Structure.Column::name)
                    .toArray(String[]::new);
            String[] avroHeader = columns.stream()
                    .map(RawdataStructure.Document.Structure.Column::name)
                    .map(AvroUtils::formatToken)
                    .toArray(String[]::new);
            Function<String, Object>[] csvToAvroMapping = new Function[csvHeader.length];
            for (int i = 0; i < columns.size(); i++) {
                RawdataStructure.Document.Structure.Column column = columns.get(i);
                if ("String".equals(column.type())) {
                    fields.optionalString(column.name());
                    csvToAvroMapping[i] = str -> str;
                } else if ("Boolean".equals(column.type())) {
                    fields.optionalBoolean(column.name());
                    csvToAvroMapping[i] = Boolean::parseBoolean;
                } else if ("Long".equals(column.type())) {
                    fields.optionalLong(column.name());
                    csvToAvroMapping[i] = Long::parseLong;
                } else if ("Integer".equals(column.type())) {
                    fields.optionalLong(column.name());
                    csvToAvroMapping[i] = Integer::parseInt;
                } else if ("DateTime".equals(column.type())) {
                    throw new RuntimeException("TODO: DateTime csv to avro mapping");
                } else {
                    throw new RuntimeException("Type not supported: " + column.type());
                }
            }
            dataSchema = fields.endRecord();

            char delimiter = structure.arg("delimiter").charAt(0);
            CSVFormat csvFormat = CSVFormat.RFC4180
                    .withFirstRecordAsHeader()
                    .withDelimiter(delimiter)
                    .withHeader(csvHeader);

            targetAvroAggregateSchemaBuilder
                    .schema(documentName, dataSchema)
                    .build();

            documentMappingsById.put(documentName, new DocumentMappings(csvFormat, csvHeader, avroHeader, csvToAvroMapping));
        }

        targetAvroSchema = targetAvroAggregateSchemaBuilder.build();
    }

    @Override
    public Schema targetAvroSchema() {
        if (targetAvroSchema == null) {
            throw new IllegalStateException("targetAvroSchema is null. Make sure RawdataConverter#init() was invoked in advance.");
        }

        return targetAvroSchema;
    }

    @Override
    public boolean isConvertible(RawdataMessage rawdataMessage) {
        return true;
    }

    @Override
    public ConversionResult convert(RawdataMessage rawdataMessage) {
        ConversionResultBuilder resultBuilder = ConversionResult.builder(targetAvroSchema, rawdataMessage);

        addManifest(rawdataMessage, resultBuilder);

        int rawdataRecordsDelta = 0;
        for (Map.Entry<String, byte[]> entry : rawdataMessage.data().entrySet()) {
            String documentId = entry.getKey();
            byte[] documentBytes = entry.getValue();
            DocumentMappings documentMappings = documentMappingsById.get(documentId);
            try (CSVParser parser = documentMappings.csvFormat.parse(new InputStreamReader(new ByteArrayInputStream(documentBytes), StandardCharsets.UTF_8))) {
                Iterator<CSVRecord> iterator = parser.iterator();
                if (iterator.hasNext()) {
                    CSVRecord csvRecord = iterator.next();
                    GenericRecordBuilder avroRecordBuilder = new GenericRecordBuilder(dataSchema);
                    for (int i = 0; i < csvRecord.size(); i++) {
                        String columnValue = csvRecord.get(i);
                        Object avroValue = documentMappings.csvToAvroMapping[i].apply(columnValue);
                        avroRecordBuilder.set(documentMappings.avroHeader[i], avroValue);
                    }
                    GenericData.Record avroRecord = avroRecordBuilder.build();
                    resultBuilder.withRecord(documentId, avroRecord);
                    rawdataRecordsDelta++;
                    if (iterator.hasNext()) {
                        throw new CsvRawdataConverterException("More than one line in CSV file!");
                    }
                }
            } catch (Exception e) {
                resultBuilder.addFailure(e);
                throw new CsvRawdataConverterException("Error converting CSV data at " + posAndIdOf(rawdataMessage), e);
            }
        }
        resultBuilder.appendCounter(MetricName.RAWDATA_RECORDS_TOTAL, rawdataRecordsDelta);

        return resultBuilder.build();
    }

    void addManifest(RawdataMessage rawdataMessage, ConversionResultBuilder resultBuilder) {
        GenericRecord manifest = new GenericRecordBuilder(manifestSchema)
                .set(FIELDNAME_COLLECTOR, dcManifestSchemaAdapter.newRecord(rawdataMessage, valueInterceptorChain))
                .build();

        resultBuilder.withRecord(FIELDNAME_MANIFEST, manifest);
    }

    public static class CsvRawdataConverterException extends RawdataConverterException {
        public CsvRawdataConverterException(String msg) {
            super(msg);
        }

        public CsvRawdataConverterException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}