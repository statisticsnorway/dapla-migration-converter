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
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static no.ssb.rawdata.converter.util.RawdataMessageAdapter.posAndIdOf;

@Slf4j
public class MigrationRawdataConverter implements RawdataConverter {

    private static final String RAWDATA_ITEMNAME_ENTRY = "entry";
    private static final String FIELDNAME_MANIFEST = "manifest";
    private static final String FIELDNAME_COLLECTOR = "collector";
    private static final String FIELDNAME_DATA = "data";

    private final MigrationRawdataConverterConfig converterConfig;
    private final ValueInterceptorChain valueInterceptorChain;

    private DcManifestSchemaAdapter dcManifestSchemaAdapter;
    private Schema manifestSchema;
    private Schema targetAvroSchema;
    private Schema dataSchema;

    private RawdataMetadata metadata;
    private RawdataStructure structure;

    CSVFormat csvFormat;
    String[] csvHeader;
    String[] avroHeader;
    Function<String, Object>[] csvToAvroMapping;

    public MigrationRawdataConverter(MigrationRawdataConverterConfig converterConfig, ValueInterceptorChain valueInterceptorChain) {
        this.converterConfig = converterConfig;
        this.valueInterceptorChain = valueInterceptorChain;
    }

    @Override
    public void init(RawdataMetadataClient metadataClient) {
        log.info("Determine target avro schema from metadata of topic: {}", metadataClient.topic());

        RawdataMetadata metadata;
        RawdataStructure structure;
        Set<String> keys = metadataClient.keys();
        if (!keys.contains("metadata.json")) {
            throw new IllegalStateException("Missing 'metadata.json' from metadata");
        }
        if (!keys.contains("structure.json")) {
            throw new IllegalStateException("Missing 'structure.json' from metadata");
        }

        metadata = RawdataMetadata.of(metadataClient.get("metadata.json")).build();
        structure = RawdataStructure.of(metadataClient.get("structure.json")).build();

        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("item").fields();
        List<String> columnNames = structure.columnNames();
        csvHeader = columnNames.toArray(new String[0]);
        avroHeader = columnNames.stream()
                .map(AvroUtils::formatToken)
                .toArray(String[]::new);
        for (int i = 0; i < columnNames.size(); i++) {
            RawdataStructure.Column column = structure.column(columnNames.get(i));
            if ("String".equals(column.type)) {
                fields.optionalString(column.name);
                csvToAvroMapping[i] = str -> str;
            } else if ("Boolean".equals(column.type)) {
                fields.optionalBoolean(column.name);
                csvToAvroMapping[i] = Boolean::parseBoolean;
            } else if ("Long".equals(column.type)) {
                fields.optionalLong(column.name);
                csvToAvroMapping[i] = Long::parseLong;
            } else if ("Integer".equals(column.type)) {
                fields.optionalLong(column.name);
                csvToAvroMapping[i] = Integer::parseInt;
            } else if ("DateTime".equals(column.type)) {
                throw new RuntimeException("TODO: DateTime csv to avro mapping");
            } else {
                throw new RuntimeException("Type not supported: " + column.type);
            }
        }
        dataSchema = fields.endRecord();

        String manifestJson = new String(metadataClient.get("manifest.json"), StandardCharsets.UTF_8);
        dcManifestSchemaAdapter = DcManifestSchemaAdapter.of(manifestJson);

        manifestSchema = new AggregateSchemaBuilder("dapla.rawdata.manifest")
                .schema(FIELDNAME_COLLECTOR, dcManifestSchemaAdapter.getDcManifestSchema())
                .build();

        String targetNamespace = "dapla.rawdata.migration." + metadataClient.topic();
        targetAvroSchema = new AggregateSchemaBuilder(targetNamespace)
                .schema(FIELDNAME_MANIFEST, manifestSchema)
                .schema(FIELDNAME_DATA, dataSchema)
                .build();

        char delimiter = structure.structureArgs().get("delimiter").charAt(0);
        csvFormat = CSVFormat.RFC4180
                .withFirstRecordAsHeader()
                .withDelimiter(delimiter)
                .withHeader(csvHeader);
    }

    public DcManifestSchemaAdapter dcManifestSchemaAdapter() {
        if (dcManifestSchemaAdapter == null) {
            throw new IllegalStateException("dcManifestSchemaAdapter is null. Make sure RawdataConverter#init() was invoked in advance.");
        }

        return dcManifestSchemaAdapter;
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

        byte[] entry = rawdataMessage.get(RAWDATA_ITEMNAME_ENTRY);
        try (CSVParser parser = csvFormat.parse(new InputStreamReader(new ByteArrayInputStream(entry), StandardCharsets.UTF_8))) {
            for (CSVRecord csvRecord : parser) {
                GenericRecordBuilder avroRecordBuilder = new GenericRecordBuilder(dataSchema);
                for (int i = 0; i < csvRecord.size(); i++) {
                    String value = csvRecord.get(i);
                    Object avroValue = csvToAvroMapping[i].apply(value);
                    avroRecordBuilder.set(avroHeader[i], avroValue);
                }
                GenericData.Record avroRecord = avroRecordBuilder.build();
                resultBuilder.appendCounter(MetricName.RAWDATA_RECORDS_TOTAL, 1);
                resultBuilder.withRecord(FIELDNAME_DATA, avroRecord);
            }
        } catch (Exception e) {
            resultBuilder.addFailure(e);
            throw new CsvRawdataConverterException("Error converting CSV data at " + posAndIdOf(rawdataMessage), e);
        }

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