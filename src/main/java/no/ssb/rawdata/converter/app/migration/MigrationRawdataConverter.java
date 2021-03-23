package no.ssb.rawdata.converter.app.migration;

import lombok.extern.slf4j.Slf4j;
import no.ssb.dapla.ingest.rawdata.metadata.CsvSchema;
import no.ssb.dapla.ingest.rawdata.metadata.RawdataStructure;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMetadataClient;
import no.ssb.rawdata.converter.app.migration.csv.CsvConverter;
import no.ssb.rawdata.converter.app.migration.json.JsonKostraConverter;
import no.ssb.rawdata.converter.app.migration.json.JsonOracleConverter;
import no.ssb.rawdata.converter.core.convert.ConversionResult;
import no.ssb.rawdata.converter.core.convert.ConversionResult.ConversionResultBuilder;
import no.ssb.rawdata.converter.core.convert.RawdataConverterV2;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.rawdata.converter.core.schema.AggregateSchemaBuilder;
import no.ssb.rawdata.converter.metrics.MetricName;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class MigrationRawdataConverter implements RawdataConverterV2 {

    private static final String FIELDNAME_MANIFEST = "manifest";
    private static final String FIELDNAME_COLLECTOR = "collector";

    private final ValueInterceptorChain valueInterceptorChain;

    private Schema manifestSchema;
    private Schema targetAvroSchema;
    private Schema collectorManifestSchema;

    private final Map<String, MigrationConverter> delegateByDocumentId = new LinkedHashMap<>();

    public MigrationRawdataConverter(ValueInterceptorChain valueInterceptorChain) {
        this.valueInterceptorChain = valueInterceptorChain;
    }

    @Override
    public void initialize(RawdataMetadataClient metadataClient) {
        log.info("Determine target avro schema from metadata of topic: {}", metadataClient.topic());

        Set<String> keys = metadataClient.keys();
        if (!keys.contains("metadata.json")) {
            throw new IllegalStateException("Missing 'metadata.json' from metadata");
        }
        if (!keys.contains("structure.json")) {
            throw new IllegalStateException("Missing 'structure.json' from metadata");
        }


        // RawdataMetadata rawdataMetadata = RawdataMetadata.of(metadataClient.get("metadata.json")).build();
        RawdataStructure rawdataStructure = RawdataStructure.of(metadataClient.get("structure.json")).build();

        collectorManifestSchema = SchemaBuilder.record(FIELDNAME_COLLECTOR)
                .fields()
                .requiredString("ulid")
                .requiredString("position")
                .requiredString("timestamp")
                .endRecord();

        manifestSchema = new AggregateSchemaBuilder("dapla.rawdata.manifest")
                .schema(FIELDNAME_COLLECTOR, collectorManifestSchema)
                .build();

        String targetNamespace = "dapla.rawdata.migration." + metadataClient.topic();
        AggregateSchemaBuilder targetAvroAggregateSchemaBuilder = new AggregateSchemaBuilder(targetNamespace)
                .schema(FIELDNAME_MANIFEST, manifestSchema);

        Map<String, RawdataStructure.Document> documents = rawdataStructure.documents();

        for (Map.Entry<String, RawdataStructure.Document> entry : documents.entrySet()) {
            String documentId = entry.getKey();
            RawdataStructure.Document document = entry.getValue();
            RawdataStructure.Document.Structure structure = document.structure();
            URI uri = structure.uri();
            byte[] schemaBytes = switch (uri.getScheme()) {
                case "inline" -> structure.schemaAsBytes();
                case "metadata" -> metadataClient.get(uri.getSchemeSpecificPart());
                default -> throw new RuntimeException("structure.uri scheme not supported while locating schema: " + uri.getScheme());
            };
            String converterType = switch (uri.getScheme()) {
                case "inline" -> uri.getSchemeSpecificPart();
                case "metadata" -> uri.getSchemeSpecificPart().substring(uri.getSchemeSpecificPart().lastIndexOf(".") + 1);
                default -> throw new RuntimeException("structure.uri scheme not supported while determining converterType: " + uri.getScheme());
            };
            MigrationConverter converter = switch (converterType) {
                case "csv" -> new CsvConverter(valueInterceptorChain, documentId, new CsvSchema(schemaBytes));
                case "kostra" -> new JsonKostraConverter(valueInterceptorChain, documentId, schemaBytes);
                case "oracle" -> new JsonOracleConverter(valueInterceptorChain, documentId, schemaBytes);
                default -> throw new IllegalArgumentException("converterType not supported: " + converterType);
            };
            delegateByDocumentId.put(documentId, converter);

            Schema documentSchema = converter.init(metadataClient);

            targetAvroAggregateSchemaBuilder.schema(documentId, documentSchema);
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

        resultBuilder.withRecord(FIELDNAME_MANIFEST, new GenericRecordBuilder(manifestSchema)
                .set(FIELDNAME_COLLECTOR, new GenericRecordBuilder(collectorManifestSchema)
                        .set("ulid", rawdataMessage.ulid().toString())
                        .set("position", rawdataMessage.position())
                        .set("timestamp", rawdataMessage.timestamp())
                        .build())
                .build());

        for (Map.Entry<String, byte[]> entry : rawdataMessage.data().entrySet()) {
            String documentId = entry.getKey();
            MigrationConverter migrationConverter = delegateByDocumentId.get(documentId);
            try {
                GenericRecord documentRecord = migrationConverter.convert(rawdataMessage);
                resultBuilder.withRecord(documentId, documentRecord);
            } catch (Exception e) {
                resultBuilder.addFailure(e);
            }
        }
        resultBuilder.appendCounter(MetricName.RAWDATA_RECORDS_TOTAL, 1);

        return resultBuilder.build();
    }
}