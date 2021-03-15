package no.ssb.rawdata.converter.app.migration.csv;

import no.ssb.avro.convert.csv.CsvParserSettings;
import no.ssb.avro.convert.csv.CsvToRecords;
import no.ssb.dapla.ingest.rawdata.metadata.CsvSchema;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMetadataClient;
import no.ssb.rawdata.converter.app.migration.AvroUtils;
import no.ssb.rawdata.converter.app.migration.MigrationConverter;
import no.ssb.rawdata.converter.app.migration.MigrationRawdataConverterConfig;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static no.ssb.rawdata.converter.util.RawdataMessageAdapter.posAndIdOf;

public class CsvConverter implements MigrationConverter {

    final MigrationRawdataConverterConfig converterConfig;
    final ValueInterceptorChain valueInterceptorChain;
    final String documentId;
    final CsvSchema csvSchema;
    Schema avroSchema;
    CsvParserSettings csvParserSettings;

    public CsvConverter(MigrationRawdataConverterConfig converterConfig, ValueInterceptorChain valueInterceptorChain, String documentId, CsvSchema csvSchema) {
        this.converterConfig = converterConfig;
        this.valueInterceptorChain = valueInterceptorChain;
        this.documentId = documentId;
        this.csvSchema = csvSchema;
    }

    public Schema init(RawdataMetadataClient metadataClient) {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(documentId).fields();

        List<CsvSchema.Column> columns = csvSchema.columns();
        String[] avroHeader = columns.stream()
                .map(CsvSchema.Column::name)
                .map(AvroUtils::formatToken)
                .toArray(String[]::new);
        for (int i = 0; i < columns.size(); i++) {
            CsvSchema.Column column = columns.get(i);
            String avroColumnName = avroHeader[i];
            String columnType = column.type();
            if ("String".equals(columnType)) {
                fields.optionalString(avroColumnName);
            } else if ("Boolean".equals(columnType)) {
                fields.optionalBoolean(avroColumnName);
            } else if ("Long".equals(columnType)) {
                fields.optionalLong(avroColumnName);
            } else if ("Int".equals(columnType)) {
                fields.optionalInt(avroColumnName);
            } else if ("Double".equals(columnType)) {
                fields.optionalDouble(avroColumnName);
            } else if ("Float".equals(columnType)) {
                fields.optionalFloat(avroColumnName);
            } else {
                throw new RuntimeException("Type not supported: " + columnType);
            }
        }
        avroSchema = fields.endRecord();

        csvParserSettings = new CsvParserSettings()
                .delimiters(String.valueOf(csvSchema.delimiter()))
                .columnHeadersPresent(false)
                .headers(csvSchema.columns().stream()
                        .map(CsvSchema.Column::name)
                        .map(AvroUtils::formatToken)
                        .collect(Collectors.toList()));

        return avroSchema;
    }

    @Override
    public GenericRecord convert(RawdataMessage rawdataMessage) {
        byte[] data = rawdataMessage.get(documentId);

        try (CsvToRecords records = new CsvToRecords(new ByteArrayInputStream(data), avroSchema, csvParserSettings)
                .withValueInterceptor(valueInterceptorChain::intercept)) {

            List<GenericRecord> dataItems = new ArrayList<>();
            records.forEach(dataItems::add);

            return dataItems.get(0);
        } catch (IOException e) {
            throw new CsvConverterException("Error converting CSV data at " + posAndIdOf(rawdataMessage), e);
        }
    }

    @Override
    public boolean isConvertible(RawdataMessage rawdataMessage) {
        return true;
    }

    public static class CsvConverterException extends RawdataConverterException {
        public CsvConverterException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
