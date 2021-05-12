package no.ssb.rawdata.converter.app.migration.csv;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.ResultIterator;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import lombok.extern.slf4j.Slf4j;
import no.ssb.avro.convert.core.FieldDescriptor;
import no.ssb.avro.convert.csv.CsvParserSettings;
import no.ssb.dapla.ingest.rawdata.metadata.CsvSchema;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMetadataClient;
import no.ssb.rawdata.converter.app.migration.MigrationConverter;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

@Slf4j
public class CsvConverter implements MigrationConverter {

    final ValueInterceptorChain valueInterceptorChain;
    final String documentId;
    final CsvSchema csvSchema;
    Schema avroSchema;
    CsvParserSettings csvParserSettings;
    CsvColumnMapper[] columnMappers;

    public CsvConverter(ValueInterceptorChain valueInterceptorChain, String documentId, CsvSchema csvSchema) {
        this.valueInterceptorChain = valueInterceptorChain;
        this.documentId = documentId;
        this.csvSchema = csvSchema;
    }

    public Schema init(RawdataMetadataClient metadataClient) {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(documentId).fields();

        List<CsvSchema.Column> columns = csvSchema.columns();
        String[] avroHeader = columns.stream()
                .map(CsvSchema.Column::name)
                .toArray(String[]::new);
        columnMappers = new CsvColumnMapper[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            CsvSchema.Column column = columns.get(i);
            String avroColumnName = avroHeader[i];
            String columnType = column.type();
            if ("String".equals(columnType)) {
                fields.optionalString(avroColumnName);
                columnMappers[i] = new CsvColumnMapper(avroColumnName, str -> str, new FieldDescriptor(avroColumnName));
            } else if ("Boolean".equals(columnType)) {
                fields.optionalBoolean(avroColumnName);
                columnMappers[i] = new CsvColumnMapper(avroColumnName, Boolean::parseBoolean, new FieldDescriptor(avroColumnName));
            } else if ("Long".equals(columnType)) {
                fields.optionalLong(avroColumnName);
                columnMappers[i] = new CsvColumnMapper(avroColumnName, Long::parseLong, new FieldDescriptor(avroColumnName));
            } else if ("Int".equals(columnType)) {
                fields.optionalInt(avroColumnName);
                columnMappers[i] = new CsvColumnMapper(avroColumnName, Integer::parseInt, new FieldDescriptor(avroColumnName));
            } else if ("Double".equals(columnType)) {
                fields.optionalDouble(avroColumnName);
                columnMappers[i] = new CsvColumnMapper(avroColumnName, Double::parseDouble, new FieldDescriptor(avroColumnName));
            } else if ("Float".equals(columnType)) {
                fields.optionalFloat(avroColumnName);
                columnMappers[i] = new CsvColumnMapper(avroColumnName, Float::parseFloat, new FieldDescriptor(avroColumnName));
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
                        .collect(Collectors.toList()));

        return avroSchema;
    }

    @Override
    public GenericRecord convert(RawdataMessage rawdataMessage) {
        byte[] data = rawdataMessage.get(documentId);
        System.err.println("-----> data: " + new String(data));
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

        // TODO Creating a new CSVParser is an expensive operation
        com.univocity.parsers.csv.CsvParserSettings settings = csvParserSettings.getInternal();
        CsvParser internalCsvParser = new CsvParser(settings);

        ResultIterator<Record, ParsingContext> iterator = internalCsvParser.iterateRecords(inputStream).iterator();
        if (iterator.hasNext()) {
            Record record = iterator.next();
            for (int i = 0; i < columnMappers.length; i++) {
                CsvColumnMapper columnMapper = columnMappers[i];
                ofNullable(record.getString(i))
                        .map(str -> valueInterceptorChain.intercept(columnMapper.fieldDescriptor, str))
                        .map(columnMapper.stringToAvroConverter)
                        .ifPresent(value -> recordBuilder.set(columnMapper.name, value));
            }
        }
        if (iterator.hasNext()) {
            log.warn("Received CSV data item with more than one record, skipping all but first record: pos={}, ulid={}", rawdataMessage.position(), rawdataMessage.ulid().toString());
        }
        return recordBuilder.build();
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
