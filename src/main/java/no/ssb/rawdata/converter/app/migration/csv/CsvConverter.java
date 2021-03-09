package no.ssb.rawdata.converter.app.migration.csv;

import no.ssb.dapla.ingest.rawdata.metadata.CsvSchema;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMetadataClient;
import no.ssb.rawdata.converter.app.migration.AvroUtils;
import no.ssb.rawdata.converter.app.migration.MigrationConverter;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static no.ssb.rawdata.converter.util.RawdataMessageAdapter.posAndIdOf;

public class CsvConverter implements MigrationConverter {

    final String documentId;
    final CsvSchema csvSchema;
    Schema avroSchema;

    public CsvConverter(String documentId, CsvSchema csvSchema) {
        this.documentId = documentId;
        this.csvSchema = csvSchema;
    }

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

    DocumentMappings documentMappings;

    public Schema init(RawdataMetadataClient metadataClient) {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("item").fields();

        List<CsvSchema.Column> columns = csvSchema.columns();
        String[] csvHeader = columns.stream()
                .map(CsvSchema.Column::name)
                .toArray(String[]::new);
        String[] avroHeader = columns.stream()
                .map(CsvSchema.Column::name)
                .map(AvroUtils::formatToken)
                .toArray(String[]::new);
        Function<String, Object>[] csvToAvroMapping = new Function[csvHeader.length];
        for (int i = 0; i < columns.size(); i++) {
            CsvSchema.Column column = columns.get(i);
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
            } else if ("Double".equals(column.type())) {
                fields.optionalDouble(column.name());
                csvToAvroMapping[i] = Double::parseDouble;
            } else if ("Float".equals(column.type())) {
                fields.optionalFloat(column.name());
                csvToAvroMapping[i] = Float::parseFloat;
            } else if ("ZonedDateTime".equals(column.type())) {
                fields.optionalString(column.name());
                csvToAvroMapping[i] = ZonedDateTime::parse;
            } else if ("LocalTime".equals(column.type())) {
                fields.optionalString(column.name());
                csvToAvroMapping[i] = LocalTime::parse;
            } else if ("LocalDate".equals(column.type())) {
                fields.optionalString(column.name());
                csvToAvroMapping[i] = LocalDate::parse;
            } else {
                throw new RuntimeException("Type not supported: " + column.type());
            }
        }
        avroSchema = fields.endRecord();

        CSVFormat csvFormat = CSVFormat.RFC4180
                .withDelimiter(csvSchema.delimiter())
                .withHeader(csvHeader);

        documentMappings = new DocumentMappings(csvFormat, csvHeader, avroHeader, csvToAvroMapping);

        return avroSchema;
    }

    @Override
    public GenericData.Record convert(RawdataMessage rawdataMessage) {
        byte[] documentBytes = rawdataMessage.get(documentId);
        try (CSVParser parser = documentMappings.csvFormat.parse(new InputStreamReader(new ByteArrayInputStream(documentBytes), StandardCharsets.UTF_8))) {
            Iterator<CSVRecord> iterator = parser.iterator();
            if (iterator.hasNext()) {
                CSVRecord csvRecord = iterator.next();
                GenericRecordBuilder avroRecordBuilder = new GenericRecordBuilder(avroSchema);
                for (int i = 0; i < csvRecord.size(); i++) {
                    String columnValue = csvRecord.get(i);
                    Object avroValue = documentMappings.csvToAvroMapping[i].apply(columnValue);
                    avroRecordBuilder.set(documentMappings.avroHeader[i], avroValue);
                }
                GenericData.Record avroRecord = avroRecordBuilder.build();
                if (iterator.hasNext()) {
                    throw new CsvConverterException("More than one line in CSV file!");
                }
                return avroRecord;
            }
            return null;
        } catch (IOException e) {
            throw new CsvConverterException("Error converting CSV data at " + posAndIdOf(rawdataMessage), e);
        }
    }

    @Override
    public boolean isConvertible(RawdataMessage rawdataMessage) {
        return true;
    }

    public static class CsvConverterException extends RawdataConverterException {
        public CsvConverterException(String msg) {
            super(msg);
        }

        public CsvConverterException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
