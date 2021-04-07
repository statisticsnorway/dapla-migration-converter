package no.ssb.rawdata.converter.app.migration.csv;

import no.ssb.avro.convert.core.FieldDescriptor;

import java.util.function.Function;

class CsvColumnMapper {
    final String name;
    final Function<String, Object> stringToAvroConverter;
    final FieldDescriptor fieldDescriptor;

    CsvColumnMapper(String name, Function<String, Object> stringToAvroConverter, FieldDescriptor fieldDescriptor) {
        this.name = name;
        this.stringToAvroConverter = stringToAvroConverter;
        this.fieldDescriptor = fieldDescriptor;
    }
}
