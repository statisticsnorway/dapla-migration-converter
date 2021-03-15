package no.ssb.rawdata.converter.app.migration;

import io.micronaut.core.convert.format.MapFormat;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class MigrationRawdataConverterConfig {

    /**
     * Optional csv parser settings overrides.
     * E.g. allowing to explicitly specify the delimiter character
     */
    @MapFormat(transformation = MapFormat.MapTransformation.FLAT)
    private Map<String, Object> csvSettings = new HashMap<>();

}