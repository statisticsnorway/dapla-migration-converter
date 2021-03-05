package no.ssb.rawdata.converter.app.migration;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;

@ConfigurationProperties("rawdata.converter.migration")
@Data
public class MigrationRawdataConverterConfig {

    /**
     * <p>Some config param</p>
     *
     * <p>Defaults to "default value"</p>
     *
     * TODO: Remove this
     */
    private String someParam = "default value";

}