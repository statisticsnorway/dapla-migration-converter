package no.ssb.rawdata.converter.app.migration;

import io.micronaut.runtime.Micronaut;
import lombok.extern.slf4j.Slf4j;
import no.ssb.rawdata.converter.app.RawdataConverterApplication;
import no.ssb.rawdata.converter.util.MavenArtifactUtil;

@Slf4j
public class Application extends RawdataConverterApplication {

    public static void main(String[] args) {
        log.info("rawdata-converter-app-migration version: {}", MavenArtifactUtil.findArtifactVersion("no.ssb.rawdata.converter.app", "rawdata-converter-app-migration").orElse("unknown"));
        Micronaut.run(Application.class, args);
    }

}