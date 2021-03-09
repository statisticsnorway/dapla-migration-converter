package no.ssb.rawdata.converter.app.migration;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataMetadataClient;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class MigrationRawdataConverterTest {

    @Test
    void shouldConvertRawdataMessages() {
        Map<String, String> filesystemConfig = Map.of(
                "local-temp-folder", "target/rawdata/temp",
                "filesystem.storage-folder", "src/test/resources/rawdata-messages",
                "listing.min-interval-seconds", "0",
                "avro-file.max.seconds", "60",
                "avro-file.max.bytes", "67108864",
                "avro-file.sync.interval", "200"
        );
        try (RawdataClient rawdataClient = ProviderConfigurator.configure(filesystemConfig, "filesystem", RawdataClientInitializer.class)) {
            RawdataMetadataClient metadataClient = rawdataClient.metadata("sometopic");

            // RawdataConsumer consumer = rawdataClient.consumer("sometopic");

            // RawdataMessage message = consumer.receive(0, TimeUnit.SECONDS);

            MigrationRawdataConverterConfig config = new MigrationRawdataConverterConfig();
            // TODO: Set app config

            MigrationRawdataConverter converter = new MigrationRawdataConverter(config, new ValueInterceptorChain());

            converter.init(metadataClient);
            //ConversionResult res = converter.convert(messages.index().get("123456")); // TODO: replace with message position
        }
    }
}
