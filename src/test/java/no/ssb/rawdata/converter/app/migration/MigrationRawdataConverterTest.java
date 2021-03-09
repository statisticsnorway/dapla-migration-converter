package no.ssb.rawdata.converter.app.migration;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.rawdata.converter.core.convert.ConversionResult;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;

public class MigrationRawdataConverterTest {

    @Test
    void shouldConvertRawdataMessages() throws InterruptedException {
        Map<String, String> filesystemConfig = Map.of(
                "local-temp-folder", "target/rawdata/temp",
                "filesystem.storage-folder", "src/test/resources/rawdata-messages",
                "listing.min-interval-seconds", "0",
                "avro-file.max.seconds", "60",
                "avro-file.max.bytes", "67108864",
                "avro-file.sync.interval", "200"
        );
        try (RawdataClient rawdataClient = ProviderConfigurator.configure(filesystemConfig, "filesystem", RawdataClientInitializer.class)) {
            if (false) {
                createTestData(rawdataClient);
            }

            MigrationRawdataConverterConfig config = new MigrationRawdataConverterConfig();

            MigrationRawdataConverter converter = new MigrationRawdataConverter(config, new ValueInterceptorChain());

            converter.init(rawdataClient.metadata("sometopic"));
            try (RawdataConsumer consumer = rawdataClient.consumer("sometopic")) {
                RawdataMessage message;
                while ((message = consumer.receive(0, TimeUnit.SECONDS)) != null) {
                    ConversionResult res = converter.convert(message);
                    if (res.getFailures().size() > 0) {
                        res.getFailures().get(0).printStackTrace();
                        fail("Error while converting message");
                    }
                    System.out.printf("Message: %s%n", res.getGenericRecord().toString());
                }
            }

        }
    }

    private void createTestData(RawdataClient rawdataClient) {
        try (RawdataProducer producer = rawdataClient.producer("sometopic")) {
            producer.publish(RawdataMessage.builder()
                    .put("data", "Frank Jensen;1973-04-01;4329;595000;3;0.2023;0.84;2021-02-25T12:43:24Z;22:45;true".getBytes(StandardCharsets.UTF_8))
                    .position("1").build());
            producer.publish(RawdataMessage.builder()
                    .put("data", "Jon Fransen;1976-11-23;4329;272000;15;0.9144;3.62;2020-12-18T18:50:00Z;23:07;false".getBytes(StandardCharsets.UTF_8))
                    .position("2").build());
            producer.publish(RawdataMessage.builder()
                    .put("data", "Frida Hansen;1981-08-16;4329;685000;0;0.4282;1.05;2021-03-08T14:14:14Z;22:25;false".getBytes(StandardCharsets.UTF_8))
                    .position("3").build());
        }
    }

    @Test
    public void test() {
        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date = LocalDate.parse(" 1973-04-01", pattern);
        date.toString();
    }
}
