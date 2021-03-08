package no.ssb.rawdata.converter.app.migration;

import no.ssb.rawdata.converter.core.convert.ConversionResult;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.rawdata.converter.test.message.RawdataMessageFixtures;
import no.ssb.rawdata.converter.test.message.RawdataMessages;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class MigrationRawdataConverterTest {

    static RawdataMessageFixtures fixtures;

    @BeforeAll
    static void loadFixtures() {
        fixtures = RawdataMessageFixtures.init("sometopic");
    }

    @Disabled
    @Test
    void shouldConvertRawdataMessages() {
        RawdataMessages messages = fixtures.rawdataMessages("sometopic"); // TODO: replace with topicname
        MigrationRawdataConverterConfig config = new MigrationRawdataConverterConfig();
        // TODO: Set app config

        MigrationRawdataConverter converter = new MigrationRawdataConverter(config, new ValueInterceptorChain());

        converter.init(null);
        ConversionResult res = converter.convert(messages.index().get("123456")); // TODO: replace with message position
    }

}
