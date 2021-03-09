package no.ssb.rawdata.converter.app.migration;

import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMetadataClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public interface MigrationConverter {

    Schema init(RawdataMetadataClient metadataClient);

    GenericData.Record convert(RawdataMessage rawdataMessage);

    boolean isConvertible(RawdataMessage rawdataMessage);
}
