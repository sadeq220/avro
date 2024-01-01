import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * decode binary datum
 * in the context of Apache Avro, a datum is a single unit of data that conforms to a specified schema.
 * binary datum only contains values and doesn't contain field names
 */
public class AvroBinaryDeserializer {
    private static final Logger logger = LoggerFactory.getLogger(AvroBinaryDeserializer.class);
    private static final String hexDatum = "0a7361646571000e";//binary datum: sadeq7
    public static void main(String[] args) {
        Schema writerSchema = dynamicSchema();
        Schema readerSchema = legacySchema();
        try {
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(writerSchema);
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(Hex.decodeHex(hexDatum),null);
            GenericRecord read = datumReader.read(null, binaryDecoder);
            logger.info("deserialized binary datum: {}",read);
        } catch (DecoderException e) {
            logger.error("hex decoder error!",e);
        } catch (IOException e) {
            logger.error("avro deserialization exception!",e);
        }
    }
    private static Schema getFromSchemaFile(){
        File file = new File("src/main/avro/User.avsc");
        Schema.Parser parser = new Schema.Parser();
        try {
            return parser.parse(file);
        } catch (IOException e) {
            logger.error("avro file reading exception!",e);
            return null;
        }
    }
    private static Schema dynamicSchema(){
        return SchemaBuilder.record("User").namespace("cloud.shareApp").fields().requiredString("name").nullableInt("favourite_number",0).endRecord();
    }
    private static Schema legacySchema(){
        return SchemaBuilder.record("User").namespace("cloud.shareApp").fields().nullableInt("favourite_number",0).endRecord();
    }
}
