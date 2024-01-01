import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * for avro deserialization we need both writer schema and reader schema
 * The writer’s schema is needed to know the order in which fields were written, while the reader’s schema is needed to know what fields are expected and how to fill in default values for fields added since the file was written.
 * If there are differences between the two schemas, they are resolved according to the Schema Resolution specification.
 */
public class AvroDeserializerWithGenericRecord {
    private static Logger logger= LoggerFactory.getLogger(AvroDeserializerWithGenericRecord.class);
    private static final String avroJson = "{\"name\":\"sadeq\",\"favourite_number\":{\"int\":7}}";
    public static void main(String[] args) {
        Schema schema = dynamicSchema();
        GenericDatumReader<GenericRecord> genericDatumReader = new GenericDatumReader<>(schema,legacySchema());//writer and reader schema
        try {
            JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, avroJson);//writer schema
            GenericRecord read = genericDatumReader.read(null, jsonDecoder);
            logger.info("deserialized record: {}",read);
        } catch (IOException e) {
            e.printStackTrace();
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
