import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * serialize generic record to avro json encoding(differs from plain json as it uses manifest typing)
 * this example doesn't use the code generation, instead we use GenericRecord.
 *
 * GenericRecord uses the schema to verify that we only specify valid fields.
 * DatumWriter converts Java objects into an in-memory serialized format.
 *
 * manifest typing:
 * (programming) A typing where the software programmer explicitly identifies the type of each declared variable.
 */
public class AvroSerializerWithGenericRecord {
    private static Logger logger= LoggerFactory.getLogger(AvroSerializerWithGenericRecord.class);
    public static void main(String[] args) {
        Schema schema = AvroSerializerWithGenericRecord.getFromSchemaFile();
        GenericData.Record avroRecord = new GenericRecordBuilder(schema).set("name", "sadeq").set("favourite_number", 7).build();
        GenericDatumWriter<GenericRecord> genericDatumWriter = new GenericDatumWriter<>(schema);
        try(ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()){
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, byteArrayOutputStream);// avro encodes to json or binary
            genericDatumWriter.write(avroRecord,jsonEncoder);
            jsonEncoder.flush();
            logger.info("serialized datum: {}", byteArrayOutputStream.toString());
        }catch (IOException e){
            logger.error("avro serialization failed!",e);
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

}
