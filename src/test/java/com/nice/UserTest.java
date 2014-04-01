package com.nice;

import com.nice.model.User;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class UserTest {

    @Test
    public void testUserWithCodeGeneration() throws IOException {

        User user1 = User.newBuilder()
                .setName("Ciccio")
                .setFavoriteColor("Blue")
                .setFavoriteNumber(4)
                .build();

        User user2 = User.newBuilder()
                .setName("Ciccio")
                .setFavoriteColor("Blue")
                .setFavoriteNumber(4)
                .build();

        File f = new File("src/main/data/users.avro");

        SpecificDatumWriter<User> writer = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> fileWriter = new DataFileWriter<User>(writer);

        //Populating the writer and flushing the serialized data on disk
        fileWriter.create(user1.getSchema(), f);
        fileWriter.append(user1);
        fileWriter.append(user2);
        fileWriter.close();
    }

    @Test
    public void testUserWithoutCodeGeneration() throws IOException {

        Schema schema = new Schema.Parser().parse(new File("src/main/avro/user.avsc"));

        GenericData.Record user1 = new GenericData.Record(schema);
        user1.put("name", "Ciccio");
        user1.put("favorite_number", 7);
        user1.put("favorite_color", "blue");

        GenericData.Record user2 = new GenericData.Record(schema);
        user2.put("name", "Ciccio");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "blue");

        GenericDatumWriter<GenericRecord> genericWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(genericWriter);
        fileWriter.setCodec(CodecFactory.snappyCodec());

        //Populating the writer and flushing the serialized data on disk
        fileWriter.create(schema, new File("src/main/data/SchemaLess.avro"));
        fileWriter.append(user1);
        fileWriter.append(user2);
        fileWriter.close();
    }

    @Test
    public void testUserWithoutCodeGenerationAndDeSerialization() throws IOException {

        Schema schema = new Schema.Parser().parse(new File("src/main/avro/user.avsc"));

        GenericData.Record user = new GenericData.Record(schema);
        user.put("name", "Ciccio");
        user.put("favorite_number", 4);
        user.put("favorite_color", "blue");

        GenericDatumWriter<GenericRecord> genericWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(genericWriter);

        File file = new File("src/main/data/toDeserialize.avro");
        //Populating the writer and flushing the serialized data on disk
        fileWriter.create(schema, file);
        fileWriter.append(user);
        fileWriter.close();

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(file, reader);


        for (GenericRecord record : fileReader) {
            System.out.println(record);
        }
    }
}
