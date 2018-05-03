package com.polycom.analytics.smallfiles;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.file.SortedKeyValueFile;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class SmallFilesRead
{
    private static final String FIELD_FILENAME = "filename";
    private static final String FIELD_CONTENTS = "contents";

    public static void readFromAvro(InputStream is) throws IOException
    {
        DataFileStream<Object> reader = new DataFileStream<>(is, new GenericDatumReader<>());

        for (Object o : reader)
        {
            GenericRecord r = (GenericRecord) o;
            System.out.println(
                    r.get(FIELD_FILENAME) + ": " + new String(((ByteBuffer) r.get(FIELD_CONTENTS)).array()));
        }
        IOUtils.cleanup(null, is);
        IOUtils.cleanup(null, reader);
    }

    public static void readFromSortedKeyValueFile(Configuration conf, Path p, String key) throws IOException
    {
        SortedKeyValueFile.Reader.Options readerOptions = new SortedKeyValueFile.Reader.Options()
                .withKeySchema(Schema.create(Schema.Type.STRING)).withValueSchema(Schema.create(Schema.Type.BYTES))
                .withConfiguration(conf).withPath(p);
        try (SortedKeyValueFile.Reader<CharSequence, ByteBuffer> reader = new SortedKeyValueFile.Reader<>(
                readerOptions))
        {
            for (AvroKeyValue<CharSequence, ByteBuffer> item : reader)
            {
                System.out.println(item.getKey());
            }
            System.out.println(new String(reader.get(key).array()));
        }
    }

    public static void main(String[] args) throws IOException
    {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        // System.setProperty("hadoop.home.dir", "/");
        Configuration config = new Configuration();
        String fileType = System.getProperty("type");
        Path destPath = new Path(args[0]);
        String key = args[1];
        if ("avro".equals(fileType))
        {
            FileSystem hdfs = FileSystem.get(config);

            InputStream is = hdfs.open(destPath);
            readFromAvro(is);
        }
        else
        {
            readFromSortedKeyValueFile(config, destPath, key);
        }

    }

}
