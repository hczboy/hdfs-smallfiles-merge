package com.polycom.analytics.smallfiles;

import static com.polycom.analytics.smallfiles.shared.Constants.MAX_FILE_SIZE;
import static com.polycom.analytics.smallfiles.shared.Constants.NAME_NODE;
import static com.polycom.analytics.smallfiles.shared.Constants.OUTPUTFILE_FIELDS_SEP;
import static com.polycom.analytics.smallfiles.shared.Constants.SMALLFILESWRITE_OUTPUTFILE;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.file.SortedKeyValueFile;
import org.apache.avro.hadoop.file.SortedKeyValueFile.Writer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class SmallFilesWriteAction
{

    private static final Logger log = LoggerFactory.getLogger(SmallFilesWriteAction.class);

    /* private static final String FIELD_FILENAME = "filename";
    private static final String FIELD_CONTENTS = "contents";
    
    private static final String SCHEMA_JSON = "{\"type\": \"record\", \"name\": \"SmallFilesRecord\", "
            + "\"fields\": [" + "{\"name\":\"" + FIELD_FILENAME + "\", \"type\":\"string\"}," + "{\"name\":\""
            + FIELD_CONTENTS + "\", \"type\":\"bytes\"}]}";
    
    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);
    */
    private static FileSystem hdfs;

    private static int maxFileSize = 0;
    private static final int DEFAULT_MAXFILESIZE = 10 * 1024 * 1024;

    /*    private static RestClient rClient;
    
    private static RestHighLevelClient client;*/

    private static List<Path> writtenToAvroFilesList = Lists.newLinkedList();

    private static void writeToSortedKeyValueFile(Path srcDir, Path destDir) throws IOException
    {
        SortedKeyValueFile.Writer.Options writerOptions = new SortedKeyValueFile.Writer.Options()
                .withKeySchema(Schema.create(Schema.Type.STRING)).withValueSchema(Schema.create(Schema.Type.BYTES))
                .withConfiguration(hdfs.getConf()).withPath(destDir).withIndexInterval(5);

        try (Writer<CharSequence, ByteBuffer> writer = new SortedKeyValueFile.Writer<>(writerOptions);)
        {
            List<String> overSizeFiles = null;
            FileStatus[] statuses = hdfs.listStatus(srcDir);
            Path p = null;
            long fileLen = -1L;
            ByteArrayOutputStream bos = null;

            FSDataInputStream in = null;
            for (FileStatus status : statuses)
            {
                p = status.getPath();

                fileLen = status.getLen();

                if (fileLen > maxFileSize)
                {
                    if (overSizeFiles == null)
                    {
                        overSizeFiles = Lists.newLinkedList();
                    }
                    overSizeFiles.add(p.toString());
                }
                else
                {

                    in = hdfs.open(p);
                    bos = new ByteArrayOutputStream((int) fileLen);
                    IOUtils.copyBytes(in, bos, 4096, true);
                    writer.append(p.getName(), ByteBuffer.wrap(bos.toByteArray()));
                    writtenToAvroFilesList.add(p);
                }
            }

            if (overSizeFiles != null)
            {
                log.info("over size[{} bytes] files: {}", maxFileSize, overSizeFiles);
            }
        }
        catch (IOException e)
        {
            log.error("Unexpected IOException in writeToSortedKeyValueFile, srcDir:{}, destDir: {}", srcDir,
                    destDir, e);
            List<Throwable> suppressedEx = Arrays.asList(e.getSuppressed());
            if (CollectionUtils.isNotEmpty(suppressedEx))
            {
                for (Throwable t : suppressedEx)
                {
                    log.error("suppressed ex occurs when close SortedKeyValueFile.Writer", t);
                }
            }
            throw e;

        }
    }

    /* private static void writeToAvro(Path srcDir, Path destFile) throws IOException
    {
        @SuppressWarnings("resource")
    
        DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>()).setSyncInterval(8192);
    
        writer.setCodec(CodecFactory.snappyCodec());
        OutputStream os = null;
    
        try
        {
            os = hdfs.create(destFile);
            writer.create(SCHEMA, os);
            FileStatus[] statuses = hdfs.listStatus(srcDir);
            //Path[] paths = FileUtil.stat2Paths(statuses);
            GenericRecord record = null;
            Path p = null;
            long fileLen = -1L;
            ByteArrayOutputStream bos = null;
            List<String> overSizeFiles = null;
            FSDataInputStream in = null;
            for (FileStatus status : statuses)
            {
                p = status.getPath();
    
                fileLen = status.getLen();
    
                if (fileLen > maxfileSize)
                {
                    if (overSizeFiles == null)
                    {
                        overSizeFiles = Lists.newLinkedList();
                    }
                    overSizeFiles.add(p.toString());
                }
                else
                {
    
                    in = hdfs.open(p);
                    bos = new ByteArrayOutputStream((int) fileLen);
                    IOUtils.copyBytes(in, bos, 4096, true);
                    record = new GenericData.Record(SCHEMA);
                    record.put(FIELD_FILENAME, p.getName());
    
                    record.put(FIELD_CONTENTS, ByteBuffer.wrap(bos.toByteArray()));
                    writer.append(record);
                    writtenToAvroFilesList.add(p);
                }
            }
            if (overSizeFiles != null)
            {
                log.info("over size[{} bytes] files: {}", maxfileSize, overSizeFiles);
            }
        }
        catch (IOException e)
        {
    
            log.error("Unexpected IOException in writeToAvro, srcDir:{}, destFile: {}", srcDir, destFile, e);
            throw e;
    
        }
        finally
        {
            IOUtils.cleanup(null, writer);
            IOUtils.cleanup(null, os);
    
        }
    
    }*/

    public static void writeArchivedFilePathsToFile(Path outputFile, Path destFile) throws IOException
    {
        /* OutputStream os = null;
        PrintWriter pw;
        os = hdfs.create(outputFile);*/
        StringBuilder sb;
        try (PrintWriter pw = new PrintWriter(hdfs.create(outputFile)))
        {
            for (Path p : writtenToAvroFilesList)
            {
                sb = new StringBuilder(Path.getPathWithoutSchemeAndAuthority(p).toString());
                sb.append(OUTPUTFILE_FIELDS_SEP).append(Path.getPathWithoutSchemeAndAuthority(destFile));

                pw.println(sb);
            }
        }
        catch (IOException e)
        {

            log.error("IOException when writeOutputFile", e);
            List<Throwable> suppressedEx = Arrays.asList(e.getSuppressed());
            if (CollectionUtils.isNotEmpty(suppressedEx))
            {
                for (Throwable t : suppressedEx)
                {
                    log.error("suppressed ex occur when close PrintWriter", t);
                }
            }
            throw e;
        }

    }

    private static void init() throws IOException, URISyntaxException
    {
        Configuration config = new Configuration();
        String nameNode = System.getProperty(NAME_NODE);
        String maxFileSizeStr = System.getProperty(MAX_FILE_SIZE);
        hdfs = FileSystem.get(new URI(nameNode), config);
        if (maxFileSizeStr != null)
        {
            try
            {
                maxFileSize = Integer.valueOf(maxFileSizeStr).intValue();
                if (maxFileSize <= 0)
                {
                    maxFileSize = DEFAULT_MAXFILESIZE;
                    log.warn("value of property[{}] should >0, but is {}, apply default value: {}", MAX_FILE_SIZE,
                            maxFileSize, DEFAULT_MAXFILESIZE);
                }
            }
            catch (NumberFormatException e)
            {

                log.error("value of property[{}] should be integer, but is {}, apply default value: {}",
                        MAX_FILE_SIZE, maxFileSizeStr, DEFAULT_MAXFILESIZE);

                maxFileSize = DEFAULT_MAXFILESIZE;
            }
        }
        else
        {
            maxFileSize = DEFAULT_MAXFILESIZE;
        }
    }

    /*private static void initEsClient()
    {
        String esHosts = "hanalytics-elasticsearch5-scus-1:9200,hanalytics-elasticsearch5-scus-2:9200,hanalytics-elasticsearch5-scus-3:9200";
        String[] esHostArray = esHosts.split(",");
        List<HttpHost> esHostList = Lists.newArrayListWithExpectedSize(esHostArray.length);
        String[] hostConf = null;
        HttpHost esHost = null;
        for (String esHostStr : esHostArray)
        {
            hostConf = esHostStr.split(":");
            if (hostConf.length == 2)
            {
                esHostList.add(new HttpHost(hostConf[0], Integer.valueOf(hostConf[1])));
            }
            else
            {
                log.warn("incorrect es host:{}, ignore it", esHostStr);
            }
        }
        log.info("initEsClient+, esHosts: {}", esHostList);
        rClient = RestClient.builder(esHostList.toArray(new HttpHost[esHostList.size()])).build();
        client = new RestHighLevelClient(rClient);
    
    }*/
    private static void clean()
    {
        if (hdfs != null)
        {
            try
            {
                hdfs.close();
            }
            catch (IOException e)
            {

                log.error("Unexpected Exception when hdfs.close()", e);

            }
        }
    }

    public static void main(String[] args) throws Exception
    {

        System.setProperty("HADOOP_USER_NAME", "hdfs");
        //System.setProperty("hadoop.home.dir", "/");

        // System.setProperty(NAME_NODE, "hdfs://plcmhdfsservice");

        try
        {
            init();
            //initEsClient();
        }
        catch (IOException | URISyntaxException e)
        {

            log.error("failed to do init()", e);
            throw e;

        }

        Path srcDir = new Path(args[0]);
        Path destDir = new Path(args[1]);

        Path outputFile = new Path(args[2]);

        try
        {
            //writeToAvro(srcDir, destFile);
            writeToSortedKeyValueFile(srcDir, destDir);
            Path destFile = new Path(destDir, SortedKeyValueFile.DATA_FILENAME);
            writeArchivedFilePathsToFile(outputFile, destFile);

            Properties props = new Properties();
            props.setProperty(SMALLFILESWRITE_OUTPUTFILE, outputFile.toString());
            dumpPropsToNextAction(props);
        }
        catch (Exception e)
        {
            cleanFiles(destDir, outputFile);
            throw e;
        }
        finally
        {
            clean();
        }

        ///

        /*initEsClient();
        
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("devicelogupload-2018.04.08");
        SearchResponse response = client.search(searchRequest);
        System.out.println(response.toString());
        rClient.close();*/
        /*ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
        ByteArrayInputStream bis = new ByteArrayInputStream("12345678".getBytes());
        IOUtils.copyBytes(bis, bos, 512, true);
        System.out.println(new String(bos.toByteArray()));
        System.out.println("end");*/

    }

    public static void dumpPropsToNextAction(Properties props) throws IOException
    {
        File file = new File(System.getProperty("oozie.action.output.properties"));

        try
        {
            OutputStream pos = new FileOutputStream(file);
            props.store(pos, "");
            pos.close();
        }
        catch (IOException e)
        {

            log.error("failed to write oozie output file", e);
            throw e;

        }

    }

    private static void cleanFiles(Path... deletePaths)
    {
        for (Path p : deletePaths)
        {
            try
            {
                if (hdfs.exists(p))
                {
                    hdfs.delete(p, true);
                }
            }
            catch (Exception e)
            {
                log.error("unexpected exception when cleanFiles: path: {}", p, e);
            }

        }
    }
}
