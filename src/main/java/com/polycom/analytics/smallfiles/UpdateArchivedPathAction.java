package com.polycom.analytics.smallfiles;

import static com.polycom.analytics.smallfiles.shared.Constants.NAME_NODE;
import static com.polycom.analytics.smallfiles.shared.Constants.OUTPUTFILE_FIELDS_SEP;
import static com.polycom.analytics.smallfiles.shared.Constants.SMALLFILESWRITE_OUTPUTFILE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.polycom.analytics.smallfiles.es.ESClient;
import com.polycom.analytics.smallfiles.pojo.ArchivedPathDocLocation;

public class UpdateArchivedPathAction
{

    private static final String ES_HOSTS = "esHosts";
    private static final Logger log = LoggerFactory.getLogger(UpdateArchivedPathAction.class);

    public static Map<String, ArchivedPathDocLocation> initFileIDToArchivedPathDocLocationMap(
            Path outputPath) throws Exception
    {
        Map<String, ArchivedPathDocLocation> fileIDToArchiveedPathDocLocationMap = Maps.newLinkedHashMap();
        Configuration config = new Configuration();
        String nameNode = System.getProperty(NAME_NODE);
        FileSystem hdfs = null;
        try
        {
            hdfs = FileSystem.get(new URI(nameNode), config);
            String line = null;
            ArchivedPathDocLocation aploc;
            try (BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(outputPath))))
            {
                while ((line = br.readLine()) != null)
                {
                    aploc = constructArchivedPathDocLocation(line);
                    if (aploc != null)
                    {
                        fileIDToArchiveedPathDocLocationMap.put(aploc.getFileID(), aploc);
                    }
                }
            }
        }
        catch (IOException | URISyntaxException e)
        {

            log.error("Unexpected IOException when initFileIDToArchivedPathDocLocationMap", e);
            throw e;

        }
        finally
        {
            if (hdfs != null)
            {
                try
                {
                    hdfs.close();
                }
                catch (IOException e)
                {

                    log.error("Unexpected IOException when hdfs.close()", e);

                }
            }
        }

        /*    
        ArchivedPathDocLocation one = new ArchivedPathDocLocation();
        
        one.setArchivedPath("/user/hdfs/1.avro");
        fileIDToArchiveedPathDocLocationMap.put("zip040901", one);
        ArchivedPathDocLocation two = new ArchivedPathDocLocation();
        two.setArchivedPath("/user/hdfs/2.avro");
        fileIDToArchiveedPathDocLocationMap.put("zip040802", two);
        
        ArchivedPathDocLocation error = new ArchivedPathDocLocation();
        error.setArchivedPath("/user/hdfs/error.avro");
        fileIDToArchiveedPathDocLocationMap.put("error", error);*/
        int nbOfFileID = fileIDToArchiveedPathDocLocationMap.size();
        log.info("initFileIDToArchivedPathDocLocationMap-, nbOfFileID: {}", nbOfFileID);
        return fileIDToArchiveedPathDocLocationMap;
    }

    private static ArchivedPathDocLocation constructArchivedPathDocLocation(String line)
    {
        String[] fields = line.split(OUTPUTFILE_FIELDS_SEP);
        if (fields == null || fields.length != 2)
        {
            log.error("the line:{} of {} not following format: [originalFilePath],[ArchivedFilePath]", line,
                    SMALLFILESWRITE_OUTPUTFILE);
            return null;
        }
        String fileID = extractFileIDFromPath(fields[0]);
        if (fileID != null)
        {
            ArchivedPathDocLocation aoplc = new ArchivedPathDocLocation();
            aoplc.setFileID(fileID);
            aoplc.setArchivedPath(fields[1]);
            return aoplc;
        }
        return null;
    }

    private static String extractFileIDFromPath(String origFilePath)
    {
        Path p = new Path(origFilePath);
        String fileName = p.getName();
        int i = fileName.indexOf('.');
        if (i != -1)
        {
            return fileName.substring(0, i);
        }
        log.error("bad format for fileName: {}, should be [fileID].[suffix]", fileName);
        return null;

    }

    public static void main(String[] args) throws Exception
    {
        String esHosts = System.getProperty(ES_HOSTS);
        Path outputPath = new Path(args[0]);
        Map<String, ArchivedPathDocLocation> fileIDToArchivedPathDocLocationMap = initFileIDToArchivedPathDocLocationMap(
                outputPath);
        ESClient client = null;
        try
        {
            if (!fileIDToArchivedPathDocLocationMap.isEmpty())
            {
                client = new ESClient(esHosts, fileIDToArchivedPathDocLocationMap);
                client.searchArchivedPathDocLocationByFileID();
                client.updateArchivedPathInES();

            }
            else
            {
                log.warn("fileIDToArchivedPathDocLocationMap is empty, do nothing");
            }
        }
        finally
        {
            if (client != null)
            {
                client.close();
            }
        }
    }

}
