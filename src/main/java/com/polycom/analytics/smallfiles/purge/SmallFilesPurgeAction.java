package com.polycom.analytics.smallfiles.purge;

import static com.polycom.analytics.smallfiles.shared.Constants.NAME_NODE;
import static com.polycom.analytics.smallfiles.shared.Constants.OUTPUTFILE_FIELDS_SEP;
import static com.polycom.analytics.smallfiles.shared.Constants.SMALLFILESWRITE_OUTPUTFILE;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class SmallFilesPurgeAction
{
    private static final Logger log = LoggerFactory.getLogger(SmallFilesPurgeAction.class);

    private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern("yyyy/MM/dd");

    private static FileSystem hdfs;

    private static String jobSuccIndicatorFile;

    private static String suffixOfSmallFilesWrite;

    private static void validateDate(LocalDate startDateInclusive, LocalDate endDateExclusive)
    {
        if (startDateInclusive.isAfter(endDateExclusive))
        {
            String msg = String.format("startDateInclusive[%s] should NOT after endDateExclusive[%s]",
                    startDateInclusive, endDateExclusive);
            throw new IllegalArgumentException(msg);
        }
    }

    private static void init() throws IOException, URISyntaxException
    {
        Configuration config = new Configuration();
        String nameNode = System.getProperty(NAME_NODE);
        hdfs = FileSystem.get(new URI(nameNode), config);

    }

    public static void main(String[] args) throws IOException, URISyntaxException
    {
        String startDateStrInclusive = args[0];
        String endDateStrExclusive = args[1];
        /*String startDateStrInclusive = "2018/04/30";
        String endDateStrExclusive = "2018/04/30";*/
        Path jobInfoDir = new Path(args[2]);
        suffixOfSmallFilesWrite = args[3];
        jobSuccIndicatorFile = args[4];
        //Path jobInfoDir = new Path("/user/jobinfo/");

        LocalDate startDateInclusive = LocalDate.parse(startDateStrInclusive, PATTERN);
        LocalDate endDateExclusive = LocalDate.parse(endDateStrExclusive, PATTERN);
        validateDate(startDateInclusive, endDateExclusive);
        try
        {
            init();
            List<Path> dirs = generateDirPaths(jobInfoDir, startDateInclusive, endDateExclusive);
            log.info("the dirs going through: {}", dirs);
            for (Path dir : dirs)
            {

                if (checkAndPurge(dir))
                {
                    // hdfs.delete(dir, true);
                    log.info("jobInfo dir[{}] is removed, as successful purge done", dir);
                }
            }
        }
        finally
        {
            clean();
        }
    }

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

    private static boolean checkAndPurge(Path dir) throws IOException, FileNotFoundException
    {
        boolean isSucc = false;
        if (hdfs.exists(dir))
        {
            Path jobSuccPath = new Path(dir, jobSuccIndicatorFile);
            log.info("jobSuccIndicatorFile: {}, exist:{}", jobSuccPath, hdfs.exists(jobSuccPath));
            if (hdfs.exists(jobSuccPath))
            {

                Path[] paths = FileUtil.stat2Paths(hdfs.listStatus(dir, new PathFilter()
                {

                    @Override
                    public boolean accept(Path p)
                    {
                        return p.getName().endsWith(suffixOfSmallFilesWrite);

                    }
                }));
                if (paths == null || paths.length != 1)
                {
                    String msg = String.format(
                            "only 1 SmallFilesWrite file with suffix[%s] is expected, but actual file number is %s",
                            suffixOfSmallFilesWrite, paths == null ? null : paths.length);
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                Path smallFilesWritePath = paths[0];
                log.info("smallFilesWritePath: {}", smallFilesWritePath);
                doPurgeBySmallFilesWriteFile(smallFilesWritePath);
                isSucc = true;
            }
            else
            {
                log.warn("the jobInfo directory[{}] NOT successful, skip it", dir);
            }
        }
        else
        {
            log.info("jobInfo directory[{}] non-exist, skip it", dir);
        }
        return isSucc;
    }

    private static void doPurgeBySmallFilesWriteFile(Path smallFilesWritePath) throws IOException
    {
        String line = null;
        Path deleteFile = null;
        Set<Path> parentPaths = Sets.newHashSet();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(smallFilesWritePath))))
        {
            while ((line = br.readLine()) != null)
            {
                log.info("line: {}", line);
                deleteFile = extractDeleteFilePath(line);
                log.info("deleteFile: {}", deleteFile);
                parentPaths.add(deleteFile.getParent());
                if (hdfs.exists(deleteFile))
                {
                    log.info("ready to deleteFile: {}", deleteFile);
                    //hdfs.delete(deleteFile, true);
                }
            }
        }
        log.info("parentPaths: {}", parentPaths);
        for (Path parentDir : parentPaths)
        {
            FileStatus[] subFileStatuses = hdfs.listStatus(parentDir);
            if (subFileStatuses == null || subFileStatuses.length == 0)
            {
                // hdfs.delete(parentDir, true);
                log.info("directory[{}] is empty, remove it");

            }
        }

    }

    private static Path extractDeleteFilePath(String line)
    {
        String[] fields = line.split(OUTPUTFILE_FIELDS_SEP);
        if (fields == null || fields.length != 2)
        {
            log.error("the line:{} of {} not following format: [originalFilePath]{}[ArchivedFilePath]", line,
                    SMALLFILESWRITE_OUTPUTFILE, OUTPUTFILE_FIELDS_SEP);
            return null;
        }
        return new Path(fields[0]);
    }

    private static List<Path> generateDirPaths(Path jobInfoDir, LocalDate startDateInclusive,
            LocalDate endDateExclusive)
    {
        Period period = Period.between(startDateInclusive, endDateExclusive);
        int days = period.getDays();

        List<Path> dirs = Lists.newArrayListWithCapacity(days);
        if (days == 0)
        {
            dirs.add(new Path(jobInfoDir, startDateInclusive.format(PATTERN)));
        }
        else
        {
            for (int i = 0; i < days; i++)
            {
                dirs.add(new Path(jobInfoDir, startDateInclusive.plusDays(i).format(PATTERN)));
            }
        }
        return dirs;
    }
}
