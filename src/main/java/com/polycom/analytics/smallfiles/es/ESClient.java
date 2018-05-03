package com.polycom.analytics.smallfiles.es;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.ArrayUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Builder;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.polycom.analytics.smallfiles.pojo.ArchivedPathDocLocation;

public class ESClient implements Closeable
{
    private static final String UPDATE_ARCHIVED_PATH = "{\"archivedPath\":\"%s\"}";
    private static final int ES_TIMEOUT_IN_SECONDS = 60;
    private static final int ES_SEARCH_BULK_TIMEOUT_IN_SECONDS = ES_TIMEOUT_IN_SECONDS * 2;
    private static final int ES_UPDATE_BULK_TIMEOUT_IN_SECONDS = ES_TIMEOUT_IN_SECONDS * 4;
    private static final String _ID = "_id";
    private static final String _TYPE = "_type";
    private static final String _INDEX = "_index";
    private static final String FILE_ID = "fileID";
    private static final Logger log = LoggerFactory.getLogger(ESClient.class);
    private RestClient restClient;
    private RestHighLevelClient esClient;
    private Map<String, ArchivedPathDocLocation> fileIDToArchiveedPathDocLocationMap;
    private static final String FILE_UPLOAD_INDEX = "devicelogupload-*";
    private int sizeInBulk = 1000;

    /*  public boolean initFileIDToArchivedPathDocLocationMap()
    {
        fileIDToArchiveedPathDocLocationMap = Maps.newLinkedHashMap();
        ArchivedPathDocLocation one = new ArchivedPathDocLocation();
    
        one.setArchivedPath("/user/hdfs/one.avro");
        fileIDToArchiveedPathDocLocationMap.put("zip040901", one);
        ArchivedPathDocLocation two = new ArchivedPathDocLocation();
        two.setArchivedPath("/user/hdfs/two.avro");
        fileIDToArchiveedPathDocLocationMap.put("zip040802", two);
    
        ArchivedPathDocLocation error = new ArchivedPathDocLocation();
        error.setArchivedPath("/user/hdfs/error.avro");
        fileIDToArchiveedPathDocLocationMap.put("error", error);
        int nbOfFileID = fileIDToArchiveedPathDocLocationMap.size();
        log.info("after initFileIDToArchivedPathDocLocationMap, nbOfFileID: {}", nbOfFileID);
        if (nbOfFileID < 1)
        {
            return false;
        }
    
        return true;
    
    }*/

    private class SearchResponseListener implements ActionListener<SearchResponse>
    {
        private final ESAsyncResult result = new ESAsyncResult();
        private final CountDownLatch latch;

        public ESAsyncResult getResult()
        {
            return result;
        }

        public SearchResponseListener(CountDownLatch latch)
        {
            super();
            this.latch = latch;
        }

        @Override
        public void onResponse(SearchResponse response)
        {
            RestStatus status = response.status();
            TimeValue took = response.getTook();
            Boolean isTerminatedEarly = response.isTerminatedEarly();
            boolean isTimedOut = response.isTimedOut();
            log.info("search to ES executed, status:{}, took:{}ms, isTerminatedEarly:{}, isTimedOut:{}", status,
                    took.millis(), isTerminatedEarly, isTimedOut);
            String msg = null;
            if (!RestStatus.OK.equals(status))
            {
                msg = String.format("status of searchResponse to ES: %s, NOT OK(200)", status);
                errorHandle(msg);
                return;
            }
            if (isTimedOut)
            {
                msg = String.format("search to ES timeout, timeout value: %s secs", ES_TIMEOUT_IN_SECONDS);
                errorHandle(msg);
                return;
            }
            SearchHits hits = response.getHits();
            SearchHit[] searchHits = hits.getHits();
            log.info("size of hits: {}", searchHits.length);
            Map<String, Object> sourceAsMap;
            String fileID;
            ArchivedPathDocLocation aploc;
            for (SearchHit hit : searchHits)
            {
                sourceAsMap = hit.getSourceAsMap();
                fileID = (String) sourceAsMap.get(FILE_ID);
                if (fileID != null)
                {
                    aploc = fileIDToArchiveedPathDocLocationMap.get(fileID);
                    aploc.setIndex(hit.getIndex());
                    aploc.setType(hit.getType());
                    aploc.setId(hit.getId());

                }
                else
                {
                    log.warn("unexpected case: fileID not appearing in searchResponse, hit:{}", sourceAsMap);
                }
            }
            latch.countDown();
        }

        private void errorHandle(String msg)
        {

            logAndSetResult(msg, new IllegalStateException(msg));
            latch.countDown();
        }

        private void errorHandle(String msg, Throwable t)
        {
            logAndSetResult(msg, t);
            latch.countDown();
        }

        private void logAndSetResult(String msg, Throwable t)
        {
            log.error(msg);
            result.setMsg(msg);
            result.setSucc(false);
            result.setThrowable(t);
        }

        @Override
        public void onFailure(Exception e)
        {
            errorHandle("error when sending search request to ES", e);
            return;
        }

    }

    public void searchArchivedPathDocLocationByFileID()
    {
        int nbOfFileID = fileIDToArchiveedPathDocLocationMap.size();

        String[] fileIDs = fileIDToArchiveedPathDocLocationMap.keySet().toArray(new String[0]);
        int start = 0;
        int limit;
        String[] processFileIDs;
        int c = (nbOfFileID % sizeInBulk == 0 ? nbOfFileID / sizeInBulk : nbOfFileID / sizeInBulk + 1);
        CountDownLatch latch = new CountDownLatch(c);
        List<SearchResponseListener> listeners = Lists.newLinkedList();
        SearchResponseListener listener;
        while (start < nbOfFileID)
        {
            limit = start + sizeInBulk;
            if (limit > nbOfFileID)
            {
                limit = nbOfFileID;
            }
            processFileIDs = (String[]) ArrayUtils.subarray(fileIDs, start, limit);
            listener = new SearchResponseListener(latch);
            listeners.add(listener);
            sendSearchRequestToES(processFileIDs, listener);
            start = limit;
        }
        boolean isAllAsyncFinished;

        try
        {
            isAllAsyncFinished = latch.await(ES_SEARCH_BULK_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            log.error("unexpected InterruptedException when searchArchivedPathDocLocationByFileID", e);
            throw new IllegalStateException(e);

        }
        if (isAllAsyncFinished)
        {
            boolean isAllAsyncSucc = true;
            for (SearchResponseListener srListener : listeners)
            {
                isAllAsyncSucc = isAllAsyncSucc && srListener.getResult().isSucc();
            }
            if (!isAllAsyncSucc)
            {
                String msg = "some searchRequest to get DocLocation has error";
                log.error(msg);
                throw new IllegalStateException(msg);
            }

        }
        else
        {
            String msg = String.format("waiting async search response(s) Timeout, timeout value is: %s",
                    ES_SEARCH_BULK_TIMEOUT_IN_SECONDS);
            log.error(msg);
            throw new IllegalStateException(msg);
        }

    }

    private void sendSearchRequestToES(String[] fileIDArray, SearchResponseListener listener)
    {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(QueryBuilders.constantScoreQuery(
                QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery(FILE_ID, fileIDArray))));
        String[] includeFields = new String[] { _INDEX, _TYPE, _ID, FILE_ID };

        searchSourceBuilder.fetchSource(includeFields, ArrayUtils.EMPTY_STRING_ARRAY);
        searchSourceBuilder.size(sizeInBulk);
        searchSourceBuilder.timeout(new TimeValue(ES_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS));
        SearchRequest searchRequest = new SearchRequest(FILE_UPLOAD_INDEX);
        searchRequest.source(searchSourceBuilder);
        esClient.searchAsync(searchRequest, listener);
    }

    public void updateArchivedPathInES()
    {
        BulkProcessorListener listener = createBulkListener();
        BulkProcessor bulkProcessor = createBulkProcessor(listener);
        UpdateRequest updateReq;
        ArchivedPathDocLocation aploc;

        for (String fileID : fileIDToArchiveedPathDocLocationMap.keySet())
        {
            aploc = fileIDToArchiveedPathDocLocationMap.get(fileID);
            updateReq = constructUpdateRequest(fileID, aploc);
            if (updateReq != null)
            {
                bulkProcessor.add(updateReq);
            }

        }
        boolean isAllFinished;
        try
        {
            isAllFinished = bulkProcessor.awaitClose(ES_UPDATE_BULK_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {

            log.error("Unexpected InterruptedException when updateArchivedPathInES", e);
            throw new IllegalStateException(e);

        }
        if (!isAllFinished)
        {
            String msg = String.format("partial update ArchivePath requests are timeout, timeout value: {}",
                    ES_UPDATE_BULK_TIMEOUT_IN_SECONDS);
            log.error(msg);
            throw new IllegalStateException(msg);
        }
        if (!listener.isAllSucc)
        {
            String msg = "there are some requests failed during bulk updateArchivedPathInES";

            log.error(msg);
            throw new IllegalStateException(msg);
        }

    }

    private UpdateRequest constructUpdateRequest(String fileID, ArchivedPathDocLocation aploc)
    {
        if (aploc.getIndex() != null && aploc.getType() != null && aploc.getId() != null)
        {
            UpdateRequest updateRequest = new UpdateRequest(aploc.getIndex(), aploc.getType(), aploc.getId());
            String doc = String.format(UPDATE_ARCHIVED_PATH, aploc.getArchivedPath());
            updateRequest.doc(doc, XContentType.JSON);
            return updateRequest;

        }
        log.warn("can't locate doc in ES by fileID: {}, ArchivedPathDocLocation: {}", fileID, aploc);
        return null;

    }

    private BulkProcessor createBulkProcessor(BulkProcessor.Listener listener)
    {
        ThreadPool threadPool = new ThreadPool(
                Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "rest-high-level-client").build());

        BulkProcessor.Builder builder = new Builder(esClient::bulkAsync, listener, threadPool);
        builder.setBulkActions(sizeInBulk); //default 1000
        builder.setBulkSize(new ByteSizeValue(2L, ByteSizeUnit.MB)); //default 5m
        builder.setConcurrentRequests(5); //default 1
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L)); //default NO set
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));
        return builder.build();
    }

    private static class BulkProcessorListener implements BulkProcessor.Listener
    {

        private boolean isAllSucc = true;

        public boolean isAllSucc()
        {
            return isAllSucc;
        }

        @Override
        public void beforeBulk(long executionId, BulkRequest request)
        {
            int nbOfActions = request.numberOfActions();

            log.info("executing bulk[{}] with {} requests", executionId, nbOfActions);

        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response)
        {
            if (response.hasFailures())
            {
                String msg = String.format("bulk [%s] executed with failures: %s", executionId,
                        response.buildFailureMessage());
                errorHandle(null, msg);

            }
            else
            {

                log.info("bulk [{}] completed in {} milliseconds", executionId, response.getTook().getMillis());
            }

        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure)
        {
            String msg = String.format("failed to execute bulk [%s]", executionId);
            errorHandle(failure, msg);

        }

        public void errorHandle(Throwable failure, String msg)
        {
            log.error(msg, failure);
            isAllSucc = false;
        }

    }

    private BulkProcessorListener createBulkListener()
    {
        return new BulkProcessorListener();

    }

    /* public static void main(String[] args) throws IOException
    {
    
        System.out.println(System.getProperty("test"));
        String esHosts = "hanalytics-elasticsearch5-scus-1:9200,hanalytics-elasticsearch5-scus-2:9200,hanalytics-elasticsearch5-scus-3:9200";
        ESClient client = new ESClient(esHosts);
        try
        {
            if (client.initFileIDToArchivedPathDocLocationMap())
            {
                client.searchArchivedPathDocLocationByFileID();
                client.updateArchivedPathInES();
                // client.bulkOp();
            }
        }
        finally
        {
            client.close();
        }
    
    }
    */
    public ESClient(String esHosts, Map<String, ArchivedPathDocLocation> fileIDToArchivedPathDocLocationMap)
    {
        this.fileIDToArchiveedPathDocLocationMap = fileIDToArchivedPathDocLocationMap;
        String[] esHostArray = esHosts.split(",");
        List<HttpHost> esHostList = Lists.newArrayListWithExpectedSize(esHostArray.length);
        String[] hostConf = null;

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
        if (esHostList.isEmpty())
        {
            throw new IllegalArgumentException(
                    "es hosts are are parsed as empty, original es hosts string: " + esHosts);
        }

        restClient = RestClient.builder(esHostList.toArray(new HttpHost[esHostList.size()])).build();
        esClient = new RestHighLevelClient(restClient);
    }

    @Override
    public void close() throws IOException
    {
        /*in es client 5.6.7 high level rest client, RestHighLevelClient NOT support close method
         * 
         */
        if (restClient != null)
        {
            restClient.close();
        }

    }

}
