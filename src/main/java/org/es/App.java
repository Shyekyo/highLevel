package org.es;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.MultiSearchTemplateRequest;
import org.elasticsearch.script.mustache.MultiSearchTemplateResponse;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(String[] args) throws Exception{
        RestHighLevelClient client = getHighLevelClient();
        String index = "cdrindex";
        String id = "Nc-rdWwBncJK9AacSI38";
        synGet(client,index,id);
        //aynGet(client,index,id);
        //getNotExist(client);
        //getversionconflict(client);
        //existsRequestSynchronous(client, "posts", "1");
        //aynExistsRequest(client, "posts", "1");
        //existsSource(client,index, id);
        //IndexResponse response = indexDoc(client, index, id);
        //indexResposeInfo(response);
        //synDelete(client,index, id);
        //aynDelete(client,index, id);异步删除未成功
        close(client);
    }

    /**
     * 获取高阶api客户端
     * @return
     */
    private static RestHighLevelClient getHighLevelClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"))
        );
        return client;
    }

    /**
     *关闭客户端链接
     * @param client
     */
    private static void close(RestHighLevelClient client){
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入一个文档
     * DocWriteRequest.OpType.INDEX OR DocWriteRequest.OpType.CREATE
     * 只允许这两个选项，如果是更新操作就填成 index
     * 选项是create的情况下，并且文档存在的情况下，会跑出异常 冲突异常
     * 类似下面这个方法 getversionconflict
     * @return
     */
    private static IndexResponse indexDoc(RestHighLevelClient client,String index,String id)
    throws Exception{
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"update\"" +
                "}";
        IndexRequest request = new IndexRequest(index);
        request.id(id);
        request.source(jsonString, XContentType.JSON);
        //request.opType(DocWriteRequest.OpType.CREATE);
        request.opType(DocWriteRequest.OpType.INDEX);
        request.timeout(TimeValue.timeValueSeconds(1));
        //request.timeout("1s");
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        return response;
    }

    /**
     * indexrequest请求后返回response结果，response里包含有相关index的info
     * @param indexResponse
     */
    private static void indexResposeInfo(IndexResponse indexResponse){
        String index = indexResponse.getIndex();
        String id = indexResponse.getId();
        print(index);
        print(id);
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            print("0");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            print("1");
        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            print("====");
            print(shardInfo.getTotal());
            print(shardInfo.getSuccessful());
            print("====");
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure :
                    shardInfo.getFailures()) {
                String reason = failure.reason();
                print(reason);
            }
        }
    }

    /**
     * 根据需要查询index和id生成getrequest，给同步查询和异步查询提供便利
     * @param index
     * @param id
     * @return
     */
    private static GetRequest getDoc(String index,String id){
        GetRequest getRequest = new GetRequest(index,id);
        return getRequest;
    }

    /**
     * 同步查询
     * @param client
     * @param index
     * @param id
     * @throws Exception
     */
    private static void synGet(RestHighLevelClient client,String index,String id)throws Exception{
        GetRequest request = getDoc(index,id);
        /**
         * FetchSourceContext.DO_NOT_FETCH_SOURCE 参数意味着不拉取source
         * FetchSourceContext.FETCH_SOURCE 参数意味着拉取source
         * 默认值是允许拉取
         */
        //request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        //String[] includes=Strings.EMPTY_ARRAY;
        //String[] excludes= {"user"};
        /**
         * 如果实例化了一个FetchSourceContext，则需要指定includes和excludes,
         * 意味着检索的source里会包含includes和排除掉excludes
         */
        //FetchSourceContext fetchSourceContext = new FetchSourceContext(true,includes,excludes);
        //request.fetchSourceContext(fetchSourceContext);
        //String[] includes= {"user"};
        //request.storedFields(includes);
        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
        String index_ = getResponse.getIndex();
        String id_ = getResponse.getId();
        print(index_);
        print(id_);
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            print(version);
            String sourceAsString = getResponse.getSourceAsString();
            print(sourceAsString);
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            for(String str : sourceAsMap.keySet()){
                Object o = sourceAsMap.get(str);
                print("key => "+str+"  value => "+o);
            }
            byte[] sourceAsBytes = getResponse.getSourceAsBytes();
        }
    }

    /**
     * 异步查询
     * @param client
     * @param index
     * @param id
     * @throws Exception
     */
    private static void aynGet(RestHighLevelClient client,String index,String id)throws Exception{
        GetRequest getdoc = getDoc(index,id);
        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                String index_ = getResponse.getIndex();
                String id_ = getResponse.getId();
                print(index_);
                print(id_);
                if (getResponse.isExists()) {
                    long version = getResponse.getVersion();
                    String sourceAsString = getResponse.getSourceAsString();
                    print(sourceAsString);
                    Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                    for(String str : sourceAsMap.keySet()){
                        Object o = sourceAsMap.get(str);
                        print("key => "+str+"  value => "+o);
                    }
                    byte[] sourceAsBytes = getResponse.getSourceAsBytes();
                }
            }
            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        client.getAsync(getdoc, RequestOptions.DEFAULT,listener);
        Thread.sleep(5000);
    }

    /**
     * 查询index不存在的时候会抛出 ElasticsearchException
     * reststatus 返回码是404的话，就是未找到相关index
     * @param client
     * @throws Exception
     */
    private static void getNotExist(RestHighLevelClient client) throws Exception{
        GetRequest request = new GetRequest("does_not_exist", "1");
        try {
            GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                System.out.println(RestStatus.NOT_FOUND.getStatus());
            }
        }
    }

    /**
     * 查询版本冲突
     * reststatus 返回码是409的话，就是发生冲突
     * @param client
     * @throws Exception
     */
    private static void getversionconflict(RestHighLevelClient client) throws Exception{
        GetRequest request = new GetRequest("posts", "1").version(2);
        try {
            GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                System.out.println(RestStatus.CONFLICT.getStatus());
            }
        }
    }

    /**
     *同步查询是否存在index和id是否存在
     * 如果只需要判断某个index中的_doc是否存在的话用这个exists的就好，返回的是boolean
     * @param client
     * @param index
     * @param id
     * @return true or false
     * @throws Exception
     */
    private static void existsRequestSynchronous(RestHighLevelClient client,String index,String id)
    throws Exception{
        GetRequest getRequest = new GetRequest(
                index,
                id);
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        print(exists);
    }

    /**
     * 异步查询doc是否存在
     * 如果只需要判断某个index中的_doc是否存在的话用这个exists的就好，返回的是boolean
     * @param client
     * @param index
     * @param id
     * @throws Exception
     */
    private static void aynExistsRequest(RestHighLevelClient client,String index,String id)
            throws Exception{
        GetRequest getRequest = new GetRequest(
                index,
                id);
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        ActionListener<Boolean> listener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean exists) {
                print(exists);
            }
            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        client.existsAsync(getRequest, RequestOptions.DEFAULT, listener);
        Thread.sleep(5000);
    }

    /**
     * 还是判断index和doc是否存在，更简单的方法
     * 比existsRequestSynchronous，aynExistsRequest都简单的方法
     * @param client
     * @param index
     * @param id
     * @throws Exception
     */
    private static void existsSource(RestHighLevelClient client,String index,String id)
            throws Exception{
        GetRequest request = new GetRequest(
                index,
                id);
        boolean b = client.existsSource(request, RequestOptions.DEFAULT);
        request.versionType(VersionType.EXTERNAL);
        print(b);
    }
//DELETE API

    /**
     * 同步删除
     * @param client
     * @param index
     * @param id
     * @throws Exception
     */
    private static void synDelete(RestHighLevelClient client,String index,String id)
            throws Exception{
        DeleteRequest request = new DeleteRequest(
                index,
                id);
        //request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        //request.versionType(VersionType.EXTERNAL);
        DeleteResponse deleteResponse = client.delete(
                request, RequestOptions.DEFAULT);
    }

    /**
     * 很奇怪异步删除没有显示成功或者失败
     * @param client
     * @param index
     * @param id
     * @throws Exception
     */
    private static void aynDelete(RestHighLevelClient client,String index,String id)
            throws Exception{
        ActionListener<DeleteResponse> listener = new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                String index = deleteResponse.getIndex();
                String id = deleteResponse.getId();
                long version = deleteResponse.getVersion();
                print(index);
                print(id);
                print(version);
                ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
                if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                    print(shardInfo.getTotal());
                }
                if (shardInfo.getFailed() > 0) {
                    for (ReplicationResponse.ShardInfo.Failure failure :
                            shardInfo.getFailures()) {
                        String reason = failure.reason();
                        print(reason);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                print("delete failure");
            }
        };
        DeleteRequest request = new DeleteRequest(
                index,
                id);
        //request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        //request.versionType(VersionType.EXTERNAL);
        client.deleteAsync(request, RequestOptions.DEFAULT,listener);
    }
    /**
     *删除找不到的doc
     * @param client
     * @throws Exception
     */
    private static void deleteNotExist(RestHighLevelClient client,String index) throws Exception{
        DeleteRequest request = new DeleteRequest(index, "does_not_exist");
        DeleteResponse deleteResponse = client.delete(
                request, RequestOptions.DEFAULT);
        if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {

        }
    }

    private static void deleteVersionConflict(RestHighLevelClient client,String index,String id) throws Exception{
        try {
            DeleteResponse deleteResponse = client.delete(
                    new DeleteRequest(index, id).setIfSeqNo(100).setIfPrimaryTerm(2),
                    RequestOptions.DEFAULT);
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.CONFLICT) {

            }
        }
    }


    /**
     * 多条件的查询
     */
    private static void mSearchRequest(){
        MultiSearchRequest request = new MultiSearchRequest();
        SearchRequest firstSearchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("user", "kimchy"));
        firstSearchRequest.source(searchSourceBuilder);
        request.add(firstSearchRequest);
        SearchRequest secondSearchRequest = new SearchRequest();
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("user", "luca"));
        secondSearchRequest.source(searchSourceBuilder);
        request.add(secondSearchRequest);
    }

    /**
     * 多条件的查询模版
     */
    private static void mSearchRequestTemp(String index,RestHighLevelClient client)
    throws Exception {
        String[] searchTerms = {"2"};
        MultiSearchTemplateRequest multiRequest = new MultiSearchTemplateRequest();
        for (String searchTerm : searchTerms) {
            SearchTemplateRequest request = new SearchTemplateRequest();
            request.setRequest(new SearchRequest(index));
            request.setScriptType(ScriptType.INLINE);
            request.setScript(
                    "{" +
                            "  \"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" } }," +
                            "  \"size\" : \"{{size}}\"" +
                            "}");

            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("field", "route_type");
            scriptParams.put("value", searchTerm);
            scriptParams.put("size", 5);
            request.setScriptParams(scriptParams);
            multiRequest.add(request);
            MultiSearchTemplateResponse multiResponse = client.msearchTemplate(multiRequest, RequestOptions.DEFAULT);
            MultiSearchTemplateResponse.Item[] responses = multiResponse.getResponses();
        }
    }

    private static void print(Object obj){
        System.out.println("print => "+obj);
    }
}
