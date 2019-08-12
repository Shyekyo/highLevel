package org.es;

import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.concurrent.TimeUnit;

/**
 * Created by zhangxiaofan on 2019/8/12.
 */
public class SearchApi {
    public static void main(String[] args) throws Exception{
        String index = "posts";
        String id = "1";
        String name="callee_num";
        String value = "61298868000";
        RestHighLevelClient client = DocumentApi.getHighLevelClient();
        //searchBuilder(client,index,name,value);
        //aggbuilder(client,index,name,value);
        //multiBuilder(client,index,name,value);
        //countBuilder(client,index,name,value);
        //infoBuilder(client);
        /**查看client是否ok
         * boolean response = client.ping(RequestOptions.DEFAULT);
         * DocumentApi.print(response);
         */
        DocumentApi.close(client);
    }

    /**
     * 构建SearchSourceBuilder来匹配相关名称和值，size参数最多显示1w行,可打印出命中的数据
     * 利用QueryBuilders来构建查询条件
     * 利用MatchQueryBuilder构建的效果和QueryBuilders一致
     * @param client
     * @param index
     * @param name
     * @param value
     * @throws Exception
     */
    public static void searchBuilder(RestHighLevelClient client,String index,String name,String value)
    throws Exception{
        //SearchRequest searchRequest = new SearchRequest();
        SearchRequest searchRequest = new SearchRequest(index);
       /* SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);*/
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(name, value);
        /**是否模糊匹配
         * matchQueryBuilder.fuzziness(Fuzziness.AUTO);
         */
        /**matchQueryBuilder.prefixLength(3);
         * 个人理解是按照前几位模糊匹配
         */

        /**
         * matchQueryBuilder.maxExpansions(10);
         * When using fuzzy or prefix type query, the number of term expansions to use.
         */
        matchQueryBuilder.maxExpansions(10);
        sourceBuilder.query(QueryBuilders.termQuery(name, value));
        //sourceBuilder.query(matchQueryBuilder);
        sourceBuilder.size(10);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);
        //排序builder
        //sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        sourceBuilder.sort(new FieldSortBuilder("_id").order(SortOrder.DESC));
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        DocumentApi.print(response);
        /*SearchHits hits = response.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit hit : hits) { // MM-rdWwBncJK9AacSI38,M8-rdWwBncJK9AacSI38
            DocumentApi.print(hit.getId());
            String sourceAsString = hit.getSourceAsString();
            DocumentApi.print(sourceAsString);
        }*/
    }
    /**
     * 聚合
     * @param client
     * @param index
     * @param name
     * @param value
     * @throws Exception
     */
    private static void aggbuilder(RestHighLevelClient client,String index,String name,String value)
    throws Exception{
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_user")
                .field("user.keyword");
        //aggregation.subAggregation(AggregationBuilders.sum("average_duration")
        //        .field("duration"));
        searchSourceBuilder.aggregation(aggregation);
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        DocumentApi.print(search);
    }
    /**
     * 多个SearchRequest叠加查询，封装到一个MultiSearchRequest里
     * @param client
     * @param index
     * @param name
     * @param value
     * @throws Exception
     */
    private static void multiBuilder(RestHighLevelClient client,String index,String name,String value)
    	    throws Exception{
    	MultiSearchRequest request = new MultiSearchRequest();    
    	SearchRequest firstSearchRequest = new SearchRequest(index);   
    	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    	searchSourceBuilder.query(QueryBuilders.matchQuery("user", "kimchy"));
    	firstSearchRequest.source(searchSourceBuilder);
    	request.add(firstSearchRequest);                          
    	SearchRequest secondSearchRequest = new SearchRequest(index);  
    	searchSourceBuilder = new SearchSourceBuilder();
    	searchSourceBuilder.query(QueryBuilders.matchQuery("massage", "trying"));
    	secondSearchRequest.source(searchSourceBuilder);
    	request.add(secondSearchRequest);
    	MultiSearchResponse msearch = client.msearch(request, RequestOptions.DEFAULT);
    	Item[] responses = msearch.getResponses();
    	for(Item it : responses){
    		DocumentApi.print(it.getResponse().toString());
    	}
    }
    /**
     * 计数的request，查看搜索的记录count
     * @param client
     * @param index
     * @param name
     * @param value
     * @throws Exception
     */
    private static void countBuilder(RestHighLevelClient client,String index,String name,String value)
    	    throws Exception{
    	CountRequest countRequest = new CountRequest("posts") 
    		    .routing("routing") 
    		    .indicesOptions(IndicesOptions.lenientExpandOpen()) 
    		    .preference("_local");
    	SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); 
    	sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
    	countRequest.source(sourceBuilder);
    	CountResponse countResponse = client
    		    .count(countRequest, RequestOptions.DEFAULT);
    	DocumentApi.print(countResponse.toString());
    }
    /**
     * 查看集群信息
     * @param client
     * @throws Exception
     */
    private static void infoBuilder(RestHighLevelClient client)
    throws Exception{
    	MainResponse response = client.info(RequestOptions.DEFAULT);
    	String clusterName = response.getClusterName();
    	DocumentApi.print(clusterName);
    	String clusterUuid = response.getClusterUuid();
    	DocumentApi.print(clusterUuid);
    	String nodeName = response.getNodeName();
    	DocumentApi.print(nodeName);
    	MainResponse.Version version = response.getVersion();
    	String buildDate = version.getBuildDate();
    	String buildFlavor = version.getBuildFlavor();
    	String buildHash = version.getBuildHash();
    	String buildType = version.getBuildType();
    	String luceneVersion = version.getLuceneVersion();
    	String minimumIndexCompatibilityVersion= version.getMinimumIndexCompatibilityVersion();
    	String minimumWireCompatibilityVersion = version.getMinimumWireCompatibilityVersion();
    	String number = version.getNumber();
    }
    
}
