package com.zing.es.sample.sample;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author zing
 * Date 2019-01-31
 */
@Slf4j
@Service
public class SampleService {

    /**
     * 名称与Config中Bean的名称一致
     */
    @Autowired
    RestHighLevelClient highLevelClient;


    /**
     * 使用方式
     *
     * @return test is passed
     */
    public boolean testEsRestClient() {
        SearchRequest searchRequest = new SearchRequest("gdp_tops*");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("city", "北京市"));
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            Arrays.stream(response.getHits().getHits())
                    .forEach(i -> {
                        log.info(i.getIndex());
                        log.info(i.getSourceAsString());
                        log.info(i.getType());
                    });
            log.info("total:{}", response.getHits().totalHits);
            return true;
        } catch (IOException e) {
            log.error("test failed", e);
            return false;
        }
    }


}
