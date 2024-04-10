package com.nbb.flink.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

public class MyElasticsearchSink {

    public static ElasticsearchSink<String> newSink(HttpHost esHttpHost) {
        return new Elasticsearch7SinkBuilder<String>()
                .setBulkFlushMaxActions(1)
                .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
                .setEmitter(
                        (element, context, indexer) -> {
                            JSONObject jsonObject = JSON.parseObject(element);
                            String type = jsonObject.getString("type");
                            if (type.equals("read") ||type.equals("insert") || type.equals("update")) {
                                IndexRequest indexRequest = createIndexRequest(jsonObject);
                                indexer.add(indexRequest);
                            } else if (type.equals("delete")) {
                                DeleteRequest deleteRequest = createDeleteRequest(jsonObject);
                                indexer.add(deleteRequest);
                            }
                        })
                .build();
    }


    private static IndexRequest createIndexRequest(JSONObject jsonObject) {
        JSONObject source = jsonObject.getJSONObject("after");
        String indexName = jsonObject.getString("tableName");
        String id = source.getString("id");

        return Requests.indexRequest()
                .index(indexName)
                .id(id)
                .source(source);
    }

    private static DeleteRequest createDeleteRequest(JSONObject jsonObject) {
        JSONObject source = jsonObject.getJSONObject("before");
        String indexName = jsonObject.getString("tableName");
        String id = source.getString("id");

        return Requests.deleteRequest(indexName)
                .id(id);
    }
}
