package software.amazon.awssdk.services.dynamodb;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.MatchType;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.nr.instrumentation.dynamodb_v2.DynamoDBMetricUtil;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;

@Weave(originalName = "software.amazon.awssdk.services.dynamodb.DefaultDynamoDbClient", type = MatchType.ExactClass)
final class DefaultDynamoDbClient_Instrumentation {
    private final SdkClientConfiguration clientConfiguration = Weaver.callOriginal();

    @Trace
    public GetItemResponse getItem(GetItemRequest getItemRequest) {
        URI endpoint = clientConfiguration != null ? clientConfiguration.option(SdkClientOption.ENDPOINT) : null;
        System.out.println("getItem -> sync client endpoint: " + endpoint.toString());
        DynamoDBMetricUtil.metrics(NewRelic.getAgent().getTracedMethod(), "getItem", getItemRequest.tableName(), endpoint);
        return Weaver.callOriginal();
    }

    @Trace
    public PutItemResponse putItem(PutItemRequest putItemRequest) {
        URI endpoint = clientConfiguration != null ? clientConfiguration.option(SdkClientOption.ENDPOINT) : null;
        System.out.println("putItem -> sync client endpoint: " + endpoint.toString());
        DynamoDBMetricUtil.metrics(NewRelic.getAgent().getTracedMethod(), "putItem", putItemRequest.tableName(), endpoint);
        return Weaver.callOriginal();
    }

    @Trace
    public DeleteItemResponse deleteItem(DeleteItemRequest request) {
        URI endpoint = clientConfiguration != null ? clientConfiguration.option(SdkClientOption.ENDPOINT) : null;
        DynamoDBMetricUtil.metrics(NewRelic.getAgent().getTracedMethod(), "deleteItem", request.tableName(), endpoint);
        return Weaver.callOriginal();
    }

    @Trace
    public ListTablesResponse listTables(ListTablesRequest request) {
        URI endpoint = clientConfiguration != null ? clientConfiguration.option(SdkClientOption.ENDPOINT) : null;
        System.out.println("listTables -> sync client endpoint: " + endpoint.toString());
        DynamoDBMetricUtil.metrics(NewRelic.getAgent().getTracedMethod(), "listTables", request.exclusiveStartTableName(), endpoint);
        return Weaver.callOriginal();
    }

    @Trace
    public CreateTableResponse createTable(CreateTableRequest request) {
        URI endpoint = clientConfiguration != null ? clientConfiguration.option(SdkClientOption.ENDPOINT) : null;
        System.out.println("createTable -> sync client endpoint: " + endpoint.toString());
        DynamoDBMetricUtil.metrics(NewRelic.getAgent().getTracedMethod(), "createTable", request.tableName(), endpoint);
        return Weaver.callOriginal();
    }

    @Trace
    public DeleteTableResponse deleteTable(DeleteTableRequest request) {
        URI endpoint = clientConfiguration != null ? clientConfiguration.option(SdkClientOption.ENDPOINT) : null;
        System.out.println("deleteTable -> sync client endpoint: " + endpoint.toString());
        DynamoDBMetricUtil.metrics(NewRelic.getAgent().getTracedMethod(), "deleteTable", request.tableName(), endpoint);
        return Weaver.callOriginal();
    }

    @Trace
    public DescribeTableResponse describeTable(DescribeTableRequest request) {
        URI endpoint = clientConfiguration != null ? clientConfiguration.option(SdkClientOption.ENDPOINT) : null;
        System.out.println("describeTable -> sync client endpoint: " + endpoint.toString());
        DynamoDBMetricUtil.metrics(NewRelic.getAgent().getTracedMethod(), "describeTable", request.tableName(), endpoint);
        return Weaver.callOriginal();
    }

    @Trace
    public ScanResponse scan(ScanRequest request) {
        URI endpoint = clientConfiguration != null ? clientConfiguration.option(SdkClientOption.ENDPOINT) : null;
        System.out.println("scan -> sync client endpoint: " + endpoint.toString());
        DynamoDBMetricUtil.metrics(NewRelic.getAgent().getTracedMethod(), "scan", request.tableName(), endpoint);
        return Weaver.callOriginal();
    }
}