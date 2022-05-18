package com.nr.lettuce6.instrumentation;

import com.newrelic.agent.bridge.datastore.DatastoreVendor;
import com.newrelic.agent.introspec.DatastoreHelper;
import com.newrelic.agent.introspec.InstrumentationTestConfig;
import com.newrelic.agent.introspec.InstrumentationTestRunner;
import com.newrelic.agent.introspec.Introspector;
import com.newrelic.agent.introspec.MetricsHelper;
import com.nr.lettuce6.instrumentation.helper.Data;
import com.nr.lettuce6.instrumentation.helper.RedisDataService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(InstrumentationTestRunner.class)
@InstrumentationTestConfig(includePrefixes = {"io.lettuce.core", "io.lettuce.core.protocol", "io.netty.channel"})
public class ReactiveLettuce6InstrumentationTest {

    @Rule
    public GenericContainer redis = new GenericContainer(DockerImageName.parse("redis:5.0.3-alpine"))
            .withExposedPorts(6379);
    private RedisDataService redisDataService;

    @Before
    public void before() {
        redisDataService = new RedisDataService(redis);
        redisDataService.init();
    }

    @Test
    public void testSetAndGet() {
        // given some data
        Data data1 = new Data("key1", "value1");
        Data data2 = new Data("key2", "value3");
        Data data3 = new Data("key3", "value3");

        // when reactive 'set' called
        Iterable<String> idsIter = redisDataService.reactiveSet(Flux.just(data1, data2, data3))
                        .toIterable();
        List<String> ids = new ArrayList<>();
        idsIter.forEach(ids::add);

        // then all 'OK'
        assertArrayEquals("All responses should be 'OK'",
                new String[]{"OK", "OK", "OK"}, ids.toArray());

        // when reactive get 'get' called
        Iterable<String> valuesIter = redisDataService
                .reactiveGet(Flux.just("key1", "key2", "key3"))
                .toIterable();
        List<String> values = new ArrayList<>();
        valuesIter.forEach(values::add);

        // then 3 values returned
        assertEquals("Get values size did not math the amount set", 3, values.size());

        // and 2 transactions have been sent
        Introspector introspector = InstrumentationTestRunner.getIntrospector();
        assertEquals(2, introspector.getFinishedTransactionCount(1000));

        // And
        DatastoreHelper helper = new DatastoreHelper(DatastoreVendor.Redis.name());
        //helper.assertAggregateMetrics();

        Collection<String> transactionNames = InstrumentationTestRunner.getIntrospector().getTransactionNames();
        assertEquals(2, transactionNames.size());

        String setTransactionName = "OtherTransaction/Custom/com.nr.lettuce6.instrumentation.helper.RedisDataService/reactiveSet";
        String getTransactionName = "OtherTransaction/Custom/com.nr.lettuce6.instrumentation.helper.RedisDataService/reactiveGet";

        assertTrue("Should contain transaction name for 'set'", transactionNames.contains(setTransactionName));
        assertTrue("Should contain transaction name for 'get'", transactionNames.contains(getTransactionName));

        // and
        //assertEquals(2, introspector.getTransactionEvents(txName).iterator().next().getDatabaseCallCount());
//        helper.assertScopedStatementMetricCount(setTransactionName, "INSERT", "users", 1);
//        helper.assertScopedStatementMetricCount(transactionName, "SELECT", "users", 3);
//        helper.assertScopedStatementMetricCount(transactionName, "UPDATE", "users", 1);
//        helper.assertScopedStatementMetricCount(transactionName, "DELETE", "users", 1);
//        helper.assertAggregateMetrics();
//        helper.assertUnscopedOperationMetricCount("INSERT", 1);
//        helper.assertUnscopedOperationMetricCount("SELECT", 3);
//        helper.assertUnscopedOperationMetricCount("UPDATE", 1);
//        helper.assertUnscopedOperationMetricCount("DELETE", 1);
//        helper.assertUnscopedStatementMetricCount("INSERT", "users", 1);
//        helper.assertUnscopedStatementMetricCount("SELECT", "users", 3);
//        helper.assertUnscopedStatementMetricCount("UPDATE", "users", 1);
//        helper.assertUnscopedStatementMetricCount("DELETE", "users", 1);

    }

}
