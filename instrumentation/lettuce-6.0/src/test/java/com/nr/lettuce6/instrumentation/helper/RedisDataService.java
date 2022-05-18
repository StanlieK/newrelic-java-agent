package com.nr.lettuce6.instrumentation.helper;

import com.newrelic.api.agent.Trace;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisStringReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;

import java.util.function.Function;

public class RedisDataService {
        private RedisAsyncCommands<String, String> asyncCommands;
        private RedisCommands<String, String> syncCommands;
        private RedisStringReactiveCommands<String, String> reactiveCommands;

        private final GenericContainer genericContainer;

        public RedisDataService(GenericContainer container) {
            this.genericContainer = container;
        }

        public String syncSet(String key, String value) {
            syncCommands.set(key, value);
            return key;
        }

        public String syncGet(String key) {
            return syncCommands.get(key);
        }

        public RedisFuture<String> asyncGet(String key) {
            return asyncCommands.get(key);
        }

        @Trace(dispatcher = true)
        public Flux<String> reactiveSet(Flux<Data> dataFlux) {
            return dataFlux.map(data -> reactiveCommands.set(data.key, data.value))
                    .flatMap(Function.identity());
        }

        @Trace(dispatcher = true)
        public Flux<String> reactiveGet(Flux<String> keys) {
            return keys.map(key -> reactiveCommands.get(key))
                    .flatMap(Function.identity());
        }

        public void init() {
            setupRedisClient();
        }

        private void setupRedisClient() {
            RedisClient redisClient = RedisClient.create("redis://" + genericContainer.getHost() + ":" + genericContainer.getMappedPort(6379));
            StatefulRedisConnection<String, String> connection = redisClient.connect();
            this.syncCommands = connection.sync();
            this.asyncCommands = connection.async();
            this.reactiveCommands = connection.reactive();
        }

}
