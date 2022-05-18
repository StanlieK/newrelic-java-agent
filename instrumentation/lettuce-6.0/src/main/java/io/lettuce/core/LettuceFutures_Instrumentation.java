package io.lettuce.core;

import io.lettuce.core.protocol.AsyncCommand;

import java.util.concurrent.TimeUnit;

import com.newrelic.api.agent.DatastoreParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;

@Weave(originalName = "io.lettuce.core.LettuceFutures")
public class LettuceFutures_Instrumentation {

    @SuppressWarnings("rawtypes")
    @Trace
    public static <T> T awaitOrCancel(RedisFuture<T> cmd, long timeout, TimeUnit unit) {
        if (AsyncCommand.class.isInstance(cmd)) {
            AsyncCommand asyncCommand = (AsyncCommand) cmd;
            String type = asyncCommand.getType().name();
            DatastoreParameters params = DatastoreParameters.product("Lettuce").collection("?").operation(type).build();
            NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
        }
        return Weaver.callOriginal();
    }
}
