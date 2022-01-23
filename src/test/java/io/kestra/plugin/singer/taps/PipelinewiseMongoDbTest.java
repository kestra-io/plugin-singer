package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.singer.models.streams.AbstractStream;
import io.kestra.plugin.singer.models.DiscoverMetadata;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@MicronautTest
class PipelinewiseMongoDbTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        PipelinewiseMongoDb.PipelinewiseMongoDbBuilder<?, ?> builder = PipelinewiseMongoDb.builder()
            .id(IdUtils.create())
            .type(PipelinewiseMongoDb.class.getName())
            .raw(false)
            .host("127.0.0.1")
            .username("root")
            .password("example")
            .port(57017)
            .database("samples")
            .authDatabase("admin")
            .streamsConfigurations(Collections.singletonList(
                StreamsConfiguration.builder()
                    .stream("books")
                    .replicationMethod(DiscoverMetadata.ReplicationMethod.INCREMENTAL)
                    .replicationKeys("_id")
                    .build()
            ));

        PipelinewiseMongoDb task = builder.build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        PipelinewiseMongoDb.Output output = task.run(runContext);

        assertThat(output.getSchemas().size(), is(1));
        assertThat(output.getSchemas().get("books"), is(notNullValue()));
        assertThat(output.getStreams().size(), is(1));
        assertThat(output.getStreams().get("books"), is(notNullValue()));
        assertThat(output.getState(), is(notNullValue()));

        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(output.getStreams().get("books"))));
        List<Object> result = new ArrayList<>();
        FileSerde.reader(inputStream, result::add);

        assertThat(output.getCount().get(AbstractStream.Type.RECORD), is(431L));
        assertThat(result.size(), is(431));
        assertThat(((Map<String, Object>) result.get(0)).size(), is(3));

        // second sync, no result, except tag is bug and will return the last one
        task = builder.build();
        output = task.run(runContext);
        assertThat(output.getCount().get(AbstractStream.Type.RECORD), is(1L));
    }
}
