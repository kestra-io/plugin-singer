package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.singer.models.DiscoverMetadata;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PipelinewiseMongoDbTest extends TapsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        PipelinewiseMongoDb.PipelinewiseMongoDbBuilder<?, ?> builder = PipelinewiseMongoDb.builder()
            .id(IdUtils.create())
            .type(PipelinewiseMongoDb.class.getName())
            .host("172.17.0.1")
            .username("root")
            .password(Property.of("example"))
            .port(Property.of(57017))
            .database(Property.of("samples"))
            .authDatabase(Property.of("admin"))
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

        Map<StreamType, List<Map<String, Object>>> groupedByType = groupedByType(runContext, output.getRaw());

        assertThat(groupedByType.get(StreamType.SCHEMA).size(), is(1));
        assertThat(groupedByType.get(StreamType.SCHEMA).get(0).get("stream"), is("books"));

        assertThat(groupedByType.get(StreamType.RECORD).size(), is(431));
        assertThat(((Map<String, Object>) groupedByType.get(StreamType.RECORD).get(0).get("record")).size(), is(3));

        assertThat(groupedByType.get(StreamType.STATE).size(), is(3));

        // second sync, no result, except tap is bug and will return the last one
        groupedByType = groupedByType(runContext, builder.build().run(runContext).getRaw());
        assertThat(groupedByType.get(StreamType.RECORD).size(), is(1));
    }
}
