package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.singer.models.DiscoverMetadata;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PipelinewisePostgresTest extends TapsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        PipelinewisePostgres.PipelinewisePostgresBuilder<?, ?> builder = PipelinewisePostgres.builder()
            .id(IdUtils.create())
            .type(PipelinewisePostgres.class.getName())
            .host("172.17.0.1")
            .username("postgres")
            .password("pg_passwd")
            .port(65432)
            .dbName("postgres")
            .streamsConfigurations(Arrays.asList(
                StreamsConfiguration.builder()
                    .stream("category")
                    .replicationMethod(DiscoverMetadata.ReplicationMethod.INCREMENTAL)
                    .replicationKeys("categoryid")
                    .build(),
                StreamsConfiguration.builder()
                    .selected(false)
                    .propertiesPattern(Collections.singletonList("description"))
                    .build()
            ));

        PipelinewisePostgres task = builder.build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        PipelinewisePostgres.Output output = task.run(runContext);

        Map<StreamType, List<Map<String, Object>>> groupedByType = groupedByType(runContext, output.getRaw());

        assertThat(groupedByType.get(StreamType.SCHEMA).size(), is(1));
        assertThat(groupedByType.get(StreamType.SCHEMA).get(0).get("stream"), is("public-category"));

        assertThat(groupedByType.get(StreamType.RECORD).size(), is(8));
        Map<String, Object> firstRecord = (Map<String, Object>) groupedByType.get(StreamType.RECORD).get(0).get("record");
        assertThat(firstRecord.size(), is(3));
        assertThat(firstRecord.containsKey("description"), is(true));

        assertThat(groupedByType.get(StreamType.STATE).size(), is(2));

        // second sync, no result, except tap is bug and will return the last one
        groupedByType = groupedByType(runContext, builder.build().run(runContext).getRaw());
        assertThat(groupedByType.get(StreamType.RECORD).size(), is(1));
    }
}
