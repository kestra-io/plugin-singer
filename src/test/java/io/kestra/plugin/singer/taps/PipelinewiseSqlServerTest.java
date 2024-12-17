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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PipelinewiseSqlServerTest extends TapsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        var builder = PipelinewiseSqlServer.builder()
            .id(IdUtils.create())
            .type(PipelinewiseSqlServer.class.getName())
            .host("172.17.0.1")
            .database("msdb")
            .username("SA")
            .password("SQLServer_Passwd")
            .port(57037)
            .filterDbs(Collections.singletonList("dbo"))
            .stateName(Property.of("tap-test"))
            .streamsConfigurations(Arrays.asList(
                StreamsConfiguration.builder()
                    .stream("Categories")
                    .replicationMethod(DiscoverMetadata.ReplicationMethod.INCREMENTAL)
                    .replicationKeys("CategoryID")
                    .build(),
                StreamsConfiguration.builder()
                    .selected(false)
                    .propertiesPattern(Collections.singletonList("Description"))
                    .build()
            ));

        PipelinewiseSqlServer task = builder.build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        PipelinewiseSqlServer.Output output = task.run(runContext);

        Map<StreamType, List<Map<String, Object>>> groupedByType = groupedByType(runContext, output.getRaw());

        assertThat(groupedByType.get(StreamType.SCHEMA).size(), is(1));
        assertThat(groupedByType.get(StreamType.SCHEMA).get(0).get("stream"), is("dbo-Categories"));
        assertThat(groupedByType.get(StreamType.STATE).size(), is(4));

        assertThat(groupedByType.get(StreamType.RECORD).size(), is(8));
        assertThat(((Map<String, Object>) groupedByType.get(StreamType.RECORD).get(0).get("record")).get("CategoryName"), is("Beverages"));

        // second sync, no result, except tag is bug and will return the last one
        task = builder.build();
        output = task.run(runContext);
        groupedByType = groupedByType(runContext, output.getRaw());
        assertThat(groupedByType.get(StreamType.RECORD).size(), is(1));
    }
}
