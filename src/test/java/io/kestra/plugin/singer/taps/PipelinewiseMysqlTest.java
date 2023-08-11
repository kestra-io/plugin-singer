package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.singer.models.DiscoverMetadata;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class PipelinewiseMysqlTest extends TapsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        PipelinewiseMysql.PipelinewiseMysqlBuilder<?, ?> builder = PipelinewiseMysql.builder()
            .id(IdUtils.create())
            .type(PipelinewiseMysql.class.getName())
            .host("172.17.0.1")
            .username("root")
            .password("mysql_passwd")
            .port(63306)
            .streamsConfigurations(Arrays.asList(
                StreamsConfiguration.builder()
                    .stream("Category")
                    .replicationMethod(DiscoverMetadata.ReplicationMethod.INCREMENTAL)
                    .replicationKeys("categoryId")
                    .build(),
                StreamsConfiguration.builder()
                    .selected(false)
                    .propertiesPattern(Collections.singletonList("description"))
                    .build()
            ));

        PipelinewiseMysql task = builder.build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        PipelinewiseMysql.Output output = task.run(runContext);

        Map<StreamType, List<Map<String, Object>>> groupedByType = groupedByType(runContext, output.getRaw());

        assertThat(groupedByType.get(StreamType.SCHEMA).size(), is(1));
        assertThat(groupedByType.get(StreamType.SCHEMA).get(0).get("stream"), is("Northwind-Category"));
        assertThat(groupedByType.get(StreamType.STATE).size(), is(4));

        assertThat(groupedByType.get(StreamType.RECORD).size(), is(8));
        assertThat(((Map<String, Object>) groupedByType.get(StreamType.RECORD).get(0).get("record")).get("categoryName"), is("Beverages"));

        // second sync, no result, except tag is bug and will return the last one
        task = builder.build();
        output = task.run(runContext);
        groupedByType = groupedByType(runContext, output.getRaw());
        assertThat(groupedByType.get(StreamType.RECORD).size(), is(1));
    }
}
