package io.kestra.plugin.singer.targets;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.AbstractMetricEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.singer.models.DiscoverMetadata;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import io.kestra.plugin.singer.taps.AbstractPythonTap;
import io.kestra.plugin.singer.taps.PipelinewiseSqlServer;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNot.not;

@KestraTest
class SqlServerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var tapBuilder = PipelinewiseSqlServer.builder()
            .id(IdUtils.create())
            .type(PipelinewiseSqlServer.class.getName())
            .host("172.17.0.1")
            .database("msdb")
            .username("SA")
            .password("SQLServer_Passwd")
            .port(Property.ofValue(57037))
            .filterDbs(Property.ofValue(Collections.singletonList("dbo")))
            .stateName(Property.ofValue("before-target-test"))
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

        PipelinewiseSqlServer tap = tapBuilder.build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, tap, ImmutableMap.of());
        AbstractPythonTap.Output tapOutput = tap.run(runContext);

        List<AbstractMetricEntry<?>> recordMetrics = runContext.metrics().stream().filter(r -> r.getName().equals("singer.record.count")).toList();
        assertThat(recordMetrics.size(), is(1));
        assertThat(recordMetrics.get(0).getTags(), allOf(
            aMapWithSize(2),
            hasEntry("database", "dbo"),
            hasEntry("table", "categories")
        ));
        assertThat(recordMetrics.get(0).getValue(), is(8D));

        SqlServer target = SqlServer.builder()
            .id(IdUtils.create())
            .type(PipelinewiseSqlServer.class.getName())
            .from(Property.ofValue(tapOutput.getRaw().toString()))
            .host("172.17.0.1")
            .database("msdb")
            .username("SA")
            .password("SQLServer_Passwd")
            .port(Property.ofValue(57037))
            .defaultTargetSchema(Property.ofValue("target"))
            .build();

        runContext = TestsUtils.mockRunContext(runContextFactory, target, ImmutableMap.of());
        AbstractPythonTarget.Output output = target.run(runContext);

        assertThat(output.getStateKey(), not((nullValue())));

        tap = tapBuilder
            .filterDbs(Property.ofValue(Collections.singletonList("target")))
            .stateName(Property.ofValue("after-target-test"))
            .streamsConfigurations(Collections.singletonList(
                StreamsConfiguration.builder()
                    // SQL Server target transforms table & columns names to snake_case
                    .stream("categories")
                    .replicationMethod(DiscoverMetadata.ReplicationMethod.INCREMENTAL)
                    .replicationKeys("category_id")
                    .build()
            ))
            .build();
        runContext = TestsUtils.mockRunContext(runContextFactory, tap, ImmutableMap.of());
        tap.run(runContext);

        recordMetrics = runContext.metrics().stream().filter(r -> r.getName().equals("singer.record.count")).toList();
        assertThat(recordMetrics.size(), is(1));
        assertThat(recordMetrics.get(0).getTags(), allOf(
            aMapWithSize(2),
            hasEntry("database", "target"),
            hasEntry("table", "categories")
        ));
        assertThat(recordMetrics.get(0).getValue(), is(8D));
    }
}
