package io.kestra.plugin.singer.targets;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.singer.models.DiscoverMetadata;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import io.kestra.plugin.singer.taps.AbstractPythonTap;
import io.kestra.plugin.singer.taps.PipelinewiseMysql;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;

@KestraTest
class DatamillCoPostgresTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        String stateName = IdUtils.create();

        PipelinewiseMysql tap = PipelinewiseMysql.builder()
            .id(IdUtils.create())
            .type(PipelinewiseMysql.class.getName())
            .host("172.17.0.1")
            .username("root")
            .password(Property.ofValue("mysql_passwd"))
            .port(Property.ofValue(63306))
            .stateName(Property.ofValue(stateName))
            .streamsConfigurations(Arrays.asList(
                StreamsConfiguration.builder()
                    .stream("Category")
                    .replicationMethod(DiscoverMetadata.ReplicationMethod.INCREMENTAL)
                    .replicationKeys("categoryId")
                    .build(),
                StreamsConfiguration.builder()
                    .stream("Region")
                    .replicationMethod(DiscoverMetadata.ReplicationMethod.FULL_TABLE)
                    .build(),
                StreamsConfiguration.builder()
                    .selected(false)
                    .propertiesPattern(Collections.singletonList("description"))
                    .build()
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, tap, ImmutableMap.of());
        AbstractPythonTap.Output tapOutput = tap.run(runContext);

        assertThat(runContext.metrics().stream().filter(r -> r.getName().equals("singer.record.count") && r.getTags().containsValue("category")).findFirst().get().getValue(), is(8D));
        assertThat(runContext.metrics().stream().filter(r -> r.getName().equals("singer.record.count") && r.getTags().containsValue("region")).findFirst().get().getValue(), is(4D));

        DatamillCoPostgres.DatamillCoPostgresBuilder<?, ?> builder = DatamillCoPostgres
            .builder()
            .id(IdUtils.create())
            .type(io.kestra.plugin.singer.targets.DatamillCoPostgres.class.getName())
            .from(Property.ofValue(tapOutput.getRaw().toString()))
            .stateName(Property.ofValue(stateName))
            .host("172.17.0.1")
            .username("postgres")
            .password(Property.ofValue("pg_passwd"))
            .port(Property.ofValue(65432))
            .dbName(Property.ofValue("sync"));

        DatamillCoPostgres task = builder.build();

        runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        AbstractPythonTarget.Output output = task.run(runContext);

        assertThat(runContext.metrics().stream().filter(r -> r.getName().equals("singer.record.count")).mapToDouble(value -> (Double) value.getValue()).sum(), is(24D));
        assertThat(output.getStateKey(), not((nullValue())));
    }
}
