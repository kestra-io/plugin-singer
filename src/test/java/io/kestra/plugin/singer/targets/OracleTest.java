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
import io.kestra.plugin.singer.taps.PipelinewiseSqlServer;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;

@KestraTest
class OracleTest {
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

        Oracle target = Oracle.builder()
            .id(IdUtils.create())
            .type(Oracle.class.getName())
            .from(Property.ofValue(tapOutput.getRaw().toString()))
            .host("172.17.0.1")
            .database(Property.ofValue("FREE"))
            .username("system")
            .password("oracle_passwd")
            .port(Property.ofValue(57057))
            .build();

        runContext = TestsUtils.mockRunContext(runContextFactory, target, ImmutableMap.of());
        AbstractPythonTarget.Output output = target.run(runContext);

        assertThat(output.getStateKey(), not((nullValue())));
    }
}