package io.kestra.plugin.singer.targets;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.singer.models.DiscoverMetadata;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import io.kestra.plugin.singer.taps.AbstractPythonTap;
import io.kestra.plugin.singer.taps.PipelinewiseMysql;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNot.not;

@MicronautTest
class CsvTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        String stateName = IdUtils.create();

        PipelinewiseMysql.PipelinewiseMysqlBuilder<?, ?> tapBuilder = PipelinewiseMysql.builder()
            .id(IdUtils.create())
            .type(PipelinewiseMysql.class.getName())
            .host("172.17.0.1")
            .username("root")
            .password("mysql_passwd")
            .port(63306)
            .stateName(stateName)
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
            ));

        PipelinewiseMysql tap = tapBuilder.build();

        RunContext runContextTap = TestsUtils.mockRunContext(runContextFactory, tap, ImmutableMap.of());
        AbstractPythonTap.Output tapOutput = tap.run(runContextTap);

        Csv.CsvBuilder<?, ?> builder = Csv
            .builder()
            .id(IdUtils.create())
            .type(DatamillCoPostgres.class.getName())
            .from(tapOutput.getRaw().toString())
            .stateName(stateName)
            .delimiter(";");
        Csv target = builder.build();

        RunContext runContextTarget = TestsUtils.mockRunContext(runContextFactory, target, ImmutableMap.of());
        Csv.Output output = target.run(runContextTarget);

        assertThat(output.getUris().size(), is(2));
        assertThat(output.getUris().keySet(), containsInAnyOrder("Northwind-Region", "Northwind-Category"));

        assertThat(output.getState(), not((nullValue())));
    }
}