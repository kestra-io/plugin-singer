package io.kestra.plugin.singer.targets;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.singer.models.DiscoverMetadata;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import io.kestra.plugin.singer.models.streams.AbstractStream;
import io.kestra.plugin.singer.taps.PipelinewiseMysql;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;

@MicronautTest
class DatamillCoPostgresTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        DatamillCoPostgres.DatamillCoPostgresBuilder<?, ?> builder = DatamillCoPostgres
            .builder()
            .id(IdUtils.create())
            .type(io.kestra.plugin.singer.targets.DatamillCoPostgres.class.getName())
            .tap(PipelinewiseMysql.builder()
                .id(IdUtils.create())
                .type(PipelinewiseMysql.class.getName())
                .host("127.0.0.1")
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
                ))
                .build()
            )
            .host("127.0.0.1")
            .username("postgres")
            .password("pg_passwd")
            .port(65432)
            .dbName("sync");

        DatamillCoPostgres task = builder.build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        AbstractPythonTarget.Output output = task.run(runContext);

        assertThat(runContext.metrics().stream().filter(r -> r.getName().equals("singer.record.count")).findFirst().get().getValue(), is(8D));
        assertThat(output.getState(), not((nullValue())));
    }
}
