package io.kestra.plugin.singer.targets;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.singer.models.DiscoverMetadata;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import io.kestra.plugin.singer.taps.PipelinewiseMysql;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNot.not;

@MicronautTest
class CsvTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        Csv.CsvBuilder<?, ?> builder = Csv
            .builder()
            .id(IdUtils.create())
            .type(DatamillCoPostgres.class.getName())
            .workingDirectory(Path.of("/tmp/singer-tap-csv"))
            .tap(PipelinewiseMysql.builder()
                .id(IdUtils.create())
                .type(PipelinewiseMysql.class.getName())
                .workingDirectory(Path.of("/tmp/singer-tap-mysql"))
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
                        .stream("Region")
                        .replicationMethod(DiscoverMetadata.ReplicationMethod.FULL_TABLE)
                        .build(),
                    StreamsConfiguration.builder()
                        .selected(false)
                        .propertiesPattern(Collections.singletonList("description"))
                        .build()
                ))
                .build()
            )
            .delimiter(";");

        Csv task = builder.build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        Csv.Output output = task.run(runContext);

        assertThat(output.getUris().size(), is(2));
        assertThat(output.getUris().keySet(), containsInAnyOrder("Northwind-Region", "Northwind-Category"));

        assertThat(runContext.metrics().stream().filter(r -> r.getName().equals("singer.record.count") && r.getTags().containsValue("category")).findFirst().get().getValue(), is(8D));
        assertThat(runContext.metrics().stream().filter(r -> r.getName().equals("singer.record.count") && r.getTags().containsValue("region")).findFirst().get().getValue(), is(4D));
        assertThat(output.getState(), not((nullValue())));
    }
}
