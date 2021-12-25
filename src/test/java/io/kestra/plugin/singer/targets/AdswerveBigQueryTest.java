package io.kestra.plugin.singer.targets;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.singer.models.DiscoverMetadata;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import io.kestra.plugin.singer.taps.PipelinewiseMysql;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;

@MicronautTest
class AdswerveBigQueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Test
    void run() throws Exception {
        URL resource = AdswerveBigQueryTest.class.getClassLoader().getResource("gcp-service-account.yml");
        String serviceAccount = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile())));

        AdswerveBigQuery.AdswerveBigQueryBuilder<?, ?> builder = AdswerveBigQuery
            .builder()
            .id(IdUtils.create() + "_bq")
            .type(io.kestra.plugin.singer.targets.AdswerveBigQuery.class.getName())
            .tap(PipelinewiseMysql.builder()
                .id(IdUtils.create() + "_mysql")
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
            .serviceAccount(serviceAccount)
            .projectId(project)
            .datasetId(dataset)
            .location("EU");

        AdswerveBigQuery task = builder.build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        AbstractPythonTarget.Output output = task.run(runContext);

        assertThat(runContext.metrics().stream().filter(r -> r.getName().equals("singer.record.count")).findFirst().get().getValue(), is(8D));
        assertThat(output.getState(), not((nullValue())));

        output = task.run(runContext);

        // 8D + 8D + 1D (8D is duplicate as we use same run context, 1D is a bug because mysql query >= 8 (last state with = and not only >)
        assertThat(runContext.metrics().stream().filter(r -> r.getName().equals("singer.record.count")).findFirst().get().getValue(), is(17D));
        assertThat(output.getState(), not((nullValue())));
    }
}
