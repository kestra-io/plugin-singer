package io.kestra.plugin.singer.models;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class DiscoverMetadata {
    @With
    boolean selected = true;

    @With
    @JsonProperty("replication-method")
    ReplicationMethod replicationMethod;

    @With
    @JsonProperty("replication-key")
    String replicationKey;

    @JsonProperty("view-key-properties")
    List<String> viewKeyProperties;

    Inclusion inclusion;

    @JsonProperty("selected-by-default")
    boolean selectedByDefault;

    @JsonProperty("valid-replication-keys")
    Object validReplicationKeys;

    @JsonProperty("forced-replication-method")
    ForceReplicationMethod forceReplicationMethod;

    @JsonProperty("table-key-properties")
    List<String> tableKeyProperties;

    @JsonProperty("schema-name")
    String schemaName;

    @JsonProperty("is-view")
    boolean isView;

    @JsonProperty("row-count")
    Long rowCount;

    @JsonProperty("database-name")
    String databaseName;

    @JsonProperty("sql-datatype")
    String sqlDatatype;

    @Getter(AccessLevel.NONE)
    Map<String, Object> extra;

    @JsonAnyGetter
    public Map<String, Object> getExtraFields(){
        return extra;
    }

    @JsonAnySetter
    public void setExtra(String name, Object value) {
        if (extra == null) {
            extra = new HashMap<>();
        }

        extra.put(name, value);
    }

    public enum ReplicationMethod {
        FULL_TABLE,
        INCREMENTAL,
        LOG_BASED
    }

    public enum Inclusion {
        available,
        automatic,
        unsupported
    }

    public enum ForceReplicationMethod {
        FULL_TABLE,
        INCREMENTAL
    }
}
