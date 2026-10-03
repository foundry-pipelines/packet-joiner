package foundry.join;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * What Join writes: a packet with its enrichments (decoded payload and ISP). Merge builds it as JSON
 * from the packet's; this record is the source of its registered schema (jsonschema-maven-plugin, pom.xml).
 */
public record EnrichedPackets(
        @JsonProperty(value = "guid", required = true) UUID guid,
        @JsonProperty(value = "data", required = true) String data,
        @JsonProperty("ISP") String isp,
        @JsonProperty(value = "time_sent", required = true) OffsetDateTime timeSent,
        @JsonProperty(value = "host_ip", required = true) String hostIp,
        @JsonProperty(value = "target_ip", required = true) String targetIp) {}
