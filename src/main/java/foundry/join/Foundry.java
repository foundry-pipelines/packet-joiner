package foundry.join;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** The Foundry section of appsettings.json, as manifest.py renders it for a flink processor. */
record Foundry(String pipeline, String processor, List<DataSet> inputs, DataSet output) {

    /** A dataset: its topic, schemas and how to connect. */
    record DataSet(String name, String topic, List<String> schemas, Properties kafka) {}

    static Foundry load(Path path) throws IOException {
        var f = new ObjectMapper().readTree(path.toFile()).get("Foundry");
        var inputs = new ArrayList<DataSet>();
        for (var ds : f.has("Inputs") ? f.get("Inputs") : List.of(f.get("Input"))) inputs.add(dataSet(ds));
        return new Foundry(f.get("Pipeline").asText(), f.get("Processor").asText(), inputs, dataSet(f.get("Output")));
    }

    String groupId() {
        return pipeline + "." + processor;
    }

    private static DataSet dataSet(JsonNode ds) {
        var schemas = new ArrayList<String>();
        if (ds.has("DataSchemas")) ds.get("DataSchemas").forEach(s -> schemas.add(s.asText()));
        else if (ds.hasNonNull("DataSchema") && !ds.get("DataSchema").asText().isEmpty()) schemas.add(ds.get("DataSchema").asText());
        return new DataSet(ds.get("DataSet").asText(), ds.get("Topic").asText(), schemas, kafka(ds.get("ConnectionSettings")));
    }

    // Credentials come from the environment, Secrets__<SecretRef>__Username/Password, like the .NET processors.
    private static Properties kafka(JsonNode settings) {
        var props = new Properties();
        props.setProperty("bootstrap.servers", settings.get("Brokers").asText());
        if (settings.hasNonNull("SecurityProtocol")) props.setProperty("security.protocol", settings.get("SecurityProtocol").asText());
        if (settings.hasNonNull("SaslMechanism")) props.setProperty("sasl.mechanism", settings.get("SaslMechanism").asText());
        if (settings.hasNonNull("SecretRef")) {
            var ref = settings.get("SecretRef").asText();
            props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
                    + System.getenv("Secrets__" + ref + "__Username") + "\" password=\"" + System.getenv("Secrets__" + ref + "__Password") + "\";");
        }
        return props;
    }
}
