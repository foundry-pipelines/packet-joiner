package foundry.join;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** The Foundry section of appsettings.json, as scripts' deploy/render.py renders it for a flink processor. */
record Foundry(String pipeline, String processor, List<Kafka> inputs, Kafka output) {

    /** A Kafka of the manifest: its topic, allowed types and how to connect. */
    record Kafka(String name, String topic, List<String> types, Properties kafka) {}

    static Foundry load(Path path) throws IOException {
        var f = new ObjectMapper().readTree(path.toFile()).get("Foundry");
        var inputs = new ArrayList<Kafka>();
        for (var k : f.has("Inputs") ? f.get("Inputs") : List.of(f.get("Input"))) inputs.add(kafka(k));
        return new Foundry(f.get("Pipeline").asText(), f.get("Processor").asText(), inputs, kafka(f.get("Output")));
    }

    String groupId() {
        return pipeline + "." + processor;
    }

    private static Kafka kafka(JsonNode k) {
        var types = new ArrayList<String>();
        k.path("AllowedTypes").forEach(t -> types.add(t.asText()));
        return new Kafka(k.get("Kafka").asText(), k.get("Topic").asText(), types, connection(k.get("ConnectionSettings")));
    }

    // Credentials come from the environment, Secrets__<SecretRef>__Username/Password, like the .NET processors.
    private static Properties connection(JsonNode settings) {
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
