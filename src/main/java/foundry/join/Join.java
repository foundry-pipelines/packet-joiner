package foundry.join;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Joins each packet with its enrichments into an EnrichedPackets record. Packets and enrichments are
 * keyed by guid and collected in a session window per guid, closed once nothing new has arrived for
 * {@link #GAP}; whatever enrichments arrived by then go into the record. Windows are Flink state, so
 * they survive restarts and deploys.
 */
public final class Join {
    static final String TYPE_HEADER = "foundry-type"; // stamped by every processor on what it writes
    static final String PACKET = "Packets", DECODE = "DecodeEnrichment", ISP = "IspEnrichment", OUT = "EnrichedPackets";
    static final Duration GAP = Duration.ofSeconds(10);
    static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        var foundry = Foundry.load(Path.of(System.getenv().getOrDefault("FOUNDRY_SETTINGS", "/opt/flink/foundry/appsettings.json")));
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Piece> pieces = null;
        for (var in : foundry.inputs()) {
            var source = KafkaSource.<Piece>builder()
                    .setProperties(in.kafka())
                    .setTopics(in.topic())
                    .setGroupId(foundry.groupId())
                    .setStartingOffsets(OffsetsInitializer.latest()) // restored jobs resume from their checkpoint instead
                    .setDeserializer(new Pieces(in.types().size() == 1 ? in.types().get(0) : null))
                    .build();
            var stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), in.name());
            pieces = pieces == null ? stream : pieces.union(stream);
        }

        var out = foundry.output();
        var sink = KafkaSink.<Piece>builder()
                .setKafkaProducerConfig(out.kafka())
                .setRecordSerializer(new Records(out.topic()))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        pieces.keyBy(p -> p.guid)
                .window(ProcessingTimeSessionWindows.withGap(GAP))
                .process(new Merge())
                .name("Join")
                .sinkTo(sink);
        env.execute(foundry.groupId());
    }

    /** Kafka record -> Piece: the type from its header (or the topic's only allowed type), the guid from its JSON. */
    static final class Pieces implements KafkaRecordDeserializationSchema<Piece> {
        private final String fallback;

        Pieces(String fallback) {
            this.fallback = fallback;
        }

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Piece> out) {
            var header = record.headers().lastHeader(TYPE_HEADER);
            var type = header != null ? new String(header.value(), StandardCharsets.UTF_8) : fallback;
            try {
                var guid = JSON.readTree(record.value()).path("guid").asText("");
                if (type != null && !guid.isEmpty()) out.collect(new Piece(guid, type, new String(record.value(), StandardCharsets.UTF_8)));
            } catch (java.io.IOException e) {
                // Not JSON: skip it, like the .NET processors skip bad messages.
            }
        }

        @Override
        public TypeInformation<Piece> getProducedType() {
            return TypeInformation.of(Piece.class);
        }
    }

    /** A guid's packet and enrichments -> one EnrichedPackets record; enrichments without their packet are dropped. */
    static final class Merge extends ProcessWindowFunction<Piece, Piece, String, TimeWindow> {
        @Override
        public void process(String guid, Context context, Iterable<Piece> pieces, Collector<Piece> out) throws Exception {
            ObjectNode packet = null, decode = null, isp = null;
            for (var p : pieces) {
                var node = (ObjectNode) JSON.readTree(p.json);
                switch (p.type) {
                    case PACKET -> packet = node;
                    case DECODE -> decode = node;
                    case ISP -> isp = node;
                    default -> {}
                }
            }
            if (packet == null) return;
            var enriched = packet.deepCopy();
            if (decode != null) enriched.set("data", decode.get("data"));
            enriched.set("ISP", isp != null ? isp.get("ISP") : null);
            out.collect(new Piece(guid, OUT, JSON.writeValueAsString(enriched)));
        }
    }

    /** Piece -> Kafka record keyed by guid, with the type header. */
    record Records(String topic) implements KafkaRecordSerializationSchema<Piece> {
        @Override
        public ProducerRecord<byte[], byte[]> serialize(Piece p, KafkaSinkContext context, Long timestamp) {
            var headers = new RecordHeaders().add(TYPE_HEADER, p.type.getBytes(StandardCharsets.UTF_8));
            return new ProducerRecord<>(topic, null, null, p.guid.getBytes(StandardCharsets.UTF_8), p.json.getBytes(StandardCharsets.UTF_8), headers);
        }
    }
}
