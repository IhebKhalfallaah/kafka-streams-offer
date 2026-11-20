package app.streams;

import app.config.KafkaConfig;
import com.google.gson.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.util.Map;
import java.util.Properties;

public class StreamsApp {
    static final Gson G = new Gson();

    public  static class SerdeJson implements Serde<Map<String,Object>> {


        public SerdeJson() {
            // required public no-arg constructor
        }

        private final Serializer<Map<String, Object>> ser =
                (topic, data) -> G.toJson(data).getBytes();


        final Deserializer<Map<String,Object>> de = (topic, bytes) -> G.fromJson(new String(bytes), Map.class);
        public Serializer<Map<String,Object>> serializer() { return ser; }
        public Deserializer<Map<String,Object>> deserializer() { return de; }
        public void configure(Map<String, ?> configs, boolean isKey) {}
        public void close() {}
    }

    public static void main(String[] args) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "offers-streams");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers("localhost:9092,localhost:9093"));
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
        p.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SerdeJson.class);
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);

        StreamsBuilder b = new StreamsBuilder();

        // KTable of latest market price per SKU
        KTable<String, Map<String,Object>> prices = b.table("price.ticker");

        // Offers stream -> rekey to SKU for join
        KStream<String, Map<String,Object>> offers =
                b.stream("offer.inbound", Consumed.with(Serdes.String(), new SerdeJson()))
                        .selectKey((k,v) -> (String)v.get("sku"));

        // Simple static policy (minProfit=3, feePct/shipOut used if missing)
        final double defaultMinProfit = 3.0;

        // Join with price table and score
        KStream<String, Map<String,Object>> scored = offers.leftJoin(prices, (offer, price) -> {
            double M = price != null && price.get("marketPrice") != null
                    ? ((Number)price.get("marketPrice")).doubleValue() : Double.NaN;
            String side = ((String)offer.get("side"));
            double P = ((Number)offer.get("price")).doubleValue();
            double qty = ((Number)offer.get("qty")).doubleValue();
            double feesPct = offer.get("feesPct")==null? 0.04 : ((Number)offer.get("feesPct")).doubleValue();
            double ship = offer.get("shipping")==null? 3.0 : ((Number)offer.get("shipping")).doubleValue();

            double profitUnit;
            String reason = "OK";
            if (Double.isNaN(M)) {
                profitUnit = Double.NEGATIVE_INFINITY;
                reason = "NO_PRICE";
            } else if ("BUY".equals(side)) { // they buy from us
                double C = M; // using market as proxy cost
                profitUnit = P - (C + feesPct*P + ship);
            } else { // SELL: we buy then sell near M
                profitUnit = (M - feesPct*M - 3.0) - (P + 2.0); // shipOut=3, shipIn=2 (simplified)
            }
            double profit = Math.round(profitUnit * qty * 100.0)/100.0;

            offer.put("marketPrice", M);
            offer.put("profit", profit);
            offer.put("reason", reason);
            return offer;
        });

        // Side output
        scored.to("offer.scored", Produced.with(Serdes.String(), new SerdeJson()));

        KStream<String, Map<String,Object>>[] branches = scored.branch(
                (k,v) -> ((Number)v.get("profit")).doubleValue() >= defaultMinProfit,
                (k,v) -> true
        );

        KStream<String, Map<String,Object>> accepted = branches[0];
        KStream<String, Map<String,Object>> rejected = branches[1];

        accepted.to("offer.accepted", Produced.with(Serdes.String(), new SerdeJson()));
        rejected.to("offer.rejected", Produced.with(Serdes.String(), new SerdeJson()));

        // Best-offer per SKU (max profit) -> order.created
        KTable<String, Map<String,Object>> bestPerSku = accepted
                .groupByKey()
                .reduce((a, other) -> ((Number) a.get("profit")).doubleValue() >= ((Number) other.get("profit")).doubleValue()
                        ? a
                        : other);

        bestPerSku
                .toStream()
                .mapValues(v -> {
                    String offerId = (String) v.get("offerId");
                    String sku = (String) v.get("sku");
                    String side = (String) v.get("side");
                    double price = ((Number) v.get("price")).doubleValue();
                    double profit = ((Number) v.get("profit")).doubleValue();
                    return Map.of(
                            "orderId", "ord_" + offerId,
                            "sku", sku,
                            "offerId", offerId,
                            "side", side,
                            "unitPrice", price,
                            "profit", profit,
                            "createdAt", System.currentTimeMillis()
                    );
                })
                .to("order.created");  // default serdes used here


        KafkaStreams streams = new KafkaStreams(b.build(), p);

        // Replace a crashed stream thread instead of shutting down the whole app
        streams.setUncaughtExceptionHandler(
                e -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD
        );

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
