package app.offergenerator;


import app.config.KafkaConfig;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import java.util.UUID;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class OffersGenerator {

    private static final Gson G = new Gson();

    private static final List<String> SKUS =
            List.of("BAT-AGM-096", "ALT-FOCUS-1.0", "GBX-MT-6SPD", "TURBO-N47");

    // latest market price per SKU, filled by consuming price.ticker
    private static final Map<String, Map<String, Object>> latestPrices =
            new ConcurrentHashMap<>();

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }

    private static String randomChoice(List<String> list, Random rnd) {
        return list.get(rnd.nextInt(list.size()));
    }


    private static String randomId(String prefix) {
        String hex = UUID.randomUUID().toString().replace("-", "");
        // hex length = 32, so this is always safe
        return prefix + "_" + hex.substring(0, 8);
    }

    private static Map<String, Object> genOffer(Random rnd) {
        String sku = randomChoice(SKUS, rnd);
        Map<String, Object> latest = latestPrices.get(sku);

        double marketPrice;
        if (latest != null && latest.get("marketPrice") instanceof Number n) {
            marketPrice = n.doubleValue();
        } else {
            marketPrice = 40 + rnd.nextDouble() * 80;
        }

        String side = randomChoice(List.of("BUY", "SELL"), rnd);

        double price;
        if ("BUY".equals(side)) {
            // they buy from you; around market or slightly higher
            price = marketPrice * (0.9 + rnd.nextDouble() * 0.25);  // 0.9 .. 1.15
        } else {
            // they sell to you; cheaper than market
            price = marketPrice * (0.7 + rnd.nextDouble() * 0.28);  // 0.7 .. 0.98
        }

        int qty = 1 + rnd.nextInt(5);
        double[] feeChoices = {0.03, 0.04, 0.05};
        double feesPct = feeChoices[rnd.nextInt(feeChoices.length)];

        double[] shipChoices = {2.0, 3.0, 3.5, 4.0};
        double shipping = shipChoices[rnd.nextInt(shipChoices.length)];

        Map<String, Object> offer = new HashMap<>();
        offer.put("offerId", randomId("of"));
        offer.put("sku", sku);
        offer.put("side", side);
        offer.put("price", Double.valueOf(String.format(Locale.US, "%.2f", price)));
        offer.put("qty", qty);
        offer.put("feesPct", Double.valueOf(String.format(Locale.US, "%.3f", feesPct)));
        offer.put("shipping", Double.valueOf(String.format(Locale.US, "%.1f", shipping)));
        offer.put("ts", System.currentTimeMillis());
        return offer;
    }

    private static void startPriceConsumer() {
        new Thread(() -> {
            Properties props = KafkaConfig.baseConsumerProps("offers-price-reader", "offers-price-reader");
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(List.of("price.ticker"));
                System.out.println("Offers generator is listening to price.ticker");

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> rec : records) {
                        try {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> value =
                                    G.fromJson(rec.value(), Map.class);
                            if (value != null &&
                                    value.get("sku") instanceof String sku &&
                                    value.get("marketPrice") instanceof Number) {
                                latestPrices.put(sku, value);
                            }
                        } catch (Exception e) {
                            System.err.println("Error parsing price message: " + e.getMessage());
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Price consumer fatal error: " + e.getMessage());
                e.printStackTrace();
            }
        }, "price-ticker-consumer").start();
    }

    public static void main(String[] args) {
        // start consuming prices in background
        startPriceConsumer();

        // small delay to collect some prices first
        sleep(1000);

        Properties props = KafkaConfig.baseProducerProps("offers-generator");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Offers generator connected to Kafka for producing");
            Random rnd = new Random();

            while (true) {
                int burstSize = 1 + rnd.nextInt(3); // 1..3
                List<ProducerRecord<String, String>> batch = new ArrayList<>();

                for (int i = 0; i < burstSize; i++) {
                    Map<String, Object> offer = genOffer(rnd);
                    String key = (String) offer.get("offerId");
                    String json = G.toJson(offer);
                    batch.add(new ProducerRecord<>("offer.inbound", key, json));
                }

                for (ProducerRecord<String, String> rec : batch) {
                    producer.send(rec, (metadata, ex) -> {
                        if (ex != null) {
                            System.err.println("Error sending offer: " + ex.getMessage());
                        }
                    });
                }
                producer.flush();

                System.out.println("Sent offers: " + batch.stream()
                        .map(ProducerRecord::value)
                        .reduce((a,b) -> a + " | " + b)
                        .orElse(""));

                sleep(200 + (long)(rnd.nextDouble() * 800));
            }
        } catch (Exception e) {
            System.err.println("Offers generator fatal error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
