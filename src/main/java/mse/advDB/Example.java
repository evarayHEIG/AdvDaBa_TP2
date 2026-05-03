package mse.advDB;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonReaderFactory;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import org.neo4j.driver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.neo4j.driver.Values.parameters;

public class Example {

    private static final Logger logger = LoggerFactory.getLogger(Example.class);

    // Shared factory – avoids reloading the ServiceLoader on every parsed line (OOM fix)
    private static final JsonReaderFactory JSON_READER_FACTORY = Json.createReaderFactory(Collections.emptyMap());

    // Sentinel to signal end of producer stream
    private static final List<Map<String, Object>> END_OF_STREAM = Collections.emptyList();

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws InterruptedException {
        String jsonPath = System.getenv("JSON_FILE");
        int nbArticles  = Math.max(1000, Integer.parseInt(System.getenv().getOrDefault("MAX_NODES",  "10000000")));
        int batchSize   = Math.max(1,    Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "10000")));
        String neo4jIP  = System.getenv("NEO4J_IP");

        logger.info("JSON file   : {}", jsonPath);
        logger.info("Max articles: {}", nbArticles);
        logger.info("Batch size  : {}", batchSize);
        logger.info("Neo4j IP    : {}", neo4jIP);

        Driver driver = GraphDatabase.driver(
                "bolt://" + neo4jIP + ":7687",
                AuthTokens.basic("neo4j", "test"));

        waitForDatabase(driver);
        createConstraints(driver);

        Instant startTime = Instant.now();
        logger.info("Loading started at: {}", startTime);

        runPass1(driver, jsonPath, nbArticles, batchSize);
        runPass2(driver, jsonPath, nbArticles, batchSize);

        logFinalStats(driver, startTime);
        driver.close();
    }

    // -------------------------------------------------------------------------
    // Database setup
    // -------------------------------------------------------------------------

    private static void waitForDatabase(Driver driver) {
        boolean connected = false;
        while (!connected) {
            try {
                logger.info("Waiting for Neo4j to be ready...");
                Thread.sleep(5000);
                driver.verifyConnectivity();
                connected = true;
            } catch (Exception ignored) {
            }
        }
        logger.info("Neo4j is ready.");
    }

    private static void createConstraints(Driver driver) {
        try (Session session = driver.session()) {
            session.writeTransaction(tx -> {
                tx.run("CREATE CONSTRAINT constraint_article IF NOT EXISTS " +
                       "FOR (a:Article) REQUIRE a._id IS UNIQUE");
                tx.run("CREATE CONSTRAINT constraint_author IF NOT EXISTS " +
                       "FOR (a:Author) REQUIRE a._id IS UNIQUE");
                return null;
            });
            logger.info("Constraints created.");
        }
    }

    // -------------------------------------------------------------------------
    // Pass 1 – Articles, Authors, AUTHORED relations
    // -------------------------------------------------------------------------

    private static void runPass1(Driver driver, String jsonPath, int nbArticles, int batchSize)
            throws InterruptedException {
        logger.info("=== PASS 1: Creating Articles, Authors and AUTHORED relations ===");
        BlockingQueue<List<Map<String, Object>>> queue = new LinkedBlockingQueue<>(4);

        Thread producer = new Thread(() -> {
            try (BufferedReader br = openReader(jsonPath)) {
                String line;
                int count = 0;
                List<Map<String, Object>> batch = new ArrayList<>(batchSize);

                while ((line = br.readLine()) != null && count < nbArticles) {
                    JsonObject obj = parseLine(line);
                    if (obj == null) continue;

                    String articleId = obj.getString("id", null);
                    if (articleId == null || articleId.isEmpty()) continue;

                    String title = obj.getString("title", "");
                    if (title.length() > 500) title = title.substring(0, 500);

                    Map<String, Object> row = new HashMap<>();
                    row.put("id",      articleId);
                    row.put("title",   title);
                    row.put("authors", extractAuthors(articleId, obj.getJsonArray("authors")));
                    batch.add(row);

                    if (++count % batchSize == 0)
                        logger.info("[PASS 1 Producer] Read {} articles", count);

                    if (batch.size() >= batchSize) {
                        queue.put(new ArrayList<>(batch));
                        batch.clear();
                    }
                }

                if (!batch.isEmpty()) queue.put(batch);
                queue.put(END_OF_STREAM);
                logger.info("[PASS 1 Producer] Done – {} articles read", count);

            } catch (Exception e) {
                logger.error("[PASS 1 Producer] Error: {}", e.getMessage(), e);
            }
        });

        Thread consumer = new Thread(() -> {
            try (Session session = driver.session()) {
                int batches = 0, total = 0;
                while (true) {
                    List<Map<String, Object>> batch = queue.take();
                    if (batch == END_OF_STREAM) break;
                    flushBatchPass1(session, batch);
                    total += batch.size();
                    logger.info("[PASS 1 Consumer] Committed batch {} ({} articles total)", ++batches, total);
                }
                logger.info("[PASS 1 Consumer] Finished – {} articles committed", total);
            } catch (Exception e) {
                logger.error("[PASS 1 Consumer] Error: {}", e.getMessage(), e);
            }
        });

        runAndJoin(producer, consumer);
        logger.info("PASS 1 complete.");
    }

    private static void flushBatchPass1(Session session, List<Map<String, Object>> batch) {
        if (batch.isEmpty()) return;
        session.writeTransaction(tx -> {
            tx.run(
                "UNWIND $rows AS row " +
                "MERGE (a:Article {_id: row.id}) " +
                "ON CREATE SET a.title = row.title " +
                "ON MATCH SET a.title = row.title " +
                "WITH a, row " +
                "CALL { " +
                "  WITH a, row " +
                "  UNWIND coalesce(row.authors, []) AS author " +
                "  MERGE (au:Author {_id: author.id}) " +
                "  ON CREATE SET au.name = author.name, au.org = author.org " +
                "  WITH a, au " +
                "  MERGE (au)-[:AUTHORED]->(a) " +
                "  RETURN count(*) AS authorWrites " +
                "} " +
                "RETURN count(*) AS processed",
                parameters("rows", batch));
            return null;
        });
    }

    // -------------------------------------------------------------------------
    // Pass 2 – CITES relations
    // -------------------------------------------------------------------------

    private static void runPass2(Driver driver, String jsonPath, int nbArticles, int batchSize)
            throws InterruptedException {
        logger.info("=== PASS 2: Creating CITES relations ===");
        BlockingQueue<List<Map<String, Object>>> queue = new LinkedBlockingQueue<>(4);

        Thread producer = new Thread(() -> {
            try (BufferedReader br = openReader(jsonPath)) {
                String line;
                int count = 0;
                List<Map<String, Object>> batch = new ArrayList<>(batchSize);

                while ((line = br.readLine()) != null && count < nbArticles) {
                    JsonObject obj = parseLine(line);
                    if (obj == null) continue;

                    String articleId = obj.getString("id", null);
                    if (articleId == null || articleId.isEmpty()) continue;

                    Map<String, Object> row = new HashMap<>();
                    row.put("id", articleId);
                    row.put("references", extractReferences(obj.getJsonArray("references")));
                    batch.add(row);

                    if (++count % batchSize == 0)
                        logger.info("[PASS 2 Producer] Read {} articles", count);

                    if (batch.size() >= batchSize) {
                        queue.put(new ArrayList<>(batch));
                        batch.clear();
                    }
                }

                if (!batch.isEmpty()) queue.put(batch);
                queue.put(END_OF_STREAM);
                logger.info("[PASS 2 Producer] Done – {} articles read", count);

            } catch (Exception e) {
                logger.error("[PASS 2 Producer] Error: {}", e.getMessage(), e);
            }
        });

        Thread consumer = new Thread(() -> {
            try (Session session = driver.session()) {
                int batches = 0, total = 0;
                while (true) {
                    List<Map<String, Object>> batch = queue.take();
                    if (batch == END_OF_STREAM) break;
                    flushBatchPass2(session, batch);
                    total += batch.size();
                    logger.info("[PASS 2 Consumer] Committed batch {} ({} articles total)", ++batches, total);
                }
                logger.info("[PASS 2 Consumer] Finished – {} articles processed", total);
            } catch (Exception e) {
                logger.error("[PASS 2 Consumer] Error: {}", e.getMessage(), e);
            }
        });

        runAndJoin(producer, consumer);
        logger.info("PASS 2 complete.");
    }

    private static void flushBatchPass2(Session session, List<Map<String, Object>> batch) {
        if (batch.isEmpty()) return;
        session.writeTransaction(tx -> {
            tx.run(
                "UNWIND $rows AS row " +
                "MATCH (a:Article {_id: row.id}) " +
                "UNWIND row.references AS refId " +
                "MATCH (t:Article {_id: refId}) " +
                "CREATE (a)-[:CITES]->(t)",
                parameters("rows", batch));
            return null;
        });
    }

    // -------------------------------------------------------------------------
    // Stats
    // -------------------------------------------------------------------------

    private static void logFinalStats(Driver driver, Instant startTime) {
        Instant endTime = Instant.now();
        try (Session session = driver.session()) {
            long articles  = countNodes(session, "Article");
            long authors   = countNodes(session, "Author");
            long citations = session.readTransaction(tx ->
                    tx.run("MATCH ()-[:CITES]->() RETURN count(*) AS c")
                      .single().get("c").asLong());

            logger.info("=== Loading complete ===");
            logger.info("Started at        : {}", startTime);
            logger.info("Ended at          : {}", endTime);
            logger.info("Duration          : {} s", endTime.getEpochSecond() - startTime.getEpochSecond());
            logger.info("Articles loaded   : {}", articles);
            logger.info("Authors loaded    : {}", authors);
            logger.info("Total nodes       : {}", articles + authors);
            logger.info("Citation relations: {}", citations);
        }
    }

    private static long countNodes(Session session, String label) {
        return session.readTransaction(tx ->
                tx.run("MATCH (n:" + label + ") RETURN count(n) AS c")
                  .single().get("c").asLong());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Opens a BufferedReader from a local path or HTTP(S) URL. */
    private static BufferedReader openReader(String path) throws IOException {
        InputStream is = path.startsWith("http://") || path.startsWith("https://")
                ? new URL(path).openStream()
                : new FileInputStream(path);
        return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    }

    /** Parses a JSON line; returns null and logs a warning on failure. */
    private static JsonObject parseLine(String line) {
        if (line == null || line.isBlank()) return null;
        try (JsonReader reader = JSON_READER_FACTORY.createReader(new StringReader(line))) {
            return reader.readObject();
        } catch (Exception e) {
            logger.warn("Skipping malformed JSON line: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Extracts author maps {id, name, org} from a JSON array.
     * Returns an empty list if null.
     */
    private static List<Map<String, Object>> extractAuthors(String articleId, JsonArray authors) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (authors == null) return result;

        for (JsonValue av : authors) {
            JsonObject a = (JsonObject) av;
            String id   = a.getString("id", null);
            String name = a.getString("name", "");
            String org  = a.getString("org", "");
            if (id == null || id.isEmpty()) id = generateAuthorId(name, org);

            Map<String, Object> row = new HashMap<>();
            row.put("id",   id);
            row.put("name", name);
            row.put("org",  org);
            result.add(row);
        }
        return result;
    }

    /** Extracts reference IDs from a JSON array; returns an empty list if null. */
    private static List<String> extractReferences(JsonArray references) {
        Set<String> refs = new HashSet<>();
        if (references != null) {
            for (JsonValue rv : references) {
                if (rv.getValueType() == JsonValue.ValueType.STRING) {
                    String refId = ((JsonString) rv).getString();
                    if (!refId.isEmpty()) refs.add(refId);
                }
            }
        }
        return new ArrayList<>(refs);
    }

    /** Generates a deterministic author ID when none is provided. */
    private static String generateAuthorId(String name, String org) {
        String key = (name != null ? name : "") + "_" + (org != null ? org : "");
        return "AUTHOR_" + Math.abs(key.hashCode());
    }

    /** Starts producer and consumer threads and waits for both to finish. */
    private static void runAndJoin(Thread producer, Thread consumer) throws InterruptedException {
        producer.start();
        consumer.start();
        producer.join();
        consumer.join();
    }
}