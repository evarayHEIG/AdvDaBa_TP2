package mse.advDB;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.neo4j.driver.Values.parameters;

public class Example {

    private static final Logger logger = LoggerFactory.getLogger(Example.class);

    // Sentinel object to signal end of stream
    private static final List<Map<String, Object>> END_OF_STREAM = new ArrayList<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        String jsonPath = System.getenv("JSON_FILE");
        logger.info("Path to JSON file is {}", jsonPath);
        int nbArticles = Integer.max(1000,Integer.parseInt(System.getenv().getOrDefault("MAX_NODES", "10000000")));
        logger.info("Number of articles to consider is {}", nbArticles);
        int batchSize = Integer.max(1, Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "10000")));
        logger.info("Batch size is {}", batchSize);
        String neo4jIP = System.getenv("NEO4J_IP");
        logger.info("IP addresss of neo4j server is {}", neo4jIP);

        Driver driver = GraphDatabase.driver("bolt://" + neo4jIP + ":7687", AuthTokens.basic("neo4j", "test"));
	boolean connected = false;
	do {
	   try {
	       logger.info("Sleeping a bit waiting for the db");	   
	       Thread.yield();   
               Thread.sleep(5000); // let some time for the neo4j container to be up and running

                driver.verifyConnectivity();
	        connected = true;
            }
	    catch(Exception e) {
  	    }
	} while(!connected);

    // Create constraints (unique index on _id)
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

        // Open JSON stream (the file can be a URL http:// or a local path)
        InputStream inputStream;
        if (jsonPath.startsWith("http://") || jsonPath.startsWith("https://")) {
            inputStream = new URL(jsonPath).openStream();
        } else {
            inputStream = new FileInputStream(jsonPath);
        }

        Instant startTime = Instant.now();
        logger.info("Loading started at instant: {}", startTime);

        // PASS 1: Create Articles and Authors with producer-consumer pattern
        logger.info("=== PASS 1: Creating Articles and Authors ===");
        BlockingQueue<List<Map<String, Object>>> queue1 = new LinkedBlockingQueue<>(batchSize);
        
        // Producer thread: reads from stream and puts batches in queue
        Thread producer1 = new Thread(() -> {
            try {
                InputStream inputStream1;
                if (jsonPath.startsWith("http://") || jsonPath.startsWith("https://")) {
                    inputStream1 = new URL(jsonPath).openStream();
                } else {
                    inputStream1 = new FileInputStream(jsonPath);
                }
                
                try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream1))) {
                    String line;
                    int articleCount = 0;
                    List<Map<String, Object>> batch = new ArrayList<>(batchSize);
                    
                    while ((line = br.readLine()) != null && articleCount < nbArticles) {
                        JsonObject obj;
                        try (JsonReader reader = Json.createReader(new StringReader(line))) {
                            obj = reader.readObject();
                        } catch (Exception e) {
                            logger.warn("Skipping malformed line: {}", e.getMessage());
                            continue;
                        }

                        String articleId = obj.getString("id", null);
                        String title = obj.getString("title", "");
                        if (articleId == null || articleId.isEmpty()) continue;

                        Map<String, Object> row = new HashMap<>();
                        row.put("id", articleId);
                        row.put("title", title);

                        // Prepare authors
                        List<Map<String, Object>> authorRows = new ArrayList<>();
                        JsonArray authors = obj.getJsonArray("authors");
                        if (authors != null) {
                            for (JsonValue av : authors) {
                                JsonObject author = (JsonObject) av;
                                String authorId = author.getString("id", null);
                                String authorName = author.getString("name", "");
                                String authorOrg = author.getString("org", "");
                                if (authorId == null || authorId.isEmpty()) {
                                    authorId = generateAuthorId(authorName, authorOrg);
                                }
                                Map<String, Object> authorRow = new HashMap<>();
                                authorRow.put("id", authorId);
                                authorRow.put("name", authorName);
                                authorRow.put("org", authorOrg != null ? authorOrg : "");
                                authorRows.add(authorRow);
                            }
                        }
                        row.put("authors", authorRows);
                        batch.add(row);

                        articleCount++;
                        if (articleCount % batchSize == 0) {
                            logger.info("[PASS 1 Producer] Read {} articles", articleCount);
                        }

                        if (batch.size() >= batchSize) {
                            queue1.put(new ArrayList<>(batch));
                            batch.clear();
                        }
                    }
                    
                    // Put remaining batch
                    if (!batch.isEmpty()) {
                        queue1.put(batch);
                    }
                    
                    // Signal end of stream
                    queue1.put(END_OF_STREAM);
                    logger.info("[PASS 1 Producer] Finished reading, sent {} articles", articleCount);
                }
            } catch (Exception e) {
                logger.error("Error in PASS 1 Producer: {}", e.getMessage());
                e.printStackTrace();
            }
        });

        // Consumer thread: takes batches from queue and commits to DB
        Thread consumer1 = new Thread(() -> {
            try (Session session = driver.session()) {
                int batchCount = 0;
                int totalArticlesCommitted = 0;
                while (true) {
                    List<Map<String, Object>> batch = queue1.take();
                    if (batch == END_OF_STREAM) {
                        logger.info("[PASS 1 Consumer] Received end signal, finishing");
                        break;
                    }
                    logger.debug("[PASS 1 Consumer] Committing batch of {} articles", batch.size());
                    flushBatchPass1(session, batch);
                    batchCount++;
                    totalArticlesCommitted += batch.size();
                    if (batchCount % 1 == 0) {
                        logger.info("[PASS 1 Consumer] Committed {} batches ({} articles)", batchCount, totalArticlesCommitted);
                    }
                }
            } catch (Exception e) {
                logger.error("Error in PASS 1 Consumer: {}", e.getMessage());
                e.printStackTrace();
            }
        });

        producer1.start();
        consumer1.start();
        producer1.join();
        consumer1.join();
        logger.info("PASS 1 Complete");

        // PASS 2: Create Reference Relations with producer-consumer pattern
        logger.info("=== PASS 2: Creating Reference Relations (CITES) ===");
        BlockingQueue<List<Map<String, Object>>> queue2 = new LinkedBlockingQueue<>(batchSize);
        
        // Producer thread: reads from stream and puts batches in queue
        Thread producer2 = new Thread(() -> {
            try {
                InputStream inputStream2;
                if (jsonPath.startsWith("http://") || jsonPath.startsWith("https://")) {
                    inputStream2 = new URL(jsonPath).openStream();
                } else {
                    inputStream2 = new FileInputStream(jsonPath);
                }
                
                try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream2))) {
                    String line;
                    int articleCount = 0;
                    List<Map<String, Object>> batch = new ArrayList<>(batchSize);
                    
                    while ((line = br.readLine()) != null && articleCount < nbArticles) {
                        JsonObject obj;
                        try (JsonReader reader = Json.createReader(new StringReader(line))) {
                            obj = reader.readObject();
                        } catch (Exception e) {
                            logger.warn("Skipping malformed line: {}", e.getMessage());
                            continue;
                        }

                        String articleId = obj.getString("id", null);
                        if (articleId == null || articleId.isEmpty()) continue;

                        Map<String, Object> row = new HashMap<>();
                        row.put("id", articleId);

                        // Prepare references
                        List<String> referenceRows = new ArrayList<>();
                        JsonArray references = obj.getJsonArray("references");
                        if (references != null) {
                            for (JsonValue rv : references) {
                                String refId = ((JsonString) rv).getString();
                                if (!refId.isEmpty()) {
                                    referenceRows.add(refId);
                                }
                            }
                        }
                        row.put("references", referenceRows);
                        batch.add(row);

                        articleCount++;
                        if (articleCount % batchSize == 0) {
                            logger.info("[PASS 2 Producer] Read {} articles", articleCount);
                        }

                        if (batch.size() >= batchSize) {
                            queue2.put(new ArrayList<>(batch));
                            batch.clear();
                        }
                    }
                    
                    // Put remaining batch
                    if (!batch.isEmpty()) {
                        queue2.put(batch);
                    }
                    
                    // Signal end of stream
                    queue2.put(END_OF_STREAM);
                    logger.info("[PASS 1 Producer] Finished reading, sent {} articles", articleCount);
                }
            } catch (Exception e) {
                logger.error("Error in PASS 1 Producer: {}", e.getMessage());
                e.printStackTrace();
            }
        });

        // Consumer thread: takes batches from queue and commits to DB
        Thread consumer2 = new Thread(() -> {
            try (Session session = driver.session()) {
                int batchCount = 0;
                int totalReferencesCommitted = 0;
                while (true) {
                    List<Map<String, Object>> batch = queue2.take();
                    if (batch == END_OF_STREAM) {
                        logger.info("[PASS 2 Consumer] Received end signal, finishing");
                        break;
                    }
                    logger.debug("[PASS 2 Consumer] Committing batch of {} references", batch.size());
                    flushBatchPass2(session, batch);
                    batchCount++;
                    totalReferencesCommitted += batch.size();
                    if (batchCount % 1 == 0) {
                        logger.info("[PASS 2 Consumer] Committed {} batches ({} references)", batchCount, totalReferencesCommitted);
                    }
                }
                
                // Clean up articles without title
                session.writeTransaction(tx -> {
                    tx.run("MATCH (a:Article) WHERE a.title IS NULL DETACH DELETE a");
                    return null;
                });
            } catch (Exception e) {
                logger.error("Error in PASS 2 Producer: {}", e.getMessage());
                e.printStackTrace();
            }
        });

        producer2.start();
        consumer2.start();
        producer2.join();
        consumer2.join();
        logger.info("PASS 2 Complete");

        // Final stats
        Instant endTime = Instant.now();

        // Count total articles and authors loaded
        try (Session session = driver.session()) {
            long totalArticles = session.readTransaction(tx -> {
                Result r = tx.run("MATCH (a:Article) RETURN count(a) AS c");
                return r.single().get("c").asLong();
            });
            long totalAuthors = session.readTransaction(tx -> {
                Result r = tx.run("MATCH (a:Author) RETURN count(a) AS c");
                return r.single().get("c").asLong();
            });
            long totalRelations = session.readTransaction(tx -> {
                Result r = tx.run("MATCH ()-[:CITES]->() RETURN count(*) AS c");
                return r.single().get("c").asLong();
            });
            long totalNodes = totalArticles + totalAuthors;

            logger.info("=== Loading complete ===");
            logger.info("Started at           : {}", startTime);
            logger.info("Ended at             : {}", endTime);
            logger.info("Duration            : {} seconds", (endTime.getEpochSecond() - startTime.getEpochSecond()));
            logger.info("Articles loaded     : {}", totalArticles);
            logger.info("Authors loaded      : {}", totalAuthors);
            logger.info("Total nodes         : {}", totalNodes);
            logger.info("Citation relations  : {}", totalRelations);
        }

    driver.close();
    }

    /**
     * PASS 1: Create Articles and Authors with AUTHORED relations
     * Optimized with ON CREATE to avoid unnecessary writes
     */
    private static void flushBatchPass1(Session session, List<Map<String, Object>> batchRows) {
        if (batchRows.isEmpty()) {
            return;
        }

        session.writeTransaction(tx -> {
            String query = new StringBuilder()
                .append("UNWIND $rows AS row ")
                .append("MERGE (a:Article {_id: row.id}) ")
                .append("ON CREATE SET a.title = row.title ")
                .append("ON MATCH SET a.title = row.title ")
                .append("WITH a, row ")
                .append("CALL { ")
                .append("  WITH a, row ")
                .append("  UNWIND coalesce(row.authors, []) AS author ")
                .append("  MERGE (au:Author {_id: author.id}) ")
                .append("  ON CREATE SET au.name = author.name, au.org = author.org ")
                .append("  WITH a, au ")
                .append("  MERGE (au)-[:AUTHORED]->(a) ")
                .append("  RETURN count(*) AS authorWrites ")
                .append("} ")
                .append("RETURN count(*) AS processed")
                .toString();

            tx.run(
                query,
                parameters("rows", batchRows)
            );
            return null;
        });
    }

    /**
     * PASS 2: Create reference relations (CITES) only if target article exists
     */
    private static void flushBatchPass2(Session session, List<Map<String, Object>> batchRows) {
        if (batchRows.isEmpty()) {
            return;
        }

        session.writeTransaction(tx -> {
            String query = new StringBuilder()
                .append("UNWIND $rows AS row ")
                .append("MATCH (a:Article {_id: row.id}) ")
                .append("WITH a, row ")
                .append("CALL { ")
                .append("  WITH a, row ")
                .append("  UNWIND coalesce(row.references, []) AS refId ")
                .append("  MATCH (target:Article {_id: refId}) ")
                .append("  MERGE (a)-[:CITES]->(target) ")
                .append("  RETURN count(*) AS refWrites ")
                .append("} ")
                .append("RETURN count(*) AS processed")
                .toString();

            tx.run(
                query,
                parameters("rows", batchRows)
            );
            return null;
        });
    }

    /**
     * Generate an author ID if missing. Format: AUTHOR_<hash of name+org>
     */
    private static String generateAuthorId(String name, String org) {
        String key = (name != null ? name : "") + "_" + (org != null ? org : "");
        int hash = Math.abs(key.hashCode());
        return "AUTHOR_" + hash;
    }
}
