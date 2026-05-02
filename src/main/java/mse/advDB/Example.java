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

import java.net.URL;

import static org.neo4j.driver.Values.parameters;

public class Example {

    public static void main(String[] args) throws IOException, InterruptedException {
        String jsonPath = System.getenv("JSON_FILE");
        System.out.println("Path to JSON file is " + jsonPath);
        int nbArticles = Integer.max(1000,Integer.parseInt(System.getenv("MAX_NODES")));
        System.out.println("Number of articles to consider is " + nbArticles);
        int batchSize = Integer.max(1, Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "1000")));
        System.out.println("Batch size is " + batchSize);
        String neo4jIP = System.getenv("NEO4J_IP");
        System.out.println("IP addresss of neo4j server is " + neo4jIP);

        Driver driver = GraphDatabase.driver("bolt://" + neo4jIP + ":7687", AuthTokens.basic("neo4j", "test"));
	boolean connected = false;
	do {
	   try {
	       System.out.println("Sleeping a bit waiting for the db");	   
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
            System.out.println("Constraints created.");
        }

        // Open JSON stream (the file can be a URL http:// or a local path)
        InputStream inputStream;
        if (jsonPath.startsWith("http://") || jsonPath.startsWith("https://")) {
            inputStream = new URL(jsonPath).openStream();
        } else {
            inputStream = new FileInputStream(jsonPath);
        }

        Instant startTime = Instant.now();
        System.out.println("Loading started at instant: " + startTime);

        int articleCount = 0;
        List<Map<String, Object>> batchRows = new ArrayList<>(batchSize);

        try (Session session = driver.session();
             BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {

            String line;
            while ((line = br.readLine()) != null && articleCount < nbArticles) {

                // parse the line as JSON
                JsonObject obj;
                try (JsonReader reader = Json.createReader(new StringReader(line))) {
                    obj = reader.readObject();
                } catch (Exception e) {
                    System.err.println("Skipping malformed line: " + e.getMessage());
                    continue;
                }

                String articleId = obj.getString("id", null);
                String title     = obj.getString("title", "");


                if (articleId == null || articleId.isEmpty()) continue;

                Map<String, Object> row = new HashMap<>();
                row.put("id", articleId);
                row.put("title", title);

                // Prepare authors data for a batched UNWIND query
                List<Map<String, String>> authorRows = new ArrayList<>();
                JsonArray authors = obj.getJsonArray("authors");
                if (authors != null) {
                    for (JsonValue av : authors) {
                        JsonObject author   = (JsonObject) av;
                        String authorId     = author.getString("id", null);
                        String authorName   = author.getString("name", "");

                        if (authorId == null || authorId.isEmpty()) continue;

                        Map<String, String> authorRow = new HashMap<>();
                        authorRow.put("id", authorId);
                        authorRow.put("name", authorName);
                        authorRows.add(authorRow);
                    }
                }
                row.put("authors", authorRows);

                // Prepare references data for a batched UNWIND query
                List<String> referenceRows = new ArrayList<>();
                JsonArray references = obj.getJsonArray("references");
                if (references != null) {
                    for (JsonValue rv : references) {
                        String refId = ((JsonString) rv).getString();
                        if (refId.isEmpty()) continue;

                        referenceRows.add(refId);
                    }
                }
                row.put("references", referenceRows);

                batchRows.add(row);

                articleCount++;
                if (articleCount % 500 == 0) {
                    System.out.println("Processed " + articleCount + " articles...");
                }

                if (batchRows.size() >= batchSize) {
                    flushBatch(session, batchRows);
                    batchRows.clear();
                }
            }

            // Flush remaining rows
            flushBatch(session, batchRows);

            // Clean up articles without title (if any)
            session.writeTransaction(tx -> {
                tx.run("MATCH (a:Article) WHERE a.title IS NULL DETACH DELETE a");
                return null;
            });
        }

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
            long totalNodes = totalArticles + totalAuthors;

            System.out.println("=== Loading complete ===");
            System.out.println("Started at           : " + startTime);
            System.out.println("Ended at             : " + endTime);
            System.out.println("Duration            : " + (endTime.getEpochSecond() - startTime.getEpochSecond()) + " seconds");
            System.out.println("Articles loaded     : " + totalArticles);
            System.out.println("Authors loaded      : " + totalAuthors);
            System.out.println("Total nodes         : " + totalNodes);
        }

    driver.close();
    }

    private static void flushBatch(Session session, List<Map<String, Object>> batchRows) {
        if (batchRows.isEmpty()) {
            return;
        }

        session.writeTransaction(tx -> {
            String query = new StringBuilder()
                .append("UNWIND $rows AS row")
                .append("MERGE (a:Article {_id: row.id})")
                .append("SET a.title = row.title")
                .append("WITH a, row")
                .append("CALL {")
                .append("  WITH a, row")
                .append("  UNWIND coalesce(row.authors, []) AS author")
                .append("  MERGE (au:Author {_id: author.id})")
                .append("  SET au.name = author.name")
                .append("  MERGE (au)-[:AUTHORED]->(a)")
                .append("  RETURN count(*) AS authorWrites")
                .append("}")
                .append("CALL {")
                .append("  WITH a, row")
                .append("  UNWIND coalesce(row.references, []) AS refId")
                .append("  MERGE (target:Article {_id: refId})")
                .append("  MERGE (a)-[:CITES]->(target)")
                .append("  RETURN count(*) AS referenceWrites")
                .append("}")
                .append("RETURN count(*) AS processed")
                .toString();

            tx.run(
                query,
                parameters("rows", batchRows)
            );
            return null;
        });
    }
}
