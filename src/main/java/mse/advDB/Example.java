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
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.time.Instant;

import java.net.URL;

import static org.neo4j.driver.Values.parameters;

public class Example {

    public static void main(String[] args) throws IOException, InterruptedException {
        String jsonPath = System.getenv("JSON_FILE");
        System.out.println("Path to JSON file is " + jsonPath);
        int nbArticles = Integer.max(1000,Integer.parseInt(System.getenv("MAX_NODES")));
        System.out.println("Number of articles to consider is " + nbArticles);
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

                // MERGE Article node
                session.writeTransaction(tx -> {
                    tx.run(
                        "MERGE (a:Article {_id: $id}) SET a.title = $title",
                        parameters("id", articleId, "title", title)
                    );
                    return null;
                });

                // MERGE Authors nodes + relation AUTHORED
                JsonArray authors = obj.getJsonArray("authors");
                if (authors != null) {
                    for (JsonValue av : authors) {
                        JsonObject author   = (JsonObject) av;
                        String authorId     = author.getString("id", null);
                        String authorName   = author.getString("name", "");

                        if (authorId == null || authorId.isEmpty()) continue;

                        session.writeTransaction(tx -> {
                            tx.run(
                                "MERGE (au:Author {_id: $authorId}) " +
                                "  SET au.name = $name " +
                                "WITH au " +
                                "MATCH (a:Article {_id: $articleId}) " +
                                "MERGE (au)-[:AUTHORED]->(a)",
                                parameters(
                                    "authorId",   authorId,
                                    "name",       authorName,
                                    "articleId",  articleId
                                )
                            );
                            return null;
                        });
                    }
                }

                // MERGE cited Articles + relation CITES
                JsonArray references = obj.getJsonArray("references");
                if (references != null) {
                    for (JsonValue rv : references) {
                        String refId = ((JsonString) rv).getString();
                        if (refId.isEmpty()) continue;

                        final String fRefId = refId;
                        session.writeTransaction(tx -> {
                            tx.run(
                                "MERGE (target:Article {_id: $refId}) " +
                                "WITH target " +
                                "MATCH (src:Article {_id: $srcId}) " +
                                "MERGE (src)-[:CITES]->(target)",
                                parameters("refId", fRefId, "srcId", articleId)
                            );
                            return null;
                        });
                    }
                }

                articleCount++;
                if (articleCount % 500 == 0) {
                    System.out.println("Processed " + articleCount + " articles...");
                }
            }

            // Clean up articles without title (if any)
            /*session.writeTransaction(tx -> {
                tx.run("MATCH (a:Article) WHERE a.title IS NULL DETACH DELETE a");
                return null;
            });*/
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
}
