package mse.advDB;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

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
        FileReader fr = new FileReader(jsonPath);
        BufferedReader br = new BufferedReader(fr);
        System.out.println("Reading first lines of the json file :");
        for (int i = 0; i < 5 ; i++) {
            System.out.println(br.readLine());
        }
        // Create one node in the graph as an example
        try (Session session = driver.session()) {
            session.writeTransaction(tx -> {
                tx.run(
                        "CREATE (p:Person {name: $name, age: $age})",
                        parameters("name", "Alice", "age", 30)
                );
                return null;
            });

        }
        driver.close();
    }
}
