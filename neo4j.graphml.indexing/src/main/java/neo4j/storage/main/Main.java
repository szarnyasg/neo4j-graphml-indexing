package neo4j.storage.main;

import java.io.IOException;

public class Main {

	public static void main(String[] args) throws IOException {
		Neo4jLoader neo4jLoader = new Neo4jLoader();
		neo4jLoader.work();
	}
}
