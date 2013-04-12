package neo4j.storage.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;

import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

public class Neo4jLoader {
	String dbPath = "neo4j-graph";
	String xmlFile = "graph-of-the-gods.xml";
	
	public void work() throws IOException {
		FileUtils.deleteRecursively(new File(dbPath));

		GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder(dbPath)
				.setConfig(GraphDatabaseSettings.node_keys_indexable, "type")
				.setConfig(GraphDatabaseSettings.node_auto_indexing, "true")
				.newGraphDatabase();

		Neo4jGraph graph = new Neo4jGraph(graphDb);
		GraphMLReader.inputGraph(graph, new FileInputStream(xmlFile));
		
		IndexHits<Node> godsByIndex = getGodsByIndex(graphDb, "god");
		for (Node node : godsByIndex) {
			System.out.println(node.getProperty("name"));
		}
	}

	private IndexHits<Node> getGodsByIndex(GraphDatabaseService graphDb, String typeName) {
		ReadableIndex<Node> autoNodeIndex = graphDb.index()
				.getNodeAutoIndexer().getAutoIndex();
		IndexHits<Node> indexHits = autoNodeIndex.get("type", typeName);
		return indexHits;
	}

}
