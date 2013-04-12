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
		initializeDb();
		loadGraphML();
		retrieve();
	}

	private void initializeDb() {
		System.out.println("Initializing graph database.");
		GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder(dbPath)
				.setConfig(GraphDatabaseSettings.node_keys_indexable, "type")
				.setConfig(GraphDatabaseSettings.node_auto_indexing, "true")
				.newGraphDatabase();
		Transaction tx = graphDb.beginTx();

		Node node = graphDb.createNode();
		node.setProperty("type", "god");
		node.setProperty("name", "sample_god");
		tx.success();
		tx.finish();

		getGodsByIndex(graphDb);
		graphDb.shutdown();
		System.out.println();
	}

	private void getGodsByIndex(GraphDatabaseService graphDb) {
		System.out.println("Listing gods:");
		ReadableIndex<Node> autoNodeIndex = graphDb.index()
				.getNodeAutoIndexer().getAutoIndex();
		IndexHits<Node> indexHits = autoNodeIndex.get("type", "god");
		for (Node node : indexHits) {
			System.out.println(node.getProperty("name"));
		}
	}

	private void loadGraphML() throws FileNotFoundException, IOException {
		System.out.println("Loading the GraphML.");
		Neo4jGraph graph = new Neo4jGraph(dbPath);
		GraphMLReader.inputGraph(graph, new FileInputStream(xmlFile));
		graph.shutdown();
		System.out.println("GraphML loaded.");
		System.out.println();
	}

	private void retrieve() {
		System.out.println("Retreive nodes.");
		GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder(dbPath)
				.setConfig(GraphDatabaseSettings.node_keys_indexable, "type")
				.setConfig(GraphDatabaseSettings.node_auto_indexing, "true")
				.newGraphDatabase();

		GlobalGraphOperations graphOperations = GlobalGraphOperations
				.at(graphDb);
		updateAutoIndex(graphDb, graphDb.index().getNodeAutoIndexer(),
				graphOperations.getAllNodes());

		getGodsByIndex(graphDb);

		graphDb.shutdown();
	}

	// source: https://github.com/neo4j/neo4j/issues/173
	private static <T extends PropertyContainer> void updateAutoIndex(
			GraphDatabaseService db, AutoIndexer<T> autoIndexer,
			final Iterable<T> elements) {
		if (!autoIndexer.isEnabled())
			return;
		final Set<String> properties = autoIndexer.getAutoIndexedProperties();
		Transaction tx = db.beginTx();
		int count = 0;
		for (PropertyContainer pc : elements) {
			for (String property : properties) {
				if (!pc.hasProperty(property))
					continue;
				pc.setProperty(property, pc.getProperty(property));
				count++;
				if (count % 10000 == 0) {
					tx.success();
					tx.finish();
					tx = db.beginTx();
				}
			}
		}
		tx.success();
		tx.finish();
	}

}
