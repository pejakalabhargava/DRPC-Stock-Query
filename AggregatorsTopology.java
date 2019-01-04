package storm.starter.trident.tutorial;

import java.io.IOException;

import storm.starter.trident.tutorial.filters.PrintFilter;
import storm.starter.trident.tutorial.spouts.FakeTweetsBatchSpout;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

//------------------------------------
// Step 1: Import proper spouts, filters, functions, etc.
//------------------------------------
// Imports for Custom Spouts
// Imports for Custom Filters
// Imports for Custom Functions
// Imports for Custom Aggregators
/**
 * Skeleton Trident topology
 ** 
 * @author bkakran
 */
public class AggregatorsTopology {
	// -----------------------------------------
	// Step 2: Specify which file spout
	// should read from (if any).
	// Data path relative to pom.xml file.
	// ------------------------------------------
	private static final String DATA_PATH = "data/500_sentences_en.txt";

	public static StormTopology buildTopology() throws IOException {
		// ----------------------------------------
		// Step 3: Define input tuple’s fields of interest
		// ----------------------------------------
		Fields inputFields = new Fields("id", "text", "actor", "location",
				"date");
		// --------------------------------------
		// Step 4: Create Trident Spout that emits
		// batches of tuples
		// --------------------------------------
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(DATA_PATH);
		// ---------------------------------------------------------
		// Step 5: Define filters to apply to the input fields
		// --------------------------------------------------------
		PrintFilter filter = new PrintFilter();
		// ---------------------------------------------------------
		// Step 6: Define functions to operate on the input fields
		// --------------------------------------------------------
		// ------------------------------------------------------
		// Step 7: Define output fields produced by the function
		// ------------------------------------------------------
		// --------------------------------------
		// Step 8: Create TridentTopology object
		// -------------------------------------
		TridentTopology topology = new TridentTopology();
		// -------------------------------------------------
		// Step 9: Create stream of batches using the spout
		// -------------------------------------------------
		Stream streamBatchAggregation = topology.newStream("batch-aggregation",
				spout);
		Stream streamPersistentAggregation = topology.newStream(
				"persistent-aggregation", spout);
		
		// Step 10: Define what to do with each stream’s batch
		// using the chain of methods
		streamBatchAggregation.groupBy(new Fields("actor"))
				.aggregate(new Count(), new Fields("count"))
				.each(new Fields("actor", "count"), new PrintFilter());
		
		// Use newValuesStream() to allow for further process of
		// aggregated results, while persistent aggregation proceeds
		streamPersistentAggregation																																																																																																																																																			
				.groupBy(new Fields("actor"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(),
						new Fields("count")).newValuesStream()
				.each(new Fields("actor", "count"), new PrintFilter(																));
																																																																																																																	
		// ----------------------------------
		// Step 11: Return the built topology
		// ----------------------------------
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
					buildTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("skeleton", conf, buildTopology());
		}
	}
}
