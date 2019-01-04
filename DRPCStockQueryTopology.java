// Advanced Algorithms Course, NC State, Computer Science
// Trident/Storm Lab: Develop DRPC query of the stock trades
// Instructor: Nagiza Samatova
// Student: Bhargava Pejakala kakrannaya 

package storm.starter.trident.tutorial; 

import storm.starter.trident.tutorial.functions.SplitFunction;
import storm.starter.trident.tutorial.spouts.CSVBatchSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
/**
 * DRPCStockQueryTopology to query about symbol and volume of stocks
 * 
 * @author bkakran
 *
 */
public class DRPCStockQueryTopology {

    // Path relative to pom.xml file
    private static final String DATA_PATH = "data/stocks.csv.gz";
    

    /**
     * Start a Trident topology that reads stock symbols and prices from a CSV data file and queries  the topology with DRPC 5 sec.
     * @param args
     * @throws Exception
     */
	public static void main(String[] args) throws Exception {
		//Create a config
		Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxSpoutPending(20);
		//Create a local DRPC cluster
		LocalCluster cluster = new LocalCluster();
		LocalDRPC drpc = new LocalDRPC();
		//Submit topology as state_drpc
		cluster.submitTopology("state_drpc", conf, buildTopology(drpc));
		for (int i = 0; i < 10; i++) {
			// Make DRPC call with "trade_events" handler and arguments
			// "AAPL INTC GE" and Print DRPC results to standard output
			System.out.println("DRPC RESULT: "
					+ drpc.execute("trade_events", "AAPL INTC GE"));
			Thread.sleep(5000);
		}

	}
	/**
	 * This static method ingests the data from CSV spot and groups the results
	 * by symbol and stores the count in in memory hash map.Against this  the
	 * queries are generated to count the symbol and volume
	 * @param drpc
	 * @return
	 */
    public static StormTopology buildTopology( LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();

        // Fields that needs to be emitted from the spout in the order 
        //"date", "symbol", "price", "shares"
        Fields fields = new Fields("date", "symbol", "price", "shares");
        
		//Use this spout incase data needs to eb read from a .csv file directly
        //StockBatchSpout spoutCSV = new StockBatchSpout(DATA_PATH, fields);
        
        //Create CSV spout, called spoutCSV from the CSV file specified in DATA_PATH.THis spout 
        //is used to csv from gz file
		CSVBatchSpout spoutCSV = new CSVBatchSpout(DATA_PATH, fields);
        
        
		// --Creates TridentState in-memory hash map, called tradeVolumeDBMS which ingests the data stream from the spoutCSV spout
		// --Groups the tuples by "symbol" field
		// --Performs persistent aggregation and store (key,value) in in-process memory map where key 
		//	 is "symbol" and "value" is aggregated field called "volume"
		TridentState tradeVolumeDBMS = topology
				.newStream("spout", spoutCSV)
				.groupBy(new Fields("symbol"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(),
						new Fields("volume"));
		

		//-- Creates a stream--DRPC stream whichc generates a list of "symbols" passed as args by the client
		//-- Query the persistent aggregate state, tradeVolumeDBMS, by "symbol" for the "volume" value.
		//-- Project()  the following fields of interest: "symbol" and "volume"
		//-- DRPC client prints on stdout
		topology.newDRPCStream("trade_events", drpc)
		.each(new Fields("args"), new SplitFunction(" "),
				new Fields("symbol"))
		.stateQuery(tradeVolumeDBMS, new Fields("symbol"),
				new MapGet(), new Fields("volume"))
		.each(new Fields("symbol", "volume"),
				new FilterNull()).project(new Fields("symbol", "volume"));
		
		//Return the topology
		return topology.build();
    }
}