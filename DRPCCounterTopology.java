package storm.starter.trident.tutorial;

import java.io.IOException;

import storm.starter.trident.tutorial.filters.RegexFilter;
import storm.starter.trident.tutorial.functions.SplitFunction;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class DRPCCounterTopology {
	
	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		LocalDRPC drpc = new LocalDRPC();
		cluster.submitTopology("pingpong_drpc", conf, buildTopology(drpc));
		// The 1st arg in drpc.execute(): function name == first arg in newDRPCStrem()
		// The 2nd arg: field tagged as "args" that needs to be parsed
		for (int i = 0; i < 10; i++) {
			System.out.println("DRPC RESULT: "
					+ drpc.execute("count_event", "advanced advanced algorithms course"));
			Thread.sleep(1000);
		}
		System.out.println("STATUS: OK");
		cluster.shutdown();
		drpc.shutdown();
		// You can use a client library to make calls remotely
		// DRPCClient client = new DRPCClient("drpc.server.location", 3772);
		//
	}

	private static StormTopology buildTopology(LocalDRPC drpc)
			throws IOException {
		TridentTopology topology = new TridentTopology();
		topology.newDRPCStream("count_event", drpc)
				.each(new Fields("args"), new SplitFunction(" "),
						new Fields("split"))
				.each(new Fields("split"), new RegexFilter("a.*"))
				.groupBy(new Fields("split"))
				.aggregate(new Count(), new Fields("count"))
				.project(new Fields("split","count"));
		return topology.build();

	}
}