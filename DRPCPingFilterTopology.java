package storm.starter.trident.tutorial;

import java.io.IOException;

import storm.starter.trident.tutorial.filters.RegexFilter;
import storm.starter.trident.tutorial.functions.SplitFunction;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class DRPCPingFilterTopology {
	
	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		LocalDRPC drpc = new LocalDRPC();
		cluster.submitTopology("pingpong_drpc", conf, buildTopology(drpc));
		// The 1st arg in drpc.execute(): function name == first arg in newDRPCStrem()
		// The 2nd arg: field tagged as "args" that needs to be parsed
		for (int i = 0; i < 10; i++) {
			System.out.println("DRPC RESULT: "
					+ drpc.execute("ping event", "ping pong ping"));
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
		topology.newDRPCStream("ping event", drpc)
				.each(new Fields("args"), new SplitFunction(" "),
						new Fields("reply"))
				.each(new Fields("reply"), new RegexFilter("ping"))
				.project(new Fields("reply"));
		return topology.build();

	}
}