// ExclamationTopology from Storm Starter examples
// - Added the two exclaim bolts to the same group by specifying the configuration
// - Ensured that the two bolts have the same number of workers

package org.stark.storm;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class ExampleExclamationTopology {
    public static class ExclamationBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
          _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
          _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
          _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
          declarer.declare(new Fields("word"));
        }
      }

      public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word", new TestWordSpout(), 10);
        // CHANGED THE FOLLOWING TWO LINES
        builder.setBolt("exclaim1", new ExclamationBolt(), 6).localOrShuffleGrouping("word").addConfiguration("group", "one_machine");
        builder.setBolt("exclaim2", new ExclamationBolt(), 6).localOrShuffleGrouping("exclaim1").addConfiguration("group", "one_machine");
        // CHANGED THE ABOVE TWO LINES
        
        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
          conf.setNumWorkers(3);

          StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
          conf.setNumWorkers(3);
          LocalCluster cluster = new LocalCluster();
          cluster.submitTopology("JARVIS", conf, builder.createTopology());
          Utils.sleep(10000);
          cluster.killTopology("JARVIS");
          cluster.shutdown();
        }
      }
}
