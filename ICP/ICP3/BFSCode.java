import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Reducer;


public class BFSMapReduce {

	public static String OUT = "outfile";
	public static String IN = "inputlarger";

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, LongWritable, Text> {

		/*
		 * Overriding the map function
		 */
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			Text word = new Text();
			String line = value.toString(); 
			String[] sp = line.split("\t| "); 
			int distanceadd = Integer.parseInt(sp[1]) + 1;
			String[] PointsTo = sp[2].split(":");
			for (int i = 0; i < PointsTo.length; i++) {
				word.set("VALUE " + distanceadd); 
													
				output.collect(new LongWritable(Integer.parseInt(PointsTo[i])),
						word);
				word.clear();
			}

			
			word.set("VALUE " + sp[1]);
			output.collect(new LongWritable(Integer.parseInt(sp[0])), word);
			word.clear();

			word.set("NODES " + sp[2]);
			output.collect(new LongWritable(Integer.parseInt(sp[0])), word);
			word.clear();
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<LongWritable, Text, LongWritable, Text> {

		/*
		 * Overriding the reduce function
		 */
		@Override
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			String nodes = "UNMODED";
			Text word = new Text();
			int lowest = 125; 
			while (values.hasNext()) { 
										
				String[] sp = values.next().toString().split(" "); 
																	
				
				if (sp[0].equalsIgnoreCase("NODES")) {
					nodes = null;
					nodes = sp[1];
				} else if (sp[0].equalsIgnoreCase("VALUE")) {
					int distance = Integer.parseInt(sp[1]);
					lowest = Math.min(distance, lowest);
				}
			}
			word.set(lowest + " " + nodes);
			output.collect(key, word);
			word.clear();
		}
	}

	public static void main(String[] args) throws Exception {
		run(args);
	}

	public static void run(String[] args) throws Exception {
		IN = args[0];
		OUT = args[1];
		String input = IN;
		String output = OUT+ System.nanoTime();
		boolean isdone = false;

		
		while (isdone == false) {
			JobConf conf = new JobConf(BFSMapReduce.class);
			conf.setJobName("BFSMapReduce");
			conf.setOutputKeyClass(LongWritable.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			JobClient.runJob(conf);

			input = output + "/part-00000";
			isdone = true;
			Path ofile = new Path(input);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(ofile)));
			HashMap<Integer, Integer> imap = new HashMap<Integer, Integer>();
			String line = br.readLine();
			while (line != null) {
				String[] sp = line.split("\t| ");
				int node = Integer.parseInt(sp[0]);
				int distance = Integer.parseInt(sp[1]);
				imap.put(node, distance);
				line = br.readLine();
			}
			br.close();

			
			Iterator<Integer> itr = imap.keySet().iterator();
			while (itr.hasNext()) {
				int key = itr.next();
				int value = imap.get(key);
				if (value >= 125) {
					isdone = false;
				}
			}
			input = output;
			output = OUT + System.nanoTime();
		}
	}
}
