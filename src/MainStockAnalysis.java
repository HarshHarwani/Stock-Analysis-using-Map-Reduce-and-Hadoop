
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MainStockAnalysis {

	public static void main(String[] args) throws Exception {		
		long start = new Date().getTime();		
		
	     Job job = Job.getInstance();
	     job.setJarByClass(Map_ReducePhase1.class);
	     Job job2 = Job.getInstance();
	     job2.setJarByClass(Map_ReducePhase2.class);
	     Job job3 = Job.getInstance();
	     job3.setJarByClass(Map_ReducePhase3.class);
	     
		System.out.println("\n**********Stock_Analysis_Hadoop-> Start**********\n");

		job.setJarByClass(Map_ReducePhase1.class);
		job.setMapperClass(Map_ReducePhase1.Map1.class);
		job.setReducerClass(Map_ReducePhase1.Reducer1.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("Inter_1"));
		
		job.waitForCompletion(true);
		
		
		job2.setJarByClass(Map_ReducePhase2.class);
		job2.setMapperClass(Map_ReducePhase2.Map2.class);
		job2.setReducerClass(Map_ReducePhase2.Reducer2.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path("Inter_1"));
		FileOutputFormat.setOutputPath(job2, new Path("Inter_21"));
		
		job2.waitForCompletion(true);
		
	    long mapperCounter = job2.getCounters().findCounter(Map_ReducePhase2.TestCounters.TEST).getValue();
	    Configuration conf = job3.getConfiguration();
	    conf.set("NoOfStocks",String.valueOf(mapperCounter));
	    
		job3.setJarByClass(Map_ReducePhase3.class);
		job3.setMapperClass(Map_ReducePhase3.Map3.class);
		job3.setReducerClass(Map_ReducePhase3.Reducer3.class);

		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path("Inter_21"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));	
		
		boolean status = job3.waitForCompletion(true);
		if (status == true) {
			long end = new Date().getTime();
			System.out.println("\nJob took " + (end-start)/1000 + "seconds\n");
		}
		System.out.println("\n**********Stock_Analysis_Hadoop-> End**********\n");		
	}
}
