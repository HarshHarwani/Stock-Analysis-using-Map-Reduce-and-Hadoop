import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
class Map_ReducePhase3
{

	public static class Map3 extends Mapper<LongWritable, Text, DoubleWritable, Text>
	{

		//Input-->(App-1,FinalValues)
		public void map(LongWritable key, Text value, Context context)
		{
			try {
				String[] data=value.toString().split("\t");
				String stockname=data[0];
				String FinalValue=data[1];
				Text stocknameText=new Text(stockname);
				context.write(new DoubleWritable(Double.parseDouble(FinalValue)),stocknameText);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static class Reducer3 extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{
		//Input-->(App-1,XiValues)
		public static Long counter=null;
		public int ascecount=0;
		public void reduce(DoubleWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
			String stockName=null;
			Iterator<Text> iterator=value.iterator();
			Configuration conf=context.getConfiguration();
			counter=Long.parseLong(conf.get("NoOfStocks"));
			if(ascecount==0)
				context.write(new Text("Top 10 stocks with Minimum volatility"),new DoubleWritable(0.0));
			while(iterator.hasNext())
			{
				Text stockvalue=iterator.next();
				stockName = stockvalue.toString();
				if(ascecount<10)	
					context.write(new Text(stockName),key);
				ascecount++;
				if(ascecount==10)
					context.write(new Text("Top 10 stocks with Maximum volatility"),new DoubleWritable(1.0));
				if(ascecount>=counter-9)	
					context.write(new Text(stockName),key);
			}

		}

	}
}