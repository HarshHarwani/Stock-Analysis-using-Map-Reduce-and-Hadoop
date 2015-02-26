import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


class Map_ReducePhase2
{
	static enum TestCounters { TEST }
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text>
	{
		//Input-->(App-1*JAN*2012,XiValues)
		
		public void map(LongWritable key, Text value, Context context)
		{
			if(value!=null && !value.toString().isEmpty()){
				try {
					String[] data=value.toString().split("\t");
					String stockNameFull=data[0];
					String xiValue=data[1];
					String[] stockname=stockNameFull.split("/");
					String stockId=stockname[0]; 
					Text stockIdText=new Text(stockId);
					context.write(stockIdText,new Text(xiValue));
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	public static class Reducer2 extends Reducer<Text, Text, Text, DoubleWritable>{
		//Input-->(App-1,XiValues)
		
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
			double n=0.0;
			Double xiSum=0.0,xbar=0.0,interMediateSum=0.0,xi=0.0,finalVolatility=0.0;
			List<Double> xiList=new ArrayList<Double>();
			if(value!=null & !value.toString().isEmpty())
			{
				for(Text text:value)
				{
					
					String valueinString=text.toString();
					xi=Double.parseDouble(valueinString);
					xiList.add(xi);
					xiSum+=xi;
					n++;
				}
				xbar=(xiSum/n);
				for(Double xiListValue:xiList)
				{
					interMediateSum+=Math.pow(xiListValue-xbar, 2);
				}
				finalVolatility=Math.sqrt(interMediateSum/(n-1));
				//String finalVolatilityText = finalVolatility.toString();
				if(finalVolatility>0 && key!=null)
				{
					context.getCounter(TestCounters.TEST).increment(1);
					context.write(key,new DoubleWritable(finalVolatility));
				}
			}
		}
	}
	
}