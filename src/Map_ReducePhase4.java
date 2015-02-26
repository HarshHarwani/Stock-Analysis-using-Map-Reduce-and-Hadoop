import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

class Map_ReducePhase4
{
	
	public static class Map4 extends Mapper<LongWritable, Text, Text, Text>
	{
		//Input-->(App-1,FinalValues)
		public void map(LongWritable key, Text value, Context context)
		{
			try {
				String[] data=value.toString().split("\t");
				String stockname=data[0];
				String FinalValue=data[1];
				Text stocknameText=new Text(stockname);
				context.write(new Text(FinalValue),stocknameText);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class Reducer4 extends Reducer<Text, Text, Text, Text>{
		//Input-->(App-1,XiValues)
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
			String stockName=null;
			Iterator<Text> iterator=value.iterator();
			while(iterator.hasNext())
			{
				Text stockvalue=iterator.next();
				stockName = stockvalue.toString();
			}
				context.write(new Text(stockName),key);
		}
	}
}