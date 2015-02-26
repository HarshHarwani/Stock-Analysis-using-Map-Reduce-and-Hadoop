import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

class Map_ReducePhase1
{
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>
	{
		
		public void map(LongWritable key, Text value, Context context)
		{
				//Input-->(Entire value of data)
				FileSplit fileSplit = (FileSplit)context.getInputSplit();
				String filename = fileSplit.getPath().getName();
				String filenameWoExtension=filename.substring(0,filename.lastIndexOf('.'));
				String[] line = value.toString().split(",");
				if(line!=null && line.length>0)
				{
					String date=line[0];
					if(!date.equals("Date"))
					{
						String[] dateArray=date.split("-");
						String month=dateArray[1];
						String year=dateArray[0];
						Text filenameText=new Text(filenameWoExtension+"/"+month+"/"+year);
						try {
							context.write(filenameText,value);
						} catch (IOException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
		}
	}
	public static class Reducer1 extends Reducer<Text, Text, Text, Text>{
		//Input-->(App-1*JAN*2012,Entire value of data)
		
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
			int minDate=99;
			int maxDate=-1;
			Double startDateAdjClosePrice = 0.0;
			Double endDateAdjClosePrice = 0.0;
			if(value!=null && !value.toString().isEmpty())
			{
				for(Text text:value)
				{
					String[] line = text.toString().split(",");
					String incomingDate=line[0];
					String[] dateArray=incomingDate.split("-");
					String day=dateArray[2];
					if(Integer.parseInt(day)<minDate)
					{
					 minDate=Integer.parseInt(day);
					 startDateAdjClosePrice=Double.parseDouble(line[6]);
					}
					if(Integer.parseInt(day)>maxDate)
					{
					 maxDate=Integer.parseInt(day);
					 endDateAdjClosePrice=Double.parseDouble(line[6]);
					}
				}
				Double x=(endDateAdjClosePrice-startDateAdjClosePrice)/startDateAdjClosePrice;
				String xi=x.toString();
				Text xiText=new Text(xi);
				context.write(key,xiText);
			}
		}
	}
	
}
