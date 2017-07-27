import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Hw2Part1 {
    static DecimalFormat df = new DecimalFormat("#,###0.000");  //to keep the format of double type  
 
    /**
	 *the class to save count and average,inherit from implements Writable
	 *override function include set,getX,readFields,write
	 */
    
    public static class Record implements Writable {
		private int count;
		private double sum;
		private double avg;

		public void set(int countV, double sumV) {
			this.count = countV;
			this.sum = sumV;
			this.avg = Double.parseDouble(df.format(
					sum/Double.parseDouble(String.valueOf(count))).toString());//calculate the avg with count and sum
		}
		
		/**
		 *
		 * to return Count
		 */
		public int getCount() {
			return this.count;
		}
		
		/**
		 *
		 * to return sum
		 */
		public double getSum() {
			return this.sum;
		}

		/**
		 *
		 * to return avg
		 */
		public double getAvg() {
			return this.avg;
		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			// TODO Auto-generated method stub
			
			this.count=dataInput.readInt();  
	        this.sum=dataInput.readDouble();
	        this.avg=dataInput.readDouble();

		}

		@Override
		public void write(DataOutput dataOutput) throws IOException {
			// TODO Auto-generated method stub
			dataOutput.writeInt(this.count);
			dataOutput.writeDouble(this.sum);
			dataOutput.writeDouble(this.avg);

		}
	}
    
    /**
	 * map into key value
	 * key:source +" "+destination
	 * value:Record object
	 */
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Record> {
		private final static IntWritable one = new IntWritable(1);//to initialize count as 1
		private Text map_key = new Text();//to keep the key

		public void map(Object key, Text value, Context context)//map fuction
				throws IOException, InterruptedException {

			Record record = new Record();//create new Record object
			String item = value.toString();
			String itemLine[] = item.split(" ");//split it with blank
			try{//if there are some illegal data , just ignore it and print message in catch block
			if (itemLine.length == 3)// to make sure the form of the line is collect
			{
				map_key.set(itemLine[0] + " " + itemLine[1]);//set key object
				System.out.println("maper key:" + itemLine[0] + " "
						+ itemLine[0] + "  " + itemLine[2].toString());
				record.set(1,Double.parseDouble(itemLine[2].toString()));//set record object
				context.write(map_key, record);
			}
			}catch (Exception e) {//tip message
				System.out.println("************************************************");
				System.out.println("illegality data!");
				System.out.println("************************************************");

			}

		}
	}

	/**
	 *Combiner to improve the efficiency of reduce
	 *
	 */
	public static class SumCombiner extends
			Reducer<Text, Record, Text, Record> {
		private Record result = new Record();

		public void reduce(Text key, Iterable<Record> values, Context context)
				throws IOException, InterruptedException {
			Record record = new Record();
			int count = 0;
			double sum = 0;
			for (Record val : values) {
				if (!val.equals(null)) {
					
					count += val.getCount();//get count and accumulate them
					sum += val.getSum();//get the sum to add 
				}

			}

			result.set(count, sum);//set value
			context.write(key, result);
		}
	}

	/**
	 *class reduce to combine the value of the same key
	 *
	 */
	public static class SumReducer extends Reducer<Text, Record, Text, Text> {

		private Text result_key = new Text();
		private Text result_value = new Text();
	
		public void reduce(Text key, Iterable<Record> values, Context context)
				throws IOException, InterruptedException {
			Record record = new Record();
			int count = 0;
			double sum = 0;
			for (Record val : values) {
				count += val.getCount();
				sum += val.getSum();

			}
			record.set(count, sum);			
			result_key=key;
			String item=String.valueOf(record.getAvg());
			String items[]=item.split("\\.");
			for(int i=items[1].length();i<3;i++)//in order to keep the format
			{
				item=item+"0";
			}
			result_value.set(" " +(record.getCount())
					+ " " +item);

			context.write(result_key, result_value);
		}
	}
	
	/**
	 *main function the entrence of the program
	 *args contain the path information and jar information
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(Hw2Part1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(SumCombiner.class);
		job.setReducerClass(SumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Record.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path (otherArgs[otherArgs.length - 1])))//to judge whether the path is exists
		{
			FileSystem hdfs = FileSystem.get(conf);//get FileSystem
			boolean isDel = hdfs.delete(new Path (otherArgs[otherArgs.length - 1]),true);//delete it
			System.out.println("exits and delete it seccfully");// tip to show selete it seccfully
		}

		// add the input paths as given by command line
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		// add the output path as given by the command line
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


