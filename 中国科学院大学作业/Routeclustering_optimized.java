package cluster;
/**
 * @author	snowhite
 * @version	2013-1-7 上午10:29:23
 * @purpose			hadoop jar Routeclustering_optimized.jar [主类名] <path of regioncode256.txt> <input path> <output path prefix> 
 * 					input为数据输入路径，最终一级为文件夹，是起始终止城市id编号，格式为： 起始城市id_终点城市id
 */

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.GenericOptionsParser;


public class Routeclustering_optimized 
{
 private static int samplenum = 2000;  //system parameter
 public static double gpsnumrange[] = {1000,100000};   //the permitted range of gps point number
 public static int maxK;  //it is the maximum K permitted, it is also the number of reducers in the 2nd mapreduce
 public static double percent= 0.8;
 private static double rotationmat[] = {0.707, 0.707, -0.707, 0.707};
 
 public static class point implements Writable
 {
	  public DoubleWritable x;
	  public DoubleWritable y;
	  
	  
	  public point()
	  {
		  x = new DoubleWritable();
		  y = new DoubleWritable();
	  }
	  
	  public point(double lon, double lat)
	  {
		  x = new DoubleWritable(lon);
		  y = new DoubleWritable(lat);
	  }
	  
	  public point(point temp)
	  {
		  x = temp.x;
		  y = temp.y;
	  }
	  
	  public void set_point(double lon, double lat)
	  {
		  x = new DoubleWritable(lon);
		  y = new DoubleWritable(lat);
	  }
	  
	  
	  
	  public void write(DataOutput out) throws IOException 
	  {
		  x.write(out);
		  y.write(out);
	  }
	  
	  public void readFields(DataInput in) throws IOException
	  {
		  x.readFields(in);
		  y.readFields(in);
	  }
	  
	  public double compare(point p)
	  {
		  return (x.get()-p.x.get())*(x.get()-p.x.get()) + (y.get() - p.y.get())*(y.get() - p.y.get());
	  }
}
   
 public static class vector extends ArrayWritable
 {
	   public vector()
	   {
		   super(DoubleWritable.class);
	   }
 }
 
 public static class vector_withid implements Writable
 {
	   public vector vec;
	   public Text id;
	   
	   public vector_withid()
	   {
		   vec = new vector();
		   id = new Text();
	   }
	   
	   public vector_withid(DoubleWritable[] a, Text b)
	   {
		   vec = new vector();
		   vec.set(a);
		   id = new Text(b);
	   }
		@Override
		public void readFields(DataInput in) throws IOException 
		{
			vec.readFields(in);
			id.readFields(in);
		}
		@Override
		public void write(DataOutput out) throws IOException 
		{
			vec.write(out);
			id.write(out);
		}
		   
 }
 
 public static class segment
 {
	   public double a;      //ax+b = y in (x1,x2), (y1,y2)
	   public double b;
	   public double x1;
	   public double y1;
	   public double x2;
	   public double y2;
	   public segment(double n1, double n2, double n3, double n4, double n5, double n6)
	   {
		   a = n1;
		   b = n2;
		   x1 = n3;
		   y1 = n4;
		   x2 = n5;
		   y2 = n6;
	   }
 }
 
 public static class CityGeoInfo implements Writable
 {
	   public LongWritable cityid;
	   public point cityGeopoint;
	   
	   public CityGeoInfo()
	   {
		   cityid = new LongWritable();
		   cityGeopoint = new point();
	   }
	   
	   public CityGeoInfo(long id, double x, double y)
	   {
		   cityid = new LongWritable(id);
		   cityGeopoint = new point(x,y);
	   }
	   
	   public void set(long id, double x, double y)
	   {
		   cityid = new LongWritable(id);
		   cityGeopoint.set_point(x, y);
	   }
	   
	   public double diff(point object)
	   {
		   return Math.abs((object.x.get() - cityGeopoint.x.get())+(object.y.get() - cityGeopoint.y.get()));
	   }

		@Override
		public void readFields(DataInput in) throws IOException 
		{
			cityid.readFields(in);
			cityGeopoint.readFields(in);
		}
	
		@Override
		public void write(DataOutput out) throws IOException 
		{
			cityid.write(out);
			cityGeopoint.write(out);
		}
	   
 }
 
 private static point projector(double x, double y)
 {
	   point result = new point(x*rotationmat[0] + y*rotationmat[1], x*rotationmat[2] + y*rotationmat[3]);
	   return result;
 }
 
 public static class regularization_map extends Mapper<Object,Text,IntWritable,vector_withid>
 {
	   
	   DoubleWritable[] samplepoints;
	   point destination;
	   point departure;
	   
	   @Override
	   protected void setup(Context context)  
	   {		
		   try {
				samplepoints = DefaultStringifier.loadArray(context.getConfiguration(), "samplepoints", DoubleWritable.class);
		   } catch (IOException e) {
			    e.printStackTrace();
		   }
		   destination = new point(Double.parseDouble(context.getConfiguration().get("dest_x")),Double.parseDouble(context.getConfiguration().get("dest_y")));
		   departure = new point(Double.parseDouble(context.getConfiguration().get("dep_x")),Double.parseDouble(context.getConfiguration().get("dep_y")));
		   
	   }
		  
	   private List<point> interpreter(String line)
	   {
		   List<point> result = new ArrayList<point>();
		   String[] record = line.split(";");
		   for(int i = 0; i<record.length; i++)
		   {
			   String[] x_y = record[i].split(",");
			   point temp = projector(Double.parseDouble(x_y[0]),Double.parseDouble(x_y[1]));
			   result.add(temp);
		   }
		   if(destination.compare(result.get(0)) > departure.compare(result.get(0)))
		   {
			   result.add(0, departure);
			   result.add(destination);
		   }
		   else
		   {
			   result.add(0, destination);
			   result.add(departure);
		   }
		   return result;
	   }
	   
	   public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	   {
		   String record = value.toString();
		   String[] sections = record.split("	");
		   String[] head = sections[0].split("_");
		   String departureid = head[1];
		   String destinationid = head[2];
		   
		   
		   if(!((departureid.compareTo(context.getConfiguration().get("dep_id"))==0 && destinationid.compareTo(context.getConfiguration().get("dest_id"))==0)  ||  (departureid.compareTo(context.getConfiguration().get("dest_id"))==0 && destinationid.compareTo(context.getConfiguration().get("dep_id"))==0)))
				   return;
		   
		  
	       //parse this string into gps list, which has been projected
		   List<point> datapoints = interpreter((value.toString().split("	"))[6]);	
		   
		   int datalen = datapoints.size();
		   
		   //segment representation
		   List<segment> segments = new ArrayList<segment>();
		   
		   
		   //convert the current gps sequence into segment representation
		   point p1,p2;
		   double a,b;
		   for(int i = 0;i<datalen-1; i++)
		   {
			   p1 = datapoints.get(i);
			   p2 = datapoints.get(i+1);
			   if(p2.x.get() == p1.x.get())
			   {
				   a = 0;
				   b = 0;
			   }
			   else
			   {
				   a = (p2.y.get()-p1.y.get())/(p2.x.get()-p1.x.get());
				   b = p2.y.get() - a*p2.x.get();
			   }
			   segments.add(new segment(a,b,p1.x.get(),p1.y.get(),p2.x.get(),p2.y.get()));
		   }
		   
		   DoubleWritable[] regresult = new DoubleWritable[samplepoints.length];
		   int currentpos = 0;
		   
		   //start to compute regularization
		   double currentsample;
		   for(int i = 0; i<samplepoints.length/2; i++)
		   {
			   currentsample = samplepoints[i].get();
			   for(int j = 0; j<segments.size(); j++)
			   {
				   segment currentseg = segments.get(j);
				   if((currentseg.x1<currentsample && currentsample<=currentseg.x2) ||(currentseg.x2<currentsample && currentsample<=currentseg.x1) )
				   {
					   regresult[currentpos] = new DoubleWritable(currentseg.a*currentsample+currentseg.b);
					   currentpos++;
					   break;
				   }
			   }
		   }
		   for(int i = samplepoints.length/2; i<samplepoints.length; i++)
		   {
			   currentsample = samplepoints[i].get();
			   for(int j = 0; j<segments.size(); j++)
			   {
				   segment currentseg = segments.get(j);
				   if((currentseg.y1<currentsample && currentsample<=currentseg.y2) || (currentseg.y2<currentsample && currentsample<=currentseg.y1))
				   {
					   if(currentseg.a != 0)
						   regresult[currentpos] = new DoubleWritable((currentsample-currentseg.b)/currentseg.a);
					   else
						   regresult[currentpos] = new DoubleWritable(0);
					   currentpos++;
					   break;
				   }
				}
			}
		  vector_withid result = new vector_withid(regresult, new Text(sections[0]));
		   
		   //send copy to all reducers
		  for(int i = 1;i<8;i++)
		  {
			  context.write(new IntWritable(i+1), result);
		  }
		
	   }
	   
 }
 
 public static class clustering_reduce extends Reducer<IntWritable, vector_withid, IntWritable, Text>
 {
	     
	   double[] max_vec;
	   double[] min_vec;
	   int[] closestroute;
	   
	   
	   public int[] filter(List<double[]> data, List<double[]> centroids, int[] num_eachcluster, int[] result, int k)
	   {
		   int[] filteredresult = new int[data.size()];
		   
		   filteredresult = (int[])result.clone();
		   
		   double[] threshold = new double[k];
		   
		   double[] distlist;
		   double[] dist = new double[data.size()];
		   double[] currentpath;
		   double[] itscent;
		   int counter;
		   
		   for(int i = 0; i<k; i++)
		   {
			   distlist = new double[num_eachcluster[i]];
			   counter = 0;
			   for(int j = 0; j<data.size(); j++)
			   {
				   if(result[j] == i)
				   {
					   currentpath = data.get(j);
					   itscent = centroids.get(i);
					   double temp = distance(currentpath,itscent);
					   distlist[counter] = temp;
					   if(dist[j] == 0)
						   dist[j] = temp;
					   counter++;
				   }
			   }
			   quicksort(distlist,0,distlist.length-1);
			   threshold[i] = distlist[(int)(distlist.length*percent)];
		   }
		   
		   for(int i = 0; i<data.size(); i++)
		   {
			   if(dist[i] > threshold[result[i]])
				   filteredresult[i] = -1;
		   }
		   
		   
		   return filteredresult;
	   }
	   
	   public int partition(double arr[], int left, int right)
	   {
		   int i = left;
		   int j = right;
		   double tmp;
		   double pivot = arr[(left+right)/2];
		   while(i<=j)
		   {
			   while(arr[i]<pivot)
				   i++;
			   while(arr[j]>pivot)
				   j--;
			   if(i<=j)
			   {
				   tmp = arr[i];
				   arr[i] = arr[j];
				   arr[j] = tmp;
				   i++;
				   j--;
			   }
		   }
		   return i;
	   }
	   
	   public void quicksort(double arr[], int left, int right)
	   {
		   int ind = partition(arr,left,right);
		   if(left < ind - 1)
			   quicksort(arr, left, ind - 1);
		   if(ind < right)
			   quicksort(arr, ind, right);
	   }
	   
	   public void find_boundary(List<double[]> data)
	   {
		   max_vec = new double[samplenum*2];
		   min_vec = new double[samplenum*2];
		   for(int i = 0; i<samplenum*2; i++)
		   {
			   max_vec[i] = -99999;
			   min_vec[i] = 9999;
		   }
		   double[] current;
		   for(int i = 0; i<data.size(); i++)
		   {
			   current = data.get(i);
			   for(int j = 0; j<current.length; j++)
			   {
				   if(max_vec[j] < current[j])
					   max_vec[j] = current[j];
				   if(min_vec[j] > current[j])
					   min_vec[j] = current[j];
			   }
		   }	   
	   }
	   
	   public double distance(double[] vec1, double[] vec2)
	   {
		   double result = 0;
		   if(vec1.length != vec2.length)
			   return 9999.0;
		   for(int i = 0; i<vec1.length; i++)
		   {
			   result = result + (vec1[i]-vec2[i])*(vec1[i]-vec2[i]);
		   }
		   return Math.sqrt(result);
	   }
	   
	   public double compute_DBI(List<double[]> data, List<double[]> centroids, int[] num_eachcluster, int[] result, int k)
	   {
		   double[] current;
		   double[] itscent;
		   double[] avgdistance = new double[k];
		   closestroute = new int[k];
		   double[] mindistance = new double[k];
		   for(int i = 0; i<k; i++)
			   mindistance[i] = 99999999;
		   double dist;
		   for(int i = 0; i<data.size(); i++)
		   {
			   int currentind = result[i];
			   current = data.get(i);
			   itscent = centroids.get(currentind);
			   dist = distance(current,itscent);
			   avgdistance[currentind] = avgdistance[currentind] + dist;
			   if(mindistance[currentind] > dist)
			   {
				   mindistance[currentind] = dist;
				   closestroute[currentind] = i;
			   }
			   else
				   continue;
				   
		   }
		   for(int i = 0; i<k; i++)
			   avgdistance[i] = avgdistance[i]/num_eachcluster[i];
		   double[][] centdistance = new double[k][k];
		   for(int i = 0; i<k; i++)
		   {
			   double[] cent1 = centroids.get(i);
			   for(int j = i+1; j<k; j++)
			   {
				   double[] cent2 = centroids.get(j);
				   centdistance[i][j] = distance(cent1, cent2);
			   }
		   }
		   double temp;
		   double dbi = 0;
		   for(int i = 0; i<k; i++)
		   {
			   double currentmax = -9999;
			   for(int j = 0; j<k; j++)
			   {
				   if(centdistance[i][j] != 0)
					   temp = (avgdistance[i]+avgdistance[j])/centdistance[i][j];
				   else if(centdistance[j][i] !=0)
					   temp = (avgdistance[i]+avgdistance[j])/centdistance[j][i];
				   else
					   continue;
				   if(currentmax < temp)
					   currentmax = temp;
			   }
			   dbi = dbi + currentmax;
			}
		   return dbi;
	   }
	   
	   
	   public void reduce(IntWritable key, Iterable<vector_withid> values, Context context) throws IOException, InterruptedException
	   {
		   List<double[]> data = new ArrayList<double[]>();   //regresult
		   List<Text> id = new ArrayList<Text>();
		   
		   //get regresult from values
		   for(vector_withid val:values)
		   {
			   double[] tempvector = new double[samplenum*2];
			   Writable[] currentvector = val.vec.get();
			   for(int i = 0; i<samplenum*2; i++)
			   {
				   tempvector[i] = ((DoubleWritable)currentvector[i]).get();
			   }
			   data.add(tempvector);  //
			   id.add(new Text(val.id));  //the ith regresult is of ith routes in raw data
		   }
		   
		   if(data.size() < key.get())
			   return;
		   
		   if(data.size() < 2)
		   {
			   String str1 = new String();
			   String str2 = new String();
			   String str3 = new String();
			   str1 =  id.get(0).toString() + ">0,";
			   str2 = "1,";
			   context.write(key, new Text("1")); //the first line is the dbi value
			   str3 = id.get(0).toString() + ">1,";
			   context.write(key, new Text(str3));  //the route can represent the corresponding cluster
			   context.write(key, new Text(str1));  //clustering result
			   context.write(key, new Text(str2));  //the number of routes in each cluster before filtering
			   
			   return;
		   }
		   
		   int k = key.get(); //the number of clusters the current reducer will be focusing on
		   
		   find_boundary(data); //get the low and upper bound of different dimensions of regdata
		   
		   double currentrand;
		   //generate k initial centroids
		   List<double[]> centroids = new ArrayList<double[]>();
		   for(int i = 0; i<k; i++)
		   {
			   double[] temp = new double[samplenum*2];
			   for(int j = 0; j<samplenum*2; j++)
			   {
				     currentrand = Math.random()*999999;
	    			 currentrand = currentrand % Math.abs(max_vec[j] - min_vec[j]);
	    			 temp[j] = min_vec[j] + currentrand;		  
	    		}
			   	centroids.add(i,temp);
		   }
		   
		   
		   int num_paths = data.size();
		   int[] result = new int[num_paths];
		   int[] num_eachcluster = new int[k];
		   double diff = 0;
		   int counter = 0;
		   double[] currentpath = null;
	       double[] currentcentroid = null;
	       double[] prevcentroid = null;
	       double currentmindist = 9999999;
	       double val = 0;
	       int currentresult = -1;
		   
	       List<double[]> prev_centroids = new ArrayList<double[]>();
		   while(true)
		   {
			    for(int i = 0; i<k; i++)
	    			num_eachcluster[i] = 0;
	    		currentmindist = 9999999;
	    		diff = 0;
	    		currentresult = -1;
	    		val = 0;
	    		
	    		//judge which cluster every point belongs to
		    	for(int i = 0; i<num_paths; i++)
		    	{
		    		currentpath = data.get(i);
		    		currentmindist = 999999999;
		    		currentresult = -1;
		    		for(int j=0; j<k; j++)
		    		{
		    			currentcentroid = centroids.get(j);
		    			val = distance(currentpath,currentcentroid);
		    			if(currentmindist>val)
		    			{
		    				currentmindist = val;
		    				currentresult = j;
		    			}
		    		}
		    		result[i] = currentresult;
		    		num_eachcluster[currentresult]++;
		    	}
		    	
		       //handle with the case that there is a empty cluster
	    	   Random rand = new Random();
	    	   for(int i = 0; i<k; i++)
	    	   {
	    		   if(num_eachcluster[i] == 0)
	    		   {
	    			   int randp1 = Math.abs(rand.nextInt())%num_paths;
	    			   int randp2;
	    			   while(true)
	    			   {
	    				   randp2 = Math.abs(rand.nextInt())%num_paths;
	    				   if(randp2!=randp1)
	    					   break;
	    			   }
	    			   
	    			   num_eachcluster[result[randp1]]--;
	    			   num_eachcluster[result[randp2]]--;
	    			   result[randp1] = i;
	    			   result[randp2] = i;
	    			   double[] cent = new double[samplenum*2];
	    			   double[] p1 = data.get(randp1);
	    			   double[] p2 = data.get(randp2);
	    			   for(int j = 0; j<samplenum*2; j++)
	    			   {
	    				   cent[j] = (p1[j] + p2[j])/2;
	    			   }
	    			   centroids.set(i, cent);
	    			   num_eachcluster[i] = 2;
	    		    }
	    	   }

	    	    
	    		//backup current centroids for comparing the difference
	    	    prev_centroids.clear();
	 		   
	    		/*for(int i = 0; i<centroids.size(); i++)
	    		{
	    			prev_centroids.add(i, centroids.get(i));
	    		}*/
	    	    prev_centroids = centroids;
	    		centroids.clear();
	    		//compute new centroids
	    		double[][] sum = new double[k][samplenum*2];
	    		for(int i = 0; i<num_paths; i++)
	    		{
	    			int ind = result[i];
	    			double[] currentdata = data.get(i);
	    			for(int j = 0; j<samplenum*2; j++)
	    			{
	    				sum[ind][j] = sum[ind][j] + currentdata[j];
	    			}
	    		}
	    		for(int i = 0; i<k; i++)
	    		{
	    			for(int j = 0; j<samplenum*2; j++)
	    			{
	    				sum[i][j] = sum[i][j]/num_eachcluster[i];
	    			}
	    			centroids.add(i, sum[i]);
	    		}
	    		counter++;
	    		//check if it is converge or not
	    		diff = 0;
	    		for(int i = 0; i<k; i++)
	    		{
	    			currentcentroid = centroids.get(i);
	    			prevcentroid = prev_centroids.get(i);
	    			for(int j = 0; j<samplenum*2; j++)
	    			{
	    				diff = diff + Math.abs(currentcentroid[j]-prevcentroid[j]);
	    			}
	    		}
	    		//yes, it is converged, iterations is terminated
	    		if(diff < 0.001 || counter>1000)
	    		{
	    			for(int i = 0; i<k; i++)
	    			{
	    				if(num_eachcluster[i]==0)
	    					return;
	    			}
	    			break;
	    		}
	    	
	    	}
		   
		   //write result
		   String str1 = new String();
		   String str2 = new String();
		   String str3 = new String();
		   
		   context.write(key, new Text(String.valueOf(compute_DBI(data,centroids,num_eachcluster,result,k)))); //the first line is the dbi value
		   
		   for(int i = 0; i<k; i++)
		   {
			   str3 = str3 + id.get(closestroute[i]).toString() + ">" + String.valueOf((float)num_eachcluster[i]/(float)num_paths)+",";
			   //str3 = str3 + String.valueOf(closestroute[i]) + ",";
		   } 
		   context.write(key, new Text(str3));  //the route can represent the corresponding cluster
		   
		   
		   for(int i = 0; i<num_paths; i++)
		   {
			   str1 = str1 + id.get(i).toString() + ">" + String.valueOf(result[i]) + ",";
		   }
		   context.write(key, new Text(str1));  //clustering result
		   
		   for(int i = 0; i<k; i++)
		   {
			   str2 = str2 + String.valueOf(num_eachcluster[i]) + ",";
		   }
		   context.write(key, new Text(str2));  //the number of routes in each cluster before filtering
		  
	   }
 }
 
 public class clusteringpartitioner extends Partitioner<IntWritable,vector_withid>
 {
	   public int getPartition(IntWritable key, vector_withid vec, int numPartitions)
	   {
		   return key.get()-2;
	   }
 }
 
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
	/*	if (otherArgs.length != 1) {
			System.exit(2);
		}*/

		Map<String, point> citydata = new HashMap<String, point>();

		String line;
		String[] tokens;
		BufferedReader br = new BufferedReader(new FileReader(otherArgs[0]
				));
		while ((line = br.readLine()) != null) {
			tokens = line.split("\t");
			try {
				if (Integer.parseInt(tokens[1]) == -256) {
					citydata.put(
							tokens[2],
							new point(Double.parseDouble(tokens[5]), Double
									.parseDouble(tokens[6])));
					// citydata.add(new CityGeoInfo(Long.parseLong(tokens[2]),
					// ));
				}

			} catch (NumberFormatException e) {
				System.out.println("NumberFormatException");
			}
		}
		br.close();

		String[] path = otherArgs[1].split("\\/");
		String[] ids = path[path.length - 1].split("_");
		String depid = ids[0];
		String destid = ids[1];

		point tmp = citydata.get(depid);

		double x1 = tmp.x.get(); // (x1,y1) is departure gps
		double y1 = tmp.y.get();

		tmp = citydata.get(destid);
		double x2 = tmp.x.get(); // (x2,y2) is destination gps
		double y2 = tmp.y.get();

		point departure = projector(x1, y1);
		point destination = projector(x2, y2);
		List<DoubleWritable> samplepoints = new ArrayList<DoubleWritable>();
		double interval = (destination.x.get() - departure.x.get())
				/ (samplenum + 1);
		for (int i = 0; i < samplenum; i++) {
			samplepoints.add(new DoubleWritable(departure.x.get() + (i + 1)
					* interval));
		}
		interval = (destination.y.get() - departure.y.get()) / (samplenum + 1);
		for (int i = 0; i < samplenum; i++) {
			samplepoints.add(new DoubleWritable(departure.y.get() + (i + 1)
					* interval));
		}
		DoubleWritable[] tmp_arr = new DoubleWritable[samplenum * 2];
		DefaultStringifier.storeArray(conf, samplepoints.toArray(tmp_arr),
				"samplepoints");
		conf.set("dest_x", String.valueOf(destination.x));
		conf.set("dest_y", String.valueOf(destination.y));
		conf.set("dep_x", String.valueOf(departure.x));
		conf.set("dep_y", String.valueOf(departure.y));
		conf.set("dest_id", destid);
		conf.set("dep_id", depid);

		Job job = new Job(conf, "paths clustering optimized: " + depid + "_"
				+ destid); // construct a conf object called word count
		job.setJarByClass(Routeclustering_optimized.class); // set the main
															// class
		job.setMapperClass(regularization_map.class); // set mapper class
		job.setReducerClass(clustering_reduce.class); // set reducer class
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(vector_withid.class);
		job.setOutputKeyClass(IntWritable.class); // set the type of output key
		job.setOutputValueClass(Text.class); // set the type of output value
		job.setNumReduceTasks(7);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1].replace("_"+destid, ""))); // set the
																	// input
																	// path
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]+
				 depid + "_" + destid));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
