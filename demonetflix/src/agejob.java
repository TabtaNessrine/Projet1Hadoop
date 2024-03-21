

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class agejob {

	public static void main(String[] args) throws Exception {
		 
	 	Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "netflix");
	    job.setJarByClass(agejob.class);
	    job.setMapperClass(agemapper.class);
	    job.setReducerClass(agereducer.class);
	    
	    //userid,age,dob_day,dob_year,dob_month,gender,occupation,friend_count,friendships_initiated,likes,likes_received,mobile_likes,mobile_likes_received,www_likes,www_likes_received
	    //2094382,14,19,1999,11,male,266,0,0,0,0,0,0,0,0
	    
	    job.setOutputKeyClass(Text.class);  //key: intWritable
	    job.setOutputValueClass(IntWritable.class); //value:  intWritable
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    
	    Path outputPath=new Path(args[1]);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    outputPath.getFileSystem(conf).delete(outputPath, true);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

  public static class agemapper
       extends Mapper<Object, Text, Text, IntWritable>{ //les deux derniers: key, value

    private IntWritable mobileLikes = new IntWritable(); //value
    
    private Text trancheAge= new Text(); //key
    private int ages; //tableau[1]

    public void map(Object key, Text value, Context context //text value: element de notre base de donnee.txt
                    ) throws IOException, InterruptedException {
      String ligne= value.toString();
      if(!ligne.contains("userid")) {
    	  String[] tableau = value.toString().split(",");

    	  
    	  //verifie
    	  ages= Integer.parseInt(tableau[1]);
    	  if(ages <= 12) {
    		  trancheAge.set("enfant");
    		  //utiliser set pour HADOOP : text, intwritable, floatwritable
    	  }else if (ages > 12 && ages <= 18 ) {
    		  trancheAge.set("adolescent");
    	  }else if(ages > 18 && ages <= 65 ) {
    		  trancheAge.set("Adulte");
    	  }else if(ages > 65 ) {
    		  trancheAge.set("boomer");
    	  }
    	  mobileLikes.set(Integer.parseInt(tableau[11]));
  
      }
      context.write(trancheAge, mobileLikes); //write ecrit dans disque dur key et value
      
    }
  }

  public static class agereducer
       extends Reducer<Text,IntWritable,Text,IntWritable> { //deux derniers: key,value
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      int i=0;
      for (IntWritable val : values) {
        sum += val.get();
        i++;
      }
      result.set(sum/i);
      context.write(key, result);
    }
  }

 

}
