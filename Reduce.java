package tech;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public  class Reduce extends MapReduceBase
implements Reducer<Text, IntWritable, Text, IntWritable> {


	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException {
		System.out.println("In Reducer ");
		int sum = 0;
		String line = key.toString();
		StringTokenizer itr = new StringTokenizer(line);
		while (values.hasNext()) {
			sum += values.next().get();
		}
		output.collect(key, new IntWritable(sum));
		System.out.println("Reducer : "+key+" : "+sum);
		writeToFile(key+" "+sum);
		
		int index=Integer.parseInt(itr.nextToken());
		String value=itr.nextToken();
		String classLabel=itr.nextToken();
		int count=sum;

	}

	public static void writeToFile(String text) {
		System.out.println("Reducer :writeToFile ");
		try {

			C45 id=new C45();

			Path input = new Path("C45");
				Configuration conf = new Configuration();
			
			
			//Path intermediateInfo = new Path("C45/rule.txt");
//			 FileSystem fs = FileSystem.get(conf);
//			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("C45/intermediate"+id.current_index+".txt"), true)));
			//BufferedWriter bw = new BufferedWriter(new FileWriter(new File("/home/sreeveni/workspace/DecTr/C45/intermediate"+id.current_index+".txt"), true));    
				BufferedWriter bw = new BufferedWriter(new FileWriter(new File("C45/intermediate"+id.current_index+".txt"), true));    
				bw.write(text);
			bw.newLine();
			bw.close();
		} catch (Exception e) {
		}
	}

}
