package edu.illinois.htx.cluster;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.vectorizer.DictionaryVectorizer;

public class TransactionLogTransformer {
	public static class TransactionLogMapper extends TableMapper<Text, Text> {
		@Override
		protected void map(ImmutableBytesWritable key, Result result, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) 
				throws java.io.IOException ,InterruptedException {
			System.out.println("Table Mapper for htx: " + key);
			Text keyOut = new Text();
			Text valueOut = new Text();
			for (KeyValue kv : result.raw()) {
				DataInputStream input = new DataInputStream(new ByteArrayInputStream(kv.getValue()));
				long sid = input.readLong();
				long tid = input.readLong();
				keyOut.set(kv.getQualifier());
				valueOut.set(String.valueOf(tid));
				
				System.out.println("\t" + keyOut + " : " + valueOut);

				context.write(keyOut, valueOut);
			}
		}
	}
	
	public static class TransactionLogReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text inKey, java.lang.Iterable<Text> inValues, Reducer<Text,Text,Text,Text>.Context context) 
				throws java.io.IOException ,InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text text: inValues)
			{
				sb.append(text.toString()).append(" ");
			}
			
			Text outValue = new Text(sb.toString());
			
			context.write(inKey, outValue);
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("Setting up job to transform the transaction log for clustering");
		Path outputPath = new Path("tx-tabledata");
		Configuration conf = new Configuration();
		Scan scan = new Scan();
		Job job = new Job(conf, "HtxTxLog");
		job.setJarByClass(TransactionLogTransformer.class);
		
		TableMapReduceUtil.initTableMapperJob(
				"htx",      // input table
				scan,	          // Scan instance to control CF and attribute selection
				TransactionLogMapper.class,   // mapper class
				Text.class,	          // mapper output key
				Text.class,	          // mapper output value
				job);
		job.setReducerClass(TransactionLogReducer.class);			
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);
		
		Path vectorPath = new Path("tx-vectordata");
		DictionaryVectorizer.createTermFrequencyVectors(
				outputPath, 
				vectorPath, 
				"final", 
				conf, 
				1, 
				1, 50, 2, true, 2, 100, true, false);
		
	}
}
