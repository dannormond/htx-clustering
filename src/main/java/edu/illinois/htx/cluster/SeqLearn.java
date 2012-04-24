package edu.illinois.htx.cluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SeqLearn {
	public static void main(String[] args) {
		String uri = "seq-learn/first";
		Configuration conf = new Configuration();
		
		Path p = new Path(uri);
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		SequenceFile.Writer writer = null;
		
		try {
			FileSystem fs = FileSystem.get(conf);
			writer = SequenceFile.createWriter(fs, conf, p, key.getClass(), value.getClass());
			
			for (int i = 0; i < 1000; i++) {
				key.set(i%10);
				value.set("Test-" + i);
				writer.append(key, value);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			org.apache.hadoop.io.IOUtils.closeStream(writer);
		}
	}
}
