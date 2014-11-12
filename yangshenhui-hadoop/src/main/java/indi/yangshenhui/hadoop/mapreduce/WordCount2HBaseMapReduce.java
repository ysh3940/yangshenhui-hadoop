package indi.yangshenhui.hadoop.mapreduce;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

/**
 * 
 * @author yangshenhui
 */
public class WordCount2HBaseMapReduce {
	private static final String FAMILY = "f";
	private static final String COLUMN = "c";

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws java.io.IOException, InterruptedException {
			String line = value.toString();
			IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(line),
					false);
			Lexeme lexeme = null;
			while ((lexeme = ikSegmenter.next()) != null) {
				word.set(lexeme.getLexemeText());
				context.write(word, one);
			}
		};
	}

	public static class Reduce extends
			TableReducer<Text, IntWritable, NullWritable> {
		protected void reduce(
				Text key,
				java.lang.Iterable<IntWritable> values,
				org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, NullWritable, org.apache.hadoop.io.Writable>.Context context)
				throws java.io.IOException, InterruptedException {
			int sum = 0;
			for (IntWritable intWritable : values) {
				sum += intWritable.get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUMN),
					Bytes.toBytes(String.valueOf(sum)));
			context.write(NullWritable.get(), put);
		};
	}

	public static void createHBaseTable(String tableName) throws IOException {
		HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(FAMILY);
		hTableDescriptor.addFamily(hColumnDescriptor);
		Configuration configuration = HBaseConfiguration.create();
		HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
		if (hBaseAdmin.tableExists(tableName)) {
			if (hBaseAdmin.isTableDisabled(tableName)) {
				hBaseAdmin.deleteTable(tableName);
			} else {
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
			}
		}
		hBaseAdmin.createTable(hTableDescriptor);
		try {
			if (hBaseAdmin != null) {
				hBaseAdmin.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		String tableName = "wordcount";
		Configuration configuration = HBaseConfiguration.create();
		configuration.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		createHBaseTable(tableName);
		String input = args[0];

		Job job = Job.getInstance(configuration,
				WordCount2HBaseMapReduce.class.getName());
		job.setJarByClass(WordCount2HBaseMapReduce.class);
		job.setNumReduceTasks(1);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}