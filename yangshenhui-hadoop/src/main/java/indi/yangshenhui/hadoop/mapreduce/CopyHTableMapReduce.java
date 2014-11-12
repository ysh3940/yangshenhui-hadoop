package indi.yangshenhui.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;

/**
 * CopyHTableMR
 * 
 * @author yangshenhui
 */
public class CopyHTableMapReduce {
	public static class Map extends TableMapper<ImmutableBytesWritable, Put> {

		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
			context.write(row, resultToPut(row, value));
		}

		private static Put resultToPut(ImmutableBytesWritable key, Result result)
				throws IOException {
			Put put = new Put(key.get());
			for (KeyValue kv : result.raw()) {
				put.add(kv);
			}
			return put;
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration configuration = HBaseConfiguration.create();
		Job job = Job.getInstance(configuration,
				CopyHTableMapReduce.class.getName());
		job.setJarByClass(CopyHTableMapReduce.class);

		Scan scan = new Scan();
		scan.setCaching(1000);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob("student", scan, Map.class,
				ImmutableBytesWritable.class, Put.class, job);
		TableMapReduceUtil.initTableReducerJob("cstudent", null, job);
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}