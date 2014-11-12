package indi.yangshenhui.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * 
 * @author yangshenhui
 */
public class ReadHTableMapReduce {
	public static class Map extends TableMapper<Text, Text> {
		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws InterruptedException, IOException {
			System.out.print("row = " + Bytes.toString(row.get()));
			System.out.print("\t");
			System.out.println(Bytes.toString(value.getValue(
					"information".getBytes(), "name".getBytes())));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration configuration = HBaseConfiguration.create();
		Job job = Job.getInstance(configuration,
				ReadHTableMapReduce.class.getName());
		job.setJarByClass(ReadHTableMapReduce.class);
		Scan scan = new Scan();
		scan.setCaching(1000);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob("student", scan, Map.class, null,
				null, job);
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}