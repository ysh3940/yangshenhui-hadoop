package indi.yangshenhui.hadoop.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

/**
 * 
 * @author yangshenhui
 */
public class HBaseIndexBuilderMapReduce {
	public static String column;

	public static class Map extends TableMapper<ImmutableBytesWritable, Put> {
		private Configuration configuration;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			configuration = context.getConfiguration();
			column = configuration.get("column");
		}

		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
			byte[] indexRowKey = null;
			String[] columns = StringUtils.split(column, ",");
			for (String c : columns) {
				if (StringUtils.contains(c, ":")) {
					String[] cs = StringUtils.split(c, ":");
					if (indexRowKey == null) {
						indexRowKey = value.getValue(Bytes.toBytes(cs[0]),
								Bytes.toBytes(cs[1]));
						indexRowKey = Bytes
								.add(indexRowKey, Bytes.toBytes("_"));
					} else {
						indexRowKey = Bytes.add(
								indexRowKey,
								value.getValue(Bytes.toBytes(cs[0]),
										Bytes.toBytes(cs[1])));
						indexRowKey = Bytes
								.add(indexRowKey, Bytes.toBytes("_"));
					}
				} else {
					System.out.println("[" + c
							+ "]参数格式错误,例如:family:column,列族名:列名");
				}
			}
			Put put = new Put(Bytes.add(indexRowKey, row.get()));
			put.add(Bytes.toBytes("f"), Bytes.toBytes("c"), row.get());
			context.write(new ImmutableBytesWritable(indexRowKey), put);
		}

	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd hh:mm:ss");
		if (args.length < 2) {
			System.out.println(simpleDateFormat.format(new Date())
					+ " start command such as [./hadoop -jar "
					+ HBaseIndexBuilderMapReduce.class.getName()
					+ ".jar table family:column,family:column-random 1000]");
			System.out.println(simpleDateFormat.format(new Date()) + " table"
					+ " ==> " + "表名");
			System.out
					.println(simpleDateFormat.format(new Date())
							+ " family:column,family:column-random"
							+ " ==> "
							+ "需要做索引的列,列族名:列名,多个用逗号隔开,单个列则做单列的索引,对与多个列则做组合索引(family:column,family:column-random 说明：column在前面则索引表rowKey的第一个列是column)");
			System.out.println(simpleDateFormat.format(new Date()) + " 1000"
					+ " ==> " + "一次从HBase表读取1000条数据,默认为1000");
			return;
		}
		String tableName = args[0];
		StringBuilder indexTableName = new StringBuilder();
		indexTableName.append(tableName).append("_");

		Scan scan = new Scan();
		int batch = 1000;
		try {
			batch = Integer.parseInt(args[2]);
		} catch (Exception e) {
			System.out.println("[" + args[2]
					+ "]参数转换错误,次参数必须是整数,例如:1000,默认为1000");
		}
		scan.setCaching(batch);
		scan.setCacheBlocks(false);
		scan.setMaxVersions(1);

		String[] columns = StringUtils.split(args[1], ",");
		for (String c : columns) {
			if (StringUtils.contains(c, ":")) {
				String[] cs = StringUtils.split(c, ":");
				scan.addColumn(Bytes.toBytes(cs[0]), Bytes.toBytes(cs[1]));
				indexTableName.append(cs[0]).append("-").append(cs[1])
						.append("_");
			} else {
				System.out.println("[" + c + "]参数格式错误,例如:family:column,列族名:列名");
			}
		}
		indexTableName.append("index");

		Configuration configuration = HBaseConfiguration.create();

		configuration.set("column", args[1]);

		HBaseIndexBuilderMapReduce hBaseIndexBuilderMapReduce = new HBaseIndexBuilderMapReduce();
		if (hBaseIndexBuilderMapReduce.createIndexTable(
				indexTableName.toString(), configuration)) {
			System.out.println(simpleDateFormat.format(new Date()) + " 创建索引表["
					+ indexTableName.toString() + "]成功");
		} else {
			System.out.println(simpleDateFormat.format(new Date()) + " 创建索引表["
					+ indexTableName.toString() + "]失败");
			return;
		}

		Job job = Job.getInstance(configuration,
				HBaseIndexBuilderMapReduce.class.getName());
		job.setJarByClass(HBaseIndexBuilderMapReduce.class);

		TableMapReduceUtil.initTableMapperJob(tableName, scan, Map.class,
				ImmutableBytesWritable.class, Put.class, job);
		TableMapReduceUtil.initTableReducerJob(indexTableName.toString(), null,
				job);
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private boolean createIndexTable(String indexTableName,
			Configuration configuration) {
		if (StringUtils.isEmpty(indexTableName)) {
			return false;
		}
		HBaseAdmin hBaseAdmin = null;
		try {
			hBaseAdmin = new HBaseAdmin(configuration);
			if (hBaseAdmin.tableExists(indexTableName.toString())) {
				return true;
			}
			HTableDescriptor hTableDescriptor = new HTableDescriptor(
					indexTableName.toString());
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("f");
			hColumnDescriptor.setCompressionType(Algorithm.LZO);
			hColumnDescriptor.setMaxVersions(1);
			hColumnDescriptor.setTimeToLive(Integer.MAX_VALUE);
			hTableDescriptor.addFamily(hColumnDescriptor);
			hBaseAdmin.createTable(hTableDescriptor);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			if (hBaseAdmin != null) {
				try {
					hBaseAdmin.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return true;
	}

}