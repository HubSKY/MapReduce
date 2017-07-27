import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Hw1Grp2 {
	public static Configuration configurationhbase = HBaseConfiguration
			.create();// the object of configuration in hbase
	public static String tableName = "Result";// the name of table
	public static String columnFamily = "res";// the name of column family

	/**
	 * it a function which is to judge whether a character string is a
	 * numerical. args:string
	 */
	public static boolean isNumeric(String str) {
		try {
			Double num2 = Double.valueOf(str);//
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * h the function of get x from RX , to define which row is to calculate;
	 * 
	 */
	public static int getRow(String rowString) {
		int row = 0;
		int index = 0;
		index = rowString.indexOf("R");
		row = Integer.valueOf(rowString.substring(index + 1).toString());
		return row;
	}

	/**
	 * the function of creating a table in hbase
	 * 
	 */
	public static void createTable(String tableName) {

		System.out.println("starting creating the table ...");

		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(configurationhbase);
			if (hBaseAdmin.tableExists(tableName)) {// whether the table is
													// existed
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);// if existed , drop the table
				System.out.println(tableName
						+ " existed, and delete it successfully");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

			tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
			hBaseAdmin.createTable(tableDescriptor);// create the new table
			hBaseAdmin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("finished creating the table of Result!");// print
																		// tip
																		// toshow
																		// the
																		// table
																		// has
																		// been
																		// created
	}

	/**
	 * the function of storing data into hbase args: the result of intermediate
	 * handling,type: Map<String, List>
	 */
	public static void storInHbase(Map<String, List> hashMap)//
			throws IOException, ZooKeeperConnectionException, IOException {
		createTable(tableName);
		int i = 0;
		HTable table = new HTable(configurationhbase, tableName);
		List<Put> putsList = new ArrayList();
		System.out.println("total:" + hashMap.size() + " datas");
		Iterator iter = hashMap.entrySet().iterator();
		System.out.println("storing in hbase ...!");

		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = entry.getKey().toString();
			List<String> listValue = new ArrayList();
			listValue = (List) entry.getValue();
			for (int j = 0; j < listValue.size(); j++) {
				Put put1 = new Put(Bytes.toBytes(key));// appoint the row
				String tempStr = listValue.get(j).toString();
				String[] contentItems = tempStr.split(":");
				switch (contentItems[0]) {
				case "count": {
					int count = Integer.parseInt(contentItems[2].trim());
					put1.add(Bytes.toBytes(columnFamily),
							Bytes.toBytes("count"),
							Bytes.toBytes(String.valueOf(count)));

				}
					break;
				case "avg": {
					String avg = contentItems[2].trim();
					put1.add(
							Bytes.toBytes(columnFamily),
							Bytes.toBytes("avg(R" + contentItems[1].trim()
									+ ")"), Bytes.toBytes(String.valueOf(avg)));
				}
					break;
				case "sum": {
					int sum = Integer.parseInt(contentItems[2].trim());
					put1.add(
							Bytes.toBytes(columnFamily),
							Bytes.toBytes("sum(R" + contentItems[1].trim()
									+ ")"), Bytes.toBytes(String.valueOf(sum)));

				}
					;
					break;
				case "max": {
					int max = Integer.parseInt(contentItems[2].trim());

					put1.add(
							Bytes.toBytes(columnFamily),
							Bytes.toBytes("max(R" + contentItems[1].trim()
									+ ")"), Bytes.toBytes(String.valueOf(max)));
				}
					;
					break;
				}
				putsList.add(put1);
			}
		}
		table.put(putsList);
		table.close();
		System.out.println("finished!");

	}

	/**
	 * the function of sorting the final result and output the result args: the
	 * result of intermediate handling,type: Map<String, List>
	 */
	public static void sortOutput(Map<String, List> hashMap) {
		Set<String> keySet = hashMap.keySet();// to get the key set
		List listTmp = new ArrayList();
		for (String tmp : keySet) {
			listTmp.add(tmp);
		}
		Collections.sort(listTmp);// sorting the keyset
		for (int i = 0; i < listTmp.size(); i++) {// loop through the hasmap and
													// output the key-value
			String key = listTmp.get(i).toString();// get the key
			List list = hashMap.get(key);
			String outPutString = key + "  ";
			for (int j = 0; j < list.size(); j++) {
				String tempStr = list.get(j).toString();
				String[] contentItems = tempStr.split(":");

				outPutString = outPutString + "   " + contentItems[0].trim()
						+ " R" + contentItems[1].trim() + ":"
						+ contentItems[2].trim() + "   ";
			}
			System.out.println(outPutString);
		}

	}

	/**
	 * the main function args:to store the variables pass through command line
	 * 
	 */
	public static void main(String args[]) throws IOException {
		String fileName = null;// to store the file name
		String operatorValueString = null;
		int groupbyKey = 0;
		boolean check = false;
		List outPutStringList = new ArrayList();
		List outPutInt = new ArrayList();
		try {
			int index = args[0].toString().indexOf("=");
			fileName = args[0].toString().substring(index + 1);
			System.out.println(fileName);

			index = args[1].toString().indexOf(":");
			groupbyKey = getRow(args[1].toString().substring(index + 1));
			System.out.println("groupByRow:R" + groupbyKey);

			index = args[2].toString().indexOf(":");

			String outputRow = args[2].toString().substring(index + 1);
			if (outputRow.contains("count")) {
				// outPutRowMap.put("count", groupbyKey);
				outPutStringList.add("count");
				outPutInt.add(groupbyKey);
			}
			String[] items = outputRow.split(",");
			for (int i = 0; i < items.length; i++) {
				if (items[i].contains("R")) {
					index = items[i].indexOf("R");
					int index2 = items[i].indexOf(")");
					int rowNumber = Integer.parseInt(items[i].substring(
							index + 1, index2));

					if (items[i].contains("avg")) {
						outPutStringList.add("avg");
						outPutInt.add(rowNumber);
					}
					if (items[i].contains("max")) {
						// outPutRowMap.put("max", rowNumber);
						outPutStringList.add("max");
						outPutInt.add(rowNumber);
					}
					if (items[i].contains("sum")) {
						// outPutRowMap.put("sum", rowNumber);
						outPutStringList.add("sum");
						outPutInt.add(rowNumber);
					}
				}

			}
			System.out.println("outputRow:" + outputRow);
			for (int i = 0; i < outPutInt.size(); i++) {
				System.out.println(outPutStringList.get(i) + " R:"
						+ outPutInt.get(i));
			}
		} catch (ArrayIndexOutOfBoundsException e) {// to handle the exception
													// of array index out of
													// bonds
			// TODO: handle exception
			System.out
					.println("please pass the variables in the form of \"java Hw1Grp2 R=/hw1/lineitem.tbl groupby:R2 res:count,avg'('R5')',max'('R4')'\"");// to
																																							// correct
																																							// reference
			System.exit(0);// terminate the program
		} catch (Exception e) {
			// TODO: handle exception
			System.out
					.println("please pass the variables in the form of \"java Hw1Grp2 R=/hw1/lineitem.tbl groupby:R2 res:count,avg'('R5')',max'('R4')'\"");// to
																																							// correct
																																							// reference
			System.exit(0);// terminate the program
		}

		String file = "hdfs://localhost:9000" + fileName;// to joint the adress
															// of the file with
															// the filename

		Configuration conf = new Configuration();// build new object of
													// configuration
		FileSystem fs = null;// file IO stream
		try {
			fs = FileSystem.get(URI.create(file), conf);// to configure the
														// information of file
														// IO
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Path path = new Path(file);// build new path parameter to store the path
		FSDataInputStream in_stream = fs.open(path);// open the file IO

		BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));// build
																					// a
																					// buffer
		String contentString;// to store one recored read from the file;
		String items[];// to store the data cut off with the mark "|"
		DecimalFormat df = new DecimalFormat("######0.00");// to define the
															// format of
															// doubletype, keep
															// two decimal
															// palces
		Map<String, List> hashMap = new HashMap<String, List>();// store //
																// calculate the
																// avg;
		List<String> list = new ArrayList();// store the data

		while ((contentString = in.readLine()) != null) {// loop reading the
															// content file one
															// by one
			// System.out.println(contentString);// output the content of the
			// file
			items = contentString.split("\\|");// store the split items
			String key = items[groupbyKey];
			System.out.println(key);

			if (!check) {
				for (int i = 0; i < outPutInt.size(); i++) {
					if (outPutStringList.get(i).equals("count"))
						continue;
					if (!isNumeric(items[(int) outPutInt.get(i)].trim())) {
						System.out
								.println("the column R"
										+ outPutInt.get(i)
										+ " you chose is not numerical,please choose another column.");
						System.exit(0);// terminate the program
					}

				}

			}

			if (hashMap.containsKey(key))// to judge whether the key is
											// contained, if contained,then
											// takethe list value and update
											// thecount,avg,max
			{
				List listtemp = hashMap.get(key);// get the list of the rowkey
				List<String> listNew = new ArrayList();// build a new list to
				for (int i = 0; i < listtemp.size(); i++) {
					String tempStr = listtemp.get(i).toString();
					String[] contentItems = tempStr.split(":");
					switch (contentItems[0]) {
					case "count": {
						int count = Integer.parseInt(contentItems[2].trim()) + 1;// to get foreign count and add one
						listNew.add("count:" + groupbyKey + ":" + count);

					}
						break;
					case "avg": {
						int rowNumber = Integer
								.parseInt(contentItems[1].trim());// to get the row number
																	
						double value1 = Double.parseDouble(items[rowNumber]
								.trim());// to get the new item to add
						int count = Integer.parseInt(contentItems[4].trim());
						count++;
						double sum = Double.parseDouble(contentItems[3].trim())
								+ value1;
						double avg = sum / count;
						listNew.add("avg:" + rowNumber + ":"
								+ String.valueOf(df.format(avg)) + ":"
								+ String.valueOf(sum) + ":" + count);
					}
						break;
					case "sum": {
						int rowNumber = Integer
								.parseInt(contentItems[1].trim());
						int value1 = Integer.parseInt(items[rowNumber].trim());// to get the new item to add
						int sum = Integer.parseInt(contentItems[2].trim())
								+ value1;
						listNew.add("sum:" + rowNumber + ":"
								+ String.valueOf(sum));

					}
						;
						break;
					case "max": {
						int rowNumber = Integer
								.parseInt(contentItems[1].trim());
						int value1 = Integer.parseInt(items[rowNumber].trim());// to
																				// get
																				// the
																				// new
																				// item
																				// to
																				// add
						int max = Integer.parseInt(contentItems[2].trim());
						if (value1 > max) {
							max = value1;
						}
						listNew.add("max:" + rowNumber + ":"
								+ String.valueOf(max));
					}
						;
						break;
					}

				}
				hashMap.put(key, listNew);// update the new list through key
			} else {// if not contained add new record in hasmap
				List listtemp1 = new ArrayList();

				for(int i=0;i<outPutInt.size();i++) {
					int rowNUmber = (int) outPutInt.get(i);
					String keyOutPutStr=String.valueOf(outPutStringList.get(i));
					switch (keyOutPutStr) {
					case "count":listtemp1.add("count:" + rowNUmber + ":"+ String.valueOf(1));
						break;
					case "avg": {
						double avgValue = Double.parseDouble(items[rowNUmber].trim());listtemp1.add("avg:" + rowNUmber + ":"+ String.valueOf(df.format(avgValue)) + ":"+ String.valueOf(avgValue) + ":" + 1);
					}
						break;
					case "sum": {
						int sumValue = Integer.parseInt(items[rowNUmber].trim());
						listtemp1.add("sum:" + rowNUmber + ":"+ String.valueOf(sumValue));

					}
						break;
					case "max": {
						int maxValue = Integer
								.parseInt(items[rowNUmber].trim());
						listtemp1.add("max:" + rowNUmber + ":"
								+ String.valueOf(maxValue));

					}
						break;
					}

				}

				hashMap.put(key, listtemp1);// add the key-value into the hasmap
			}

		}
		in.close();
		fs.close();// close the file IO

		storInHbase(hashMap);// call the storInHbase function
		sortOutput(hashMap);// call the sortOutPut function

	}
}

