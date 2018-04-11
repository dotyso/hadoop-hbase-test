package com.divino.hbasetest;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList; 
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration; 
import org.apache.hadoop.hbase.HColumnDescriptor; 
import org.apache.hadoop.hbase.HTableDescriptor; 
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete; 
import org.apache.hadoop.hbase.client.Get; 
import org.apache.hadoop.hbase.client.Put; 
import org.apache.hadoop.hbase.client.Result; 
import org.apache.hadoop.hbase.client.ResultScanner; 
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter; 
import org.apache.hadoop.hbase.filter.FilterList; 
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter; 
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp; 
import org.apache.hadoop.hbase.util.Bytes; 
 
public class HbaseTest { 
 
    public static Configuration configuration; 
    static { 
        configuration = HBaseConfiguration.create(); 
        configuration.set("hbase.zookeeper.property.clientPort", "2181"); 
        configuration.set("hbase.zookeeper.quorum", "192.168.142.131"); 
        //configuration.set("hbase.master", "192.168.142.131:60000"); 
    } 
 
    public static void main(String[] args) { 
        try {
        	//createTable("wujintao"); 
			//insertData("wujintao");

	        //QueryAll("wujintao"); 
	        // QueryByCondition1("wujintao"); 
	        // QueryByCondition2("wujintao"); 
	        //QueryByCondition3("wujintao"); 
	        //deleteRow("wujintao","abcdef"); 
	        // deleteByCondition("wujintao","abcdef"); 
        	
        	//new HbaseSelecter().selectRowKey("wujintao", "112233bbbcccc");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    } 
 
     
    public static void createTable(String tableName) { 
        System.out.println("start create table ......"); 
        try { 
       	
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin hBaseAdmin = connection.getAdmin();
            if (hBaseAdmin.tableExists(TableName.valueOf(tableName))) {// 如果存在要创建的表，那么先删除，再创建 
                hBaseAdmin.disableTable(TableName.valueOf(tableName)); 
                hBaseAdmin.deleteTable(TableName.valueOf(tableName)); 
                System.out.println(tableName + " is exist,detele...."); 
            } 
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName)); 
            tableDescriptor.addFamily(new HColumnDescriptor("column1")); 
            tableDescriptor.addFamily(new HColumnDescriptor("column2")); 
            tableDescriptor.addFamily(new HColumnDescriptor("column3")); 
            hBaseAdmin.createTable(tableDescriptor); 
        } catch (MasterNotRunningException e) { 
            e.printStackTrace(); 
        } catch (ZooKeeperConnectionException e) { 
            e.printStackTrace(); 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
        System.out.println("end create table ......"); 
    } 
 
     
    public static void insertData(String tableName) throws IOException { 
        System.out.println("start insert data ......"); 
        
        //ExecutorService executor = Executors.newFixedThreadPool(10);
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
        
        //HTablePool pool = new HTablePool(configuration, 1000); 
        //HTable table = (HTable) pool.getTable(tableName); 
        Put put = new Put("112233bbbcccc".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值 
        put.addColumn("column1".getBytes(), null, "aaa".getBytes());// 本行数据的第一列 
        put.addColumn("column2".getBytes(), null, "bbb".getBytes());// 本行数据的第三列 
        put.addColumn("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列 
        try { 
            table.put(put); 
        } catch (IOException e) { 
            e.printStackTrace(); 
	    } finally {
	        table.close();
	        connection.close();
	    }
        System.out.println("end insert data ......"); 
    } 
 
     
    public static void dropTable(String tableName) { 
        try { 
        	Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            admin.disableTable(TableName.valueOf(tableName)); 
            admin.deleteTable(TableName.valueOf(tableName)); 
        } catch (MasterNotRunningException e) { 
            e.printStackTrace(); 
        } catch (ZooKeeperConnectionException e) { 
            e.printStackTrace(); 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
 
    } 
     
     public static void deleteRow(String tablename, String rowkey)  { 
        try { 
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tablename));
            List list = new ArrayList(); 
            Delete d1 = new Delete(rowkey.getBytes()); 
            list.add(d1); 
             
            table.delete(list); 
            System.out.println("删除行成功!"); 
             
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
         
 
    } 
 
      
	public static void deleteByCondition(String tablename, String rowkey)  { 
            //目前还没有发现有效的API能够实现根据非rowkey的条件删除这个功能能，还有清空表全部数据的API操作 
 
	} 
 
 
     
    public static void QueryAll(String tableName) throws IOException { 
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
        try { 
            ResultScanner rs = table.getScanner(new Scan()); 
            
            System.out.println("ROW\t\t\t\tFAMILY:COLUMN\t\t\t\tVALUE");
            System.out.println("--------------------------------------------------------------------------------------------------");
            for (Result r : rs) { 
	            for (Cell cell : r.listCells()) { 
	                System.out.println(MessageFormat.format("{0}\t\t\t\t{1}:{2}\t\t\t\t{3}", new String(cell.getRow()), new String(cell.getFamily()), new String(cell.getQualifier()), new String(cell.getValue())));
	            } 
            }
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
    } 
 
     
    public static void QueryByCondition1(String tableName) throws IOException { 
 
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
        try { 
            Get scan = new Get("abcdef".getBytes());// 根据rowkey查询 
            Result r = table.get(scan); 
            System.out.println("获得到rowkey:" + new String(r.getRow())); 
            for (Cell cell : r.listCells()) { 
                System.out.println(MessageFormat.format("{0}\t\t\t\t{1}:{2}\t\t\t\t{3}", new String(cell.getRow()), new String(cell.getFamily()), new String(cell.getQualifier()), new String(cell.getValue())));
            } 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
    } 
 
     
    public static void QueryByCondition2(String tableName) { 
 
        try { 
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL, Bytes.toBytes("aaa")); // 当列column1的值为aaa时进行查询 
            Scan scan = new Scan(); 
            scan.setFilter(filter); 
            ResultScanner rs = table.getScanner(scan); 
            
            System.out.println("ROW\t\t\t\tFAMILY:COLUMN\t\t\t\tVALUE");
            System.out.println("--------------------------------------------------------------------------------------------------");
            for (Result r : rs) { 
	            for (Cell cell : r.listCells()) { 
	                System.out.println(MessageFormat.format("{0}\t\t\t\t{1}:{2}\t\t\t\t{3}", new String(cell.getRow()), new String(cell.getFamily()), new String(cell.getQualifier()), new String(cell.getValue())));
	            } 
            }
        } catch (Exception e) { 
            e.printStackTrace(); 
        } 
 
    } 
 
     
    public static void QueryByCondition3(String tableName) { 
 
        try { 
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));
 
            List<Filter> filters = new ArrayList<Filter>(); 
 
            Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL, Bytes.toBytes("aaa")); 
            filters.add(filter1); 
 
            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("column2"), null, CompareOp.EQUAL, Bytes.toBytes("bbb")); 
            filters.add(filter2); 
 
            Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("column3"), null, CompareOp.EQUAL, Bytes.toBytes("ccc")); 
            filters.add(filter3); 
 
            FilterList filterList1 = new FilterList(filters); 
 
            Scan scan = new Scan(); 
            scan.setFilter(filterList1); 
            ResultScanner rs = table.getScanner(scan); 
            
            System.out.println("ROW\t\t\t\tFAMILY:COLUMN\t\t\t\tVALUE");
            System.out.println("--------------------------------------------------------------------------------------------------");
            for (Result r : rs) { 
	            for (Cell cell : r.listCells()) { 
	                System.out.println(MessageFormat.format("{0}\t\t\t\t{1}:{2}\t\t\t\t{3}", new String(cell.getRow()), new String(cell.getFamily()), new String(cell.getQualifier()), new String(cell.getValue())));
	            } 
            }
            rs.close(); 
 
        } catch (Exception e) { 
            e.printStackTrace(); 
        } 
 
    } 
 
}