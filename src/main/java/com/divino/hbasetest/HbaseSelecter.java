package com.divino.hbasetest;
/*
 * Need Packages:
 * commons-codec-1.4.jar
 *
 * commons-logging-1.1.1.jar
 *
 * hadoop-0.20.2-core.jar
 *
 * hbase-0.90.2.jar
 *
 * log4j-1.2.16.jar
 *
 * zookeeper-3.3.2.jar
 *
 */

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseSelecter
{
    public static Configuration configuration = null;
    
    static {
        configuration = HBaseConfiguration.create();
        //configuration.set("hbase.master", "192.168.0.201:60000");
        configuration.set("hbase.zookeeper.quorum", "192.168.142.131"); 
        //configuration.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static void selectRowKey(String tableName, String rowKey) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result r = table.get(get);

        for (Cell cell : r.listCells()) { 
        	System.out.println(MessageFormat.format("{0}\t\t\t\t{1}:{2}\t\t\t\t{3}", new String(cell.getRow()), new String(cell.getFamily()), new String(cell.getQualifier()), new String(cell.getValue())));
        }
    }

    public static void selectRowKeyFamily(String tableName, String rowKey, String family) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addFamily(Bytes.toBytes(family));
        Result r = table.get(get);
        
        for (Cell cell : r.listCells()) { 
        	System.out.println(MessageFormat.format("{0}\t\t\t\t{1}:{2}\t\t\t\t{3}", new String(cell.getRow()), new String(cell.getFamily()), new String(cell.getQualifier()), new String(cell.getValue())));
        }
    }

    public static void selectRowKeyFamilyColumn(String tableName, String rowKey, String family, String column) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(family.getBytes(), column.getBytes());
        Result r = table.get(get);

        for (Cell cell : r.listCells()) { 
            System.out.println(MessageFormat.format("{0}\t\t\t\t{1}:{2}\t\t\t\t{3}", new String(cell.getRow()), new String(cell.getFamily()), new String(cell.getQualifier()), new String(cell.getValue())));
        } 
    }

    public static void selectFilter(String tableName, List<String> arr) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();// 实例化一个遍历器
        FilterList filterList = new FilterList(); // 过滤器List

        for (String v : arr) { // 下标0为列簇，1为列名，3为条件
            String[] wheres = v.split(",");

    		// 过滤器各个条件之间是" and "的关系
            filterList.addFilter(
            		new SingleColumnValueFilter(wheres[0].getBytes(), wheres[1].getBytes(), CompareOp.EQUAL, wheres[2].getBytes())
            );
        }
        
        scan.setFilter(filterList);
        ResultScanner rs = table.getScanner(scan);
        
        System.out.println("ROW\t\t\t\tFAMILY:COLUMN\t\t\t\tVALUE");
        System.out.println("--------------------------------------------------------------------------------------------------");
        for (Result r : rs) { 
            for (Cell cell : r.listCells()) { 
                System.out.println(MessageFormat.format("{0}\t\t\t\t{1}:{2}\t\t\t\t{3}", new String(cell.getRow()), new String(cell.getFamily()), new String(cell.getQualifier()), new String(cell.getValue())));
            } 
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.out.println("Usage: HbaseSelecter table key");
            System.exit(-1);
        }

        System.out.println("Table: " + args[0] + " , key: " + args[1]);
        selectRowKey(args[0], args[1]);

        /*
        System.out.println("------------------------行键  查询----------------------------------");
        selectRowKey("b2c", "yihaodian1002865");
        selectRowKey("b2c", "yihaodian1003396");

        System.out.println("------------------------行键+列簇 查询----------------------------------");
        selectRowKeyFamily("riapguh", "用户A", "user");
        selectRowKeyFamily("riapguh", "用户B", "user");

        System.out.println("------------------------行键+列簇+列名 查询----------------------------------");
        selectRowKeyFamilyColumn("riapguh", "用户A", "user", "user_code");
        selectRowKeyFamilyColumn("riapguh", "用户B", "user", "user_code");

        System.out.println("------------------------条件 查询----------------------------------");
        List<String> arr = new ArrayList<String>();
        arr.add("dpt,dpt_code,d_001");
        arr.add("user,user_code,u_0001");
        selectFilter("riapguh", arr);
        */
    }
}