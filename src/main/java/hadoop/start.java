package hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class start {

    public static Configuration configuration;  
    static {  
        configuration = HBaseConfiguration.create();  
        configuration.set("hbase.zookeeper.property.clientPort", "2181");  
        configuration.set("hbase.zookeeper.quorum", "1.9.96.104");  
        configuration.set("hbase.master", "1.9.96.104:6000");  

    }
 
//    
//    public static Configuration conf;
//    public static HBaseAdmin hBaseAdmin;
//    
//    public static void createTable(String tableName) {  
//        System.out.println("start create table ......");  
//        try {  
//            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);  
//            if (hBaseAdmin.tableExists(tableName)) {// ???????????????????  
//                hBaseAdmin.disableTable(tableName);  
//                hBaseAdmin.deleteTable(tableName);  
//                System.out.println(tableName + " is exist,detele....");  
//            }  
//            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);  
//            tableDescriptor.addFamily(new HColumnDescriptor("column1"));  
//            tableDescriptor.addFamily(new HColumnDescriptor("column2"));  
//            tableDescriptor.addFamily(new HColumnDescriptor("column3"));  
//            hBaseAdmin.createTable(tableDescriptor);
//            //ArrayList<String> lsTables=hBaseAdmin.listTables().toString();
//            HTable table = new HTable(configuration, "vadimsnSchemas");
//            Get g = new Get(Bytes.toBytes("raw###SprintSessionVOICERawData###9223090561878065151"));
//            Result result = table.get(g);
//            //byte [] value = result.getValue(Bytes.toBytes("SprintSessionWeb"),Bytes.toBytes(""));
//            byte [] value = result.getValue(Bytes.toBytes("key"),Bytes.toBytes("CCIHentity###AssignedEvent###9223090561878065151"));
//            String name = Bytes.toString(value);
//            
//            System.out.println(name);
////        } catch (MasterNotRunningException e) {  
////            e.printStackTrace();  
////        } catch (ZooKeeperConnectionException e) {  
////            e.printStackTrace();  
//        } 
//        catch (IOException e) {  
//            e.printStackTrace();  
//        }  
//        System.out.println("end create table ......");  
//    }
//    
//    public static void printRow() throws MasterNotRunningException, IOException {
//    	HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration); 
//    	// Getting all the list of tables using HBaseAdmin object
//    	HTable hTable = new HTable(configuration, "vadimsnSchemas");
//    	
//    	Get g = new Get(Bytes.toBytes("raw###SprintSessionVOICERawData###9223090561878065151"));
//        // printing all the table names.
//        // Reading the data
//        Result result = hTable.get(g);
//
//        // Reading values from Result class object
//        byte [] value = result.getValue(Bytes.toBytes("d"),Bytes.toBytes("content"));
//
//        // Printing the values
//        String name = Bytes.toString(value);
//    	System.out.println(name);
//
//    }
//    
//    public static void printColumn() throws MasterNotRunningException, IOException {
//    	HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration); 
//    	// Getting all the list of tables using HBaseAdmin object
//    	HTable hTable = new HTable(configuration, "vadimsnSchemas");
//
//    	
//    	// Instantiating the Scan class
//        Scan scan = new Scan();
//        scan.setCaching(2000);
//        
//        // Scanning the required columns
//        scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("name"));
//        scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("content"));
//
//        // Getting the scan result
//        ResultScanner scanner = hTable.getScanner(scan);
//        int i=1;
//        // Reading values from scan result
//        for (Result result = scanner.next(); result != null; result = scanner.next()){
//        	byte [] value = result.getValue(Bytes.toBytes("d"),Bytes.toBytes("content"));
//        	
//        	//System.out.println("Found row "+i+" : " + result);
//        	System.out.println("-----------------------------------------");
//        	System.out.println("Key: "+Bytes.toString(result.getRow()));
//        	System.out.println("Name: "+Bytes.toString(value));
//        	
//
//        	i++;
//        }
//        scanner.close();
//    }
//
//    public static void printTablesData() throws MasterNotRunningException, IOException {
//    	HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration); 
//    	// Getting all the list of tables using HBaseAdmin object
//        HTableDescriptor[] tableDescriptor = hBaseAdmin.listTables();
//        
//        //HTable hTable = new HTable(configuration, "vadimsnSchemas");
//        
//        // printing all the table names.
//        for (int i=0; i<tableDescriptor.length;i++ ){
//        	System.out.println("---------------------------------- TABLE ----------------------------------");
//        	System.out.println(tableDescriptor[i].getNameAsString());
//        	HTable hTable = new HTable(configuration, tableDescriptor[i].getNameAsString());
//        	//HTable hTable = new HTable(configuration, "BIMonitoring");
//            
//            Scan scan = new Scan();
//            scan.setCaching(20000);
//            // Scanning the required columns
//            //scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("name"));
//            //scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("content"));
//
//            // Getting the scan result
//            ResultScanner scanner = hTable.getScanner(scan);
//            int k=1;
//            // Reading values from scan result
//            try {
//                for (Result result = scanner.next(); result != null; result = scanner.next()){
//                	System.out.println(k+" Key: "+Bytes.toString(result.getRow()));
//                    for(KeyValue kv : result.raw()){
//                        //System.out.print(new String(kv.getRow()) + " ");
//                        //System.out.print(new String(kv.getFamily()) + ":");
//                        System.out.print(new String(kv.getQualifier()) + " ");
//                        //System.out.print(kv.getTimestamp() + " ");
//                        System.out.println(new String(kv.getValue()));
//                     }
//                	k++;
//                }
//            } finally {
//                scanner.close();  
//                hTable.close();
//            }
//        }
//    }
//    
//   
//    public static void printSchemas(String hBaseIP, String port, String tenantName,String tableName) throws MasterNotRunningException, IOException {
//        conf = HBaseConfiguration.create();  
//        conf.set("hbase.zookeeper.property.clientPort", port);  
//        conf.set("hbase.zookeeper.quorum", hBaseIP);  
//        String defPort="6000";
//        conf.set("hbase.master", hBaseIP+":"+defPort);
//        //conf.setInt("timeout", 900000);
//        
//    	hBaseAdmin = new HBaseAdmin(conf); 
//    	// Getting all the list of tables using HBaseAdmin object
//        HTableDescriptor[] tableDescriptor = hBaseAdmin.listTables();
//        
//        //HTable hTable = new HTable(configuration, "vadimsnSchemas");
//   	
//    	HTable hTable = new HTable(conf, tenantName+tableName);
//    	//HTable hTable = new HTable(configuration, "BIMonitoring");
//        
//        Scan scan = new Scan();
//        scan.setCaching(20000);
//        // Scanning the required columns
//        scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("name"));
//        scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("content"));
//
//        // Getting the scan result
//        ResultScanner scanner = hTable.getScanner(scan);
//        int k=1;
//        // Reading values from scan result
//        try {
//            for (Result result = scanner.next(); result != null; result = scanner.next()){
//            	System.out.println(k+" Key: "+Bytes.toString(result.getRow()));
//                for(KeyValue kv : result.raw()) {
//                    //System.out.print(new String(kv.getRow()) + " ");
//                    //System.out.print(new String(kv.getFamily()) + ":");
//                    System.out.print(new String(kv.getQualifier()) + " ");
//                    //System.out.print(kv.getTimestamp() + " ");
//                    System.out.println(new String(kv.getValue()));
//                 }
//            	k++;
//            }
//        } finally {
//            scanner.close();  
//            hTable.close();
//        }
//    }
//
//    public static void printTables(HBaseAdmin hBaseAdm) throws MasterNotRunningException, IOException {
//    	 
//    	// Getting all the list of tables using HBaseAdmin object
//        HTableDescriptor[] tableDescriptor = hBaseAdm.listTables();
//
//        // printing all the table names.
//        for (int i=0; i<tableDescriptor.length;i++ ){
//           System.out.println(tableDescriptor[i].getNameAsString());
//        }
//    }
    
	public static void main(String[] args) throws IOException  {
		// Create a connection to the cluster.
//		HConnection connection = HConnectionManager.createConnection(configuration);
		//HTableInterface table = connection.getTable("myTable");
		
//		System.out.println(connection.listTables());
		
		// use table as needed, the table returned is lightweight
		//table.close();
		// use the connection for other access to the cluster
//		connection.close();
		
		
		System.out.println("Start");
        Configuration conf = HBaseConfiguration.create();
        conf = HBaseConfiguration.create();  
        conf.set("hbase.zookeeper.property.clientPort", "2181");  
        conf.set("hbase.zookeeper.quorum", "1.9.96.104");  
        conf.set("hbase.master", "1.9.96.104:6000");
        try {           
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin hAdmin = conn.getAdmin();
            //hAdmin.listTableNames();
            TableName tableName=TableName.valueOf("Customer");
            if (hAdmin.isTableAvailable(tableName)) {
//            	hAdmin.disableTable(tableName);
//            	hAdmin.deleteTable(tableName);

            } else {
                HTableDescriptor hTableDesc = new HTableDescriptor(tableName);
                
                hTableDesc.addFamily(new HColumnDescriptor("personal"));
                hTableDesc.addFamily(new HColumnDescriptor("school"));
//                hTableDesc.addFamily(new HColumnDescriptor("address"));

                hAdmin.createTable(hTableDesc);
                
                System.out.println("Table created Successfully...");
             }

            Table hTable = conn.getTable(tableName);
            Date date =new Date();
            SimpleDateFormat formatter;

            formatter = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z");
            String timestamp = formatter.format(date);
            
            Put p = new Put(Bytes.toBytes(String.valueOf(date.getTime())));
            // adding values using add() method
            // accepts column family name, qualifier/row name ,value
            p.addColumn(Bytes.toBytes("personal"),
            Bytes.toBytes("photo"),Bytes.toBytes("my photo"));
            
            p.addColumn(Bytes.toBytes("personal"),
            Bytes.toBytes("name"),Bytes.toBytes("myName"));
            
            p.addColumn(Bytes.toBytes("personal"),
            Bytes.toBytes("surname"),Bytes.toBytes("mySurname"));

            p.addColumn(Bytes.toBytes("personal"),
            Bytes.toBytes("patronymic"),Bytes.toBytes("myFatherName"));
            
            p.addColumn(Bytes.toBytes("personal"),
            Bytes.toBytes("birthday"),Bytes.toBytes("yyyy.MM.dd"));
            
            p.addColumn(Bytes.toBytes("personal"),
            Bytes.toBytes("phone"),Bytes.toBytes("+38097XXXXXXX"));
            
            p.addColumn(Bytes.toBytes("personal"),
            Bytes.toBytes("alias"),Bytes.toBytes("nameSurname"));
            
            p.addColumn(Bytes.toBytes("school"),
            Bytes.toBytes("discipline"),Bytes.toBytes("math"));

            p.addColumn(Bytes.toBytes("school"),
            Bytes.toBytes("them"),Bytes.toBytes("5-Â"));
            
            p.addColumn(Bytes.toBytes("school"),
            Bytes.toBytes("mark"),Bytes.toBytes("12"));
            
            p.addColumn(Bytes.toBytes("school"),
            Bytes.toBytes("class"),Bytes.toBytes("5-Â"));
            
            p.addColumn(Bytes.toBytes("school"),
            Bytes.toBytes("date"),Bytes.toBytes(timestamp));


            // Saving the put Instance to the HTable.
            hTable.put(p);
            
            System.out.println("data inserted");
             
            //hAdmin.addColumn(tableName, new HColumnDescriptor("address1"));
            
            for(TableName t:hAdmin.listTableNames()) {
            	System.out.println(t.getNameAsString());
            }
            
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}
