package com.SuperJDBC;
import java.sql.*;
import java.util.*;
import java.util.Map.*;
import java.io.*;

public class SuperJDBC {
	private static boolean Debug = false;
	private static boolean Mysql = false;
	private static boolean Oracle = false;
	private static boolean SqlServe = false;
	private static boolean DB2 = false;
	private static boolean sybase = false;
	private static boolean PostgreSQL = false;
	
	static{
		try{
			Class.forName("com.mysql.jdbc.Driver");
			Mysql = true;
		}catch(ClassNotFoundException e)
		{
			if(Debug)
			System.out.println("Mysql驱动加载失败");
		}
		
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Oracle = true;
		}catch(ClassNotFoundException e)
		{
			if(Debug)
			System.out.println("Oracle驱动加载失败");
		}
		
		try{
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			SqlServe = true;
		}catch(ClassNotFoundException e)
		{
			if(Debug)
			System.out.println("SqlServe驱动加载失败");
		}
		
		try{
			Class.forName("org.postgresql.Driver");
			PostgreSQL = true;
		}catch(ClassNotFoundException e)
		{
			if(Debug)
			System.out.println("PostgreSQL驱动加载失败");
		}
		
		try{
			Class.forName("com.ibm.db2.jcc.DB2Driver");
			DB2 = true;
		}catch(ClassNotFoundException e)
		{
			if(Debug)
			System.out.println("DB2驱动加载失败");
		}
		
		try{
			Class.forName("com.sybase.jdbc.SybDriver");
			sybase = true;
		}catch(ClassNotFoundException e)
		{
			if(Debug)
			System.out.println("sybase驱动加载失败");
		}
	}
	
	public static void DriverInfo()
	{
		System.out.println("Mysql:"+Mysql);
		System.out.println("Oracle:"+Oracle);
		System.out.println("SqlServe:"+SqlServe);
		System.out.println("PostgreSQL:"+PostgreSQL);
		System.out.println("sybase:"+sybase);
		System.out.println("DB2:"+DB2);
	}
	
	public static boolean Debug(boolean debug_q)
	{
		Debug=debug_q;
		return Debug;
	}
	
	public static Connection Connect(String type,String ip,String Port,String DB,String user,String password)
	{
		String allUrl=null;
		if(type.equalsIgnoreCase("mysql"))
			allUrl="jdbc:mysql://"+ip+":"+Port+"/"+DB;
		else if(type.equalsIgnoreCase("oracle"))
			allUrl="jdbc:oracle:thin:@"+ip+":"+Port+":"+DB;
		else if(type.equalsIgnoreCase("sqlserve"))
			allUrl="jdbc:sqlserve://"+ip+":"+Port+";DatabaseName="+DB;
		else if(type.equalsIgnoreCase("DB2"))
			allUrl="jdbc:db2://"+ip+":"+Port+"/"+DB;
		else if(type.equalsIgnoreCase("sybase"))
			allUrl="jdbc:sybase:Tds:"+ip+":"+Port+"/"+DB;
		else if(type.equalsIgnoreCase("PostgreSQL"))
			allUrl="jdbc:postgresql://"+ip+":"+Port+"/"+DB;
		else
		{
			System.out.println("请输入有效类型，并确认驱动加载成功！");
			return null;
		}
		try {
			Connection conn=DriverManager.getConnection(allUrl,user,password);
			if(Debug)
			System.out.println("连接数据库成功");
			return conn;	
		} catch (SQLException e) {
			// TODO 自动生成的 catch 块
			System.out.println("数据库连接失败");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	public static Connection Connect(String type,String ip,String Port,String DB,String Character,String user,String password)
	{
		String allUrl=null;
		if(type.equalsIgnoreCase("mysql"))
			allUrl="jdbc:mysql://"+ip+":"+Port+"/"+DB+"?useUnicode=true&characterEncoding="+Character;
		else if(type.equalsIgnoreCase("oracle"))
			allUrl="jdbc:oracle:thin:@"+ip+":"+Port+":"+DB;
		else if(type.equalsIgnoreCase("sqlserve"))
			allUrl="jdbc:sqlserve://"+ip+":"+Port+";DatabaseName="+DB+";useunicode=true;characterEncoding="+Character;
		else if(type.equalsIgnoreCase("DB2"))
			allUrl="jdbc:db2://"+ip+":"+Port+"/"+DB+"?useUnicode=true&characterEncoding="+Character;
		else if(type.equalsIgnoreCase("sybase"))
			allUrl="jdbc:sybase:Tds:"+ip+":"+Port+"/"+DB+"?useUnicode=true&characterEncoding="+Character;
		else if(type.equalsIgnoreCase("PostgreSQL"))
			allUrl="jdbc:postgresql://"+ip+":"+Port+"/"+DB+"?useUnicode=true&characterEncoding="+Character;
		else
		{
			System.out.println("请输入有效类型，并确认驱动加载成功！");
			return null;
		}
		try {
			Connection conn=DriverManager.getConnection(allUrl,user,password);
			if(Debug)
			System.out.println("连接数据库成功");
			return conn;	
		} catch (SQLException e) {
			// TODO 自动生成的 catch 块
			System.out.println("数据库连接失败");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	public static Connection ConnectWithWindows(String ip,String Port,String DB)
	{
		String allUrl=null;
		allUrl="jdbc:sqlserve://"+ip+":"+Port+";integratedSecurity=true;DatabaseName="+DB;
		try {
			Connection conn=DriverManager.getConnection(allUrl);
			if(Debug)
			System.out.println("连接数据库成功");
			return conn;	
		} catch (SQLException e) {
			// TODO 自动生成的 catch 块
			System.out.println("数据库连接失败");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	public static LinkedHashMap<String,String> fetchOne(Connection link, String query)
	{
		LinkedHashMap<String,String> rtn = new LinkedHashMap<String,String>();
		try {
			Statement stmt = link.createStatement();
			ResultSet rs= stmt.executeQuery(query);
			if(Debug)
			System.out.println("字段查找成功");
			ResultSetMetaData rsmt=rs.getMetaData();
			int cum=rsmt.getColumnCount();
			if(rs.next())
			{
				for(int i=1;i<=cum;i++){
					String key=rsmt.getColumnName(i);
					rtn.put(key,rs.getString(key));
				}
				return rtn;
			}else{
				return null;
			}
		} catch (SQLException e) {
			// TODO 自动生成的 catch 块
			System.out.println("查询失败，不存在的词条!");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	public static boolean wasResult(Connection link, String query)
	{
		try {
			Statement stmt = link.createStatement();
			ResultSet rs= stmt.executeQuery(query);
			if(Debug)
			System.out.println("字段查找成功");
			if(rs.next())
			{
				return true;
			}else{
				return false;
			}
		} catch (SQLException e) {
			// TODO 自动生成的 catch 块
			System.out.println("查询失败，不存在的词条!");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return false;
		}
	}
	
	public static LinkedHashMap<String,String>[] fetchAll(Connection link, String query)
	{
		ArrayList<LinkedHashMap<String,String>> rtnum= new ArrayList<LinkedHashMap<String,String>>();
		try {
			Statement stmt = link.createStatement();
			ResultSet rs= stmt.executeQuery(query);
			if(Debug)
			System.out.println("字段查找成功");
			ResultSetMetaData rsmt=rs.getMetaData();
			int cum=rsmt.getColumnCount();
			while(rs.next())
			{
				LinkedHashMap<String,String> rtn = new LinkedHashMap<String,String>();
				for(int j=1;j<=cum;j++){
					String key=rsmt.getColumnName(j);
					rtn.put(key,rs.getString(key));
				}
				rtnum.add(rtn);
			}
			if(rtnum.size()==0)
				return null;
			else
			{
				LinkedHashMap<String,String>[] array = new LinkedHashMap[rtnum.size()];
		        for(int i=0;i<rtnum.size();i++){  
		            array[i]=rtnum.get(i);  
		        }  
		        return array;
			}
		} catch (SQLException e) {
			System.out.println("查询失败，不存在的词条!");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	public static boolean Insert(Connection link,LinkedHashMap<String,String> data,String table)
	{
		try {
			Statement stmt = link.createStatement();
			if(data.size()==0)
			{
			if(Debug)
			System.out.println("请传入有效数据！");
			return false;
			}
			String val="";
			String key="";
			Iterator<Entry<String, String>> iter = data.entrySet().iterator(); 
			while (iter.hasNext()) { 
			    Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next(); 
			    key += (String)entry.getKey() + ","; 
			    val += "'" + (String)entry.getValue() + "'" + ","; 
			} 
			key.trim();
			val.trim();
			key = key.substring(0,key.length()-1);
			val = val.substring(0,val.length()-1);
			String query="INSERT "+table+"("+key+") "+"VALUES("+val+")";
			int rs= stmt.executeUpdate(query);
			if(rs>0)
			{
				if(Debug)
				System.out.println("插入成功");
				return true;
			}else{
				if(Debug)
				System.out.println("插入失败");
				return false;
			}
			
		}catch (SQLException e) {
				// TODO 自动生成的 catch 块
				System.out.println("添加失败，不存在的词条!");
				System.out.print("ERROR:"+e.getMessage());
				e.printStackTrace();
				return false;
			}
	}
	
	public static boolean Update(Connection link,LinkedHashMap<String,String> data,String table,String where)
	{
		try{
			Statement stmt = link.createStatement();
			if(data.size()==0)
			{
			if(Debug)
			System.out.println("请传入有效数据！");
			return false;
			}
			String val="";
			Iterator<Entry<String, String>> iter = data.entrySet().iterator(); 
			while (iter.hasNext()) { 
			    Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next(); 
			    val += (String)entry.getKey() + "='" + (String)entry.getValue() + "'" + ",";
			} 
			val.trim();
			val = val.substring(0,val.length()-1);
			String query="UPDATE "+table + " SET "+val+" WHERE "+where;
			int rs= stmt.executeUpdate(query);
			if(rs>0)
			{
				if(Debug)
				System.out.println("修改成功");
				return true;
			}else{
				if(Debug)
				System.out.println("修改失败");
				return false;
			}
		}catch (SQLException e) {
			// TODO 自动生成的 catch 块
			System.out.println("更新失败，不存在的词条!");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return false;
		}
	}
	
	public static boolean Delete(Connection link,String table,String where)
	{
		try{
		Statement stmt = link.createStatement();
		String query="DELETE FROM "+table +" WHERE "+where;
		int rs= stmt.executeUpdate(query);
		if(rs>0)
		{
			if(Debug)
			System.out.println("删除成功");
			return true;
		}else{
			if(Debug)
			System.out.println("删除失败");
			return false;
		}
		}catch (SQLException e) {
			System.out.println("更新失败，不存在的词条!");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return false;
		}
	}
	
	public static boolean UpdateAny(Connection link,String query)
	{
		try{
		Statement stmt = link.createStatement();
		int rs= stmt.executeUpdate(query);
		if(rs>0)
		{
			if(Debug)
			System.out.println("处理成功");
			return true;
		}else{
			if(Debug)
			System.out.println("处理失败");
			return false;
		}
		}catch (SQLException e) {
			System.out.println("更新失败，不存在的词条!");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return false;
		}
	}
	
	public static ResultSet BackResultSet(Connection link,String query)
	{
		try{
		Statement stmt = link.createStatement();
		ResultSet rtn= stmt.executeQuery(query);
		if(rtn!=null)
		{
			if(Debug)
			System.out.println("返回成功");
			return rtn;
		}else{
			if(Debug)
			System.out.println("为空");
			return null;
		}
		}catch (SQLException e) {
			System.out.println("返回失败，不存在的词条!");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	public static ResultSet BackResultSet(Connection link,String query,int Scroll,int Concur)
	{
		try{
		Statement stmt = link.createStatement(Scroll,Concur);
		ResultSet rtn= stmt.executeQuery(query);
		if(rtn!=null)
		{
			if(Debug)
			System.out.println("返回成功");
			return rtn;
		}else{
			if(Debug)
			System.out.println("为空");
			return null;
		}
		}catch (SQLException e) {
			System.out.println("返回失败，不存在的词条!");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	public static LinkedHashMap<String,String>[] resultSetToMap(ResultSet rs)
	{
		try{
		ArrayList<LinkedHashMap<String,String>> rtnum= new ArrayList<LinkedHashMap<String,String>>();
		ResultSetMetaData rsmt=rs.getMetaData();
		int cum=rsmt.getColumnCount();
		while(rs.next())
		{
			LinkedHashMap<String,String> rtn = new LinkedHashMap<String,String>();
			for(int j=1;j<=cum;j++){
				String key=rsmt.getColumnName(j);
				rtn.put(key,rs.getString(key));
			}
			rtnum.add(rtn);
		}
		if(rtnum.size()==0)
			return null;
		else
		{
			LinkedHashMap<String,String>[] array = new LinkedHashMap[rtnum.size()];
	        for(int i=0;i<rtnum.size();i++){  
	            array[i]=rtnum.get(i);  
	        }  
	        return array;
		}
		}catch (SQLException e) {
			System.out.println("转换失败！");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	public static ResultSetMetaData getMetaData(Connection link, String table)
	{
		try {
			Statement stmt = link.createStatement();
			ResultSet rs= stmt.executeQuery("SELECT * FROM "+table+" LIMIT 0,1");
			if(Debug)
			System.out.println("字段查找成功");
			ResultSetMetaData rsmt=rs.getMetaData();
			return rsmt;
		} catch (SQLException e) {
			// TODO 自动生成的 catch 块
			System.out.println("查询失败，不存在的词条!");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	public static void tableInfo(Connection link, String table)
	{
		try {
			Statement stmt = link.createStatement();
			ResultSet rs= stmt.executeQuery("SELECT * FROM "+table+" LIMIT 0,1");
			if(Debug)
			System.out.println("字段查找成功");
			ResultSetMetaData rsmt=rs.getMetaData();
			int num = rsmt.getColumnCount();
			System.out.println("\t*-------------------------------------------*");
			System.out.println("\t表格名称:"+table);
			System.out.println("\t表格列数:"+num);
			System.out.println("\t表格字段:");
			for(int i=1;i<=num;i++)
			{
				String name ="("+rsmt.getColumnTypeName(i)+")"+"["+rsmt.getColumnDisplaySize(i)+"]"+rsmt.getColumnName(i);
				String Unsign=rsmt.isSigned(i)?"":"(Unsign)";
				String AutoC=rsmt.isAutoIncrement(i)?"(AutoIncrement)":"";
				String ROM=rsmt.isReadOnly(i)?"(ReadOnly)":"";
				String NULL=rsmt.isNullable(i)==ResultSetMetaData.columnNullable?"":"(Not Null)";
				name=Unsign+name+AutoC+ROM+NULL;
				System.out.println("\t"+name);
			}
			System.out.println("\t*--------------------END--------------------*");
		} catch (SQLException e) {
			// TODO 自动生成的 catch 块
			System.out.println("查询失败，不存在的词条!");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
		}
	}
	
	public static void databaseInfo(Connection link)
	{
		try {
			DatabaseMetaData dbmd = link.getMetaData();	
			ResultSet dbrs =  dbmd.getTables(null, null, null,new String[] { "TABLE" });
			System.out.println("\t*-------------------------------------------*");
			System.out.println("\t数据库名称:"+dbmd.getDatabaseProductName()+" "+dbmd.getJDBCMajorVersion());
			System.out.println("\t数据库版本:"+dbmd.getDatabaseProductVersion());
			System.out.println("\t数据库驱动:"+dbmd.getDriverVersion());
			System.out.println("\t数据库类型:"+dbmd.getTypeInfo());
			System.out.print("\t数据库内表:");
			for(int n=1;dbrs.next();n++)
			{ 
				System.out.print(dbrs.getString(3)+"\t");
				if(n%3==0)
				System.out.println();
			}
			System.out.println();
			System.out.println("\t*--------------------END--------------------*");
		} catch (SQLException e) {
			// TODO 自动生成的 catch 块
			System.out.println("查询失败，不存在的词条!");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
		}
	}
	
	public static boolean resultSetToXML(ResultSet rs,String name)
	{
		try {
			File xml = new File(name);
			FileWriter fileWritter = new FileWriter(xml);
			BufferedWriter BW = new BufferedWriter(fileWritter);
		int i=0;
		BW.write("<resultSet>\r\n");
		ResultSetMetaData rsmt=rs.getMetaData();
		int cum=rsmt.getColumnCount();
		while(rs.next())
		{
			BW.write("<set id='"+i+"'>\r\n");
			for(int j=1;j<=cum;j++){
				String key=rsmt.getColumnName(j);
				BW.write("<"+key+">");
				BW.write(rs.getString(key));
				BW.write("</"+key+">\r\n");
			}
			BW.write("</set>\r\n");
			i++;
		}
		BW.write("</resultSet>\r\n");
		BW.close();
		if(Debug)
		System.out.println("输出成功");
		return true;
		}catch (IOException e) {
			// TODO 自动生成的 catch 块
			if(Debug)
			System.out.println("文件创建失败");
			e.printStackTrace();
			return false;
		}catch (SQLException e) {
			System.out.println("连接数据库失败!");
			System.out.print("ERROR:"+e.getMessage());
			e.printStackTrace();
			return false;
		}
	}
	
	public static boolean resultSetToJson(ResultSet rs,String name){
		try {
			File xml = new File(name);
			FileWriter fileWritter = new FileWriter(xml);
			BufferedWriter BW = new BufferedWriter(fileWritter);
		LinkedHashMap<String,String>[] rsmap = SuperJDBC.resultSetToMap(rs);
		if(rsmap==null)
		{
			if(Debug)
			System.out.println("结果集为空");
			BW.close();
			return false;
		}
		int i=1;
		int num = rsmap.length;
		BW.write("{\"reasonSet\":[\r\n");
		for(LinkedHashMap<String,String> val : rsmap)
		{
			BW.write("{");
			Iterator<Entry<String, String>> iter = val.entrySet().iterator(); 
			while (iter.hasNext()) { 
			    Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next(); 
			    BW.write("\""+(String)entry.getKey()+"\":");
			    BW.write("\""+(String)entry.getValue()+"\"");
			    if(iter.hasNext())
			    	BW.write(",");
			} 
			if(i!=num)
				BW.write("},\r\n");
			else
				BW.write("}\r\n");
			i++;
		}
		BW.write("]}\r\n");
		BW.close();
		if(Debug)
		System.out.println("输出成功");
		return true;
		}catch (IOException e) {
			// TODO 自动生成的 catch 块
			if(Debug)
			System.out.println("文件创建失败");
			e.printStackTrace();
			return false;
		}
	}
	
	public static void main(String[] args)
	{
		SuperJDBC.DriverInfo();
		SuperJDBC.Debug(true);
		Connection link=SuperJDBC.Connect("mysql", "localhost", "3306", "640_schema","root","root");
		LinkedHashMap<String,String>[] a = SuperJDBC.fetchAll(link, "SELECT * FROM student");
		for(LinkedHashMap<String,String> val : a)
		{
			System.out.println("姓名:"+val.get("name")+" 年龄:"+val.get("age")+" 性别:"+val.get("sex"));
		}
		LinkedHashMap<String,String> data= new LinkedHashMap<String,String>();
		data.put("name", "求大师");
		data.put("age", "56");
		data.put("sex", "男");
		SuperJDBC.tableInfo(link, "student");
		SuperJDBC.databaseInfo(link);
		SuperJDBC.resultSetToXML(SuperJDBC.BackResultSet(link, "SELECT * FROM student"),"result.xml");
		SuperJDBC.resultSetToJson(SuperJDBC.BackResultSet(link, "SELECT * FROM student"),"result.json");
		
	}
}
