package gov.ornl.healthcare.dataimport;

import java.net.UnknownHostException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class ColumnExtractor
{
  //default connection properties
	String pg_url = "jdbc:postgresql://localhost/healthcare";
	String pg_user = "postgres";
	String pg_password = "postgres";
	String pg_table = "nppes";
	
	Connection pg_con = null;
	Statement pg_st = null;
	ResultSet pg_rs = null;

	MongoClient mongo_client = null;
	DB mongo_db = null;
	
	Logger log = Logger.getLogger(ColumnExtractor.class.getName());
	
	public void pg_initDB()
	{
		try
		{
			pg_con = DriverManager.getConnection(pg_url,pg_user,pg_password);			
		}
		catch(SQLException e)
		{
			log.log(Level.SEVERE,e.getMessage(),e);
		}
	}
	
	public void pg_closeDB()
	{
		try
		{
			if(pg_rs != null)
				pg_rs.close();
			if(pg_st != null)
				pg_st.close();
			if(pg_con != null)
				pg_con.close();
		}
		catch(SQLException e)
		{
			log.log(Level.WARNING, e.getMessage(), e);
		}
	}
	
	public void mongo_initDB()
	{
		try
		{
			mongo_client = new MongoClient();
		} catch (UnknownHostException e)
		{
			log.log(Level.SEVERE, e.getMessage(),e);
			return;
		}
		mongo_db = mongo_client.getDB( "healthcare" );
	}
	
	public void mongo_closeDB()
	{
		if(mongo_client != null)
			mongo_client.close();
	}
	
	public void extractColumn(String columnName)
	{
		try
		{
			pg_st = pg_con.createStatement();
			pg_rs = pg_st.executeQuery("SELECT DISTINCT("+columnName+") FROM "+pg_table);
			if(pg_rs.next())
				System.out.println(pg_rs.getString(1));
		}
		catch(SQLException e)
		{
			log.log(Level.WARNING, e.getMessage(),e);
		}
	}
	
	public static void main(String [] args)
	{
		ColumnExtractor extractor = new ColumnExtractor();
		extractor.pg_initDB();
		extractor.mongo_initDB();
		String columnName = "NPI";
		extractor.extractColumn(columnName);
		extractor.pg_closeDB();
		extractor.mongo_closeDB();
	}
}
