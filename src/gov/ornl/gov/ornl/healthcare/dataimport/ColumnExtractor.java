package gov.ornl.healthcare.dataimport;

import gov.ornl.healthcare.Configuration;

import java.net.UnknownHostException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.logging.Level;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
/*
 * Simple utility to connect from postgres to Mongo
 * 
 */
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

	public void pg_initDB()
	{
		try
		{
			pg_con = DriverManager.getConnection(pg_url,pg_user,pg_password);			
		}
		catch(SQLException e)
		{
			Configuration.getLogger().log(Level.SEVERE,e.getMessage(),e);
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
			Configuration.getLogger().log(Level.WARNING, e.getMessage(), e);
		}
	}

	public void mongo_initDB()
	{
		try
		{
			mongo_client = new MongoClient();
		} catch (UnknownHostException e)
		{
			Configuration.getLogger().log(Level.SEVERE, e.getMessage(),e);
			return;
		}
		mongo_db = mongo_client.getDB( "healthcare" );		
	}

	public void mongo_closeDB()
	{
		if(mongo_client != null)
			mongo_client.close();
	}

	public void extractColumn(ExtractionRule rule)
	{
		try
		{
			//extract column from Postgres
			pg_st = pg_con.createStatement();
			pg_rs = pg_st.executeQuery("SELECT DISTINCT("+rule.getQuery()+") FROM "+pg_table);
		}
		catch(SQLException e)
		{
			Configuration.getLogger().log(Level.WARNING, e.getMessage(),e);
		}		

	}

	public void migrateColumn(String collectionName, char mode)
	{
		if(pg_rs != null)
		{
			try
			{
				//choose the Mongo Collection to add to
				DBCollection collection = mongo_db.getCollection(collectionName);
				if(mode == 'w')
					collection.remove(new BasicDBObject());
				
				long cnt = collection.count()+1;
				while(pg_rs.next())
				{
					BasicDBObject obj = new BasicDBObject("_id",cnt).append("text", pg_rs.getString(1));
					collection.insert(obj);
					cnt++;
				}

			}
			catch(SQLException e)
			{	
				Configuration.getLogger().log(Level.WARNING, e.getMessage(),e);
			}
		}
	}

	public static void main(String [] args)
	{
		long start,end;
		
		// process arguments
		if(args.length < 4)
		{
			System.err.println("Incorrect Arguments");
			System.err.println("Usage:\n");
			System.err.println("java ColumnExtractor mongocolumnname mode numfields fieldstring\n");
			System.exit(0);
		}
		String columnName = args[0];
		char mode = args[1].charAt(0);
		int numFields = Integer.parseInt(args[2]);
		String fieldString = args[3];
		Vector<String> fieldNames = new Vector<String>(numFields);
		StringTokenizer tokenizer = new StringTokenizer(fieldString,",");
		while(tokenizer.hasMoreTokens())
			fieldNames.add(tokenizer.nextToken());
		
		ColumnExtractor extractor = new ColumnExtractor();
		extractor.pg_initDB();
		extractor.mongo_initDB();
		ExtractionRule rule;
		if(numFields == 1)
		{
			rule = new ExtractionRule(ExtractionRule.TYPE.SINGLE);
			rule.append(fieldNames.firstElement());
		}
		else
		{
			rule = new ExtractionRule(ExtractionRule.TYPE.COMPOUND);
			rule.setFieldNames(fieldNames);
		}
		Configuration.getLogger().log(Level.INFO,"Column extraction started.");
		start = System.currentTimeMillis();
		extractor.extractColumn(rule);
		end = System.currentTimeMillis();
		Configuration.getLogger().log(Level.INFO,"Column extracted in "+(end - start)+" milliseconds.");
		Configuration.getLogger().log(Level.INFO,"Column migration started.");
		start = System.currentTimeMillis();
		extractor.migrateColumn(columnName,mode);
		end = System.currentTimeMillis();
		Configuration.getLogger().log(Level.INFO,"Column migrated in "+(end - start)+" milliseconds.");
		extractor.pg_closeDB();
		extractor.mongo_closeDB();
	}
}
