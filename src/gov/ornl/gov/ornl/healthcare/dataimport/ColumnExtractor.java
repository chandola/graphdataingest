package gov.ornl.healthcare.dataimport;

import java.net.UnknownHostException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

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

	private final static Logger log = Logger.getLogger(ColumnExtractor.class.getName());

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

	public void extractColumn(Vector<String> fieldNames)
	{
		try
		{
			//extract column from Postgres
			ExtractionRule rule = new ExtractionRule(ExtractionRule.TYPE.COMPOUND);
			rule.setFieldNames(fieldNames);
			pg_st = pg_con.createStatement();
			pg_rs = pg_st.executeQuery("SELECT DISTINCT("+rule.getQuery()+") FROM "+pg_table);
		}
		catch(SQLException e)
		{
			log.log(Level.WARNING, e.getMessage(),e);
		}		

	}

	public void migrateColumn(String collectionName)
	{
		if(pg_rs != null)
		{
			try
			{
				//choose the Mongo Collection to add to
				DBCollection collection = mongo_db.getCollection(collectionName);
				while(pg_rs.next())
				{
					BasicDBObject obj = new BasicDBObject("text", pg_rs.getString(1));
					collection.insert(obj);
				}

			}
			catch(SQLException e)
			{
				log.log(Level.WARNING, e.getMessage(),e);
			}
		}
	}

	public static void main(String [] args)
	{
		log.setLevel(Level.INFO);
		long start,end;
		ColumnExtractor extractor = new ColumnExtractor();
		extractor.pg_initDB();
		extractor.mongo_initDB();
		Vector<String> fieldNames = new Vector<String>(2);
		fieldNames.add("provider_first_name");
		fieldNames.add("provider_last_name");
		start = System.currentTimeMillis();
		extractor.extractColumn(fieldNames);
		end = System.currentTimeMillis();
		log.log(Level.INFO,"Column extracted in "+(end - start)+" milliseconds.");
		start = System.currentTimeMillis();
		extractor.migrateColumn("provider_names");
		end = System.currentTimeMillis();
		log.log(Level.INFO,"Column migrated in "+(end - start)+" milliseconds.");
		extractor.pg_closeDB();
		extractor.mongo_closeDB();
	}
}
