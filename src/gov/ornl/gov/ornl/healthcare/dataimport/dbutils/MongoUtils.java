/**
 * 
 */
package gov.ornl.healthcare.dataimport.dbutils;

import gov.ornl.healthcare.Configuration;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.logging.Level;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * @author chandola
 *
 */
public class MongoUtils
{
	private String url = "localhost";
	private int port = 27017;
	private String database;
	private MongoClient client = null;
	private DB db = null;
	private HashMap<String,DBCollection> collectionMap;
	

	public MongoUtils(String database)
	{
		this.database = database;
		this.collectionMap = new HashMap<String,DBCollection>();
	}
	
	public MongoUtils(String url, int port, String database)
	{
		this.url = url;
		this.port = port;
		this.database = database;
	}
	
	public void initDB()
	{
		try
		{
			client = new MongoClient(url,port);
		} catch (UnknownHostException e)
		{
			Configuration.getLogger().log(Level.SEVERE, e.getMessage(),e);
			return;
		}
		db = client.getDB(database);		
	}

	public void addCollection(String collectionName)
	{
		if(!collectionMap.containsKey(collectionName))
		{
			DBCollection collection = this.db.getCollection(collectionName);
			collectionMap.put(collectionName, collection);
		}
	}
	
	public void clearCollection(String collectionName)
	{
		if(collectionMap.containsKey(collectionName))
			collectionMap.get(collectionName).remove(new BasicDBObject());
	}
	
	public long getCollectionCount(String collectionName)
	{
		if(collectionMap.containsKey(collectionName))
			return collectionMap.get(collectionName).count();
		else
			return 0;
	}
	
	public void closeDB()
	{
		if(client != null)
			client.close();
	}

	public void addToCollection(String collectionName,BasicDBObject dbObject)
	{
		if(collectionMap.containsKey(collectionName))
		{
			collectionMap.get(collectionName).insert(dbObject);
		}
	}
}
