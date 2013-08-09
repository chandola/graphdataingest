/**
 * 
 */
package gov.ornl.healthcare.dataimport;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;
import java.util.logging.Level;

import com.mongodb.BasicDBObject;

import gov.ornl.healthcare.Configuration;
import gov.ornl.healthcare.dataimport.dbutils.MongoUtils;
import gov.ornl.healthcare.dataimport.dbutils.JDBCUtils;
import gov.ornl.healthcare.dataimport.entityresolution.EntityResolver;


/**
 * Class to load a collection of providers from a JDBC accessible database into Mongo
 * @author chandola
 *
 */
public class CollectionLoader
{
	public static void run()
	{
		String mongoURL = Configuration.getStringValue("mongoURL");
		int mongoPort = Configuration.getIntegerValue("mongoPort");
		String mongoDatabase = Configuration.getStringValue("mongoDatabase");
		MongoUtils mongoUtils;
		if(mongoURL == null)
			mongoUtils = new MongoUtils(mongoDatabase);
		else
			mongoUtils = new MongoUtils(mongoURL,mongoPort,mongoDatabase);
		mongoUtils.initDB();
		createEntityCollection(mongoUtils);
/*
		String mongoCollectionFields = Configuration.getStringValue("mongoCollectionFields");
		if(mongoCollectionFields != null)
		{
			StringTokenizer tokenizer = new StringTokenizer(mongoCollectionFields,",");
			while(tokenizer.hasMoreTokens())
			{
				String field = tokenizer.nextToken();
			}
		}
		*/
		mongoUtils.closeDB();
	}

	private static void createEntityCollection(MongoUtils mongoUtils)
	{
		String dbURL = Configuration.getStringValue("dbURL");
		String dbUser = Configuration.getStringValue("dbUser");
		String dbPassword = Configuration.getStringValue("dbPassword");
		char mongoInsertMode =  Configuration.getStringValue("mongoInsertMode").charAt(0);
		String mongoCollectionName = Configuration.getStringValue("mongoCollectionName");
		String dbQuery = Configuration.getStringValue("dbQuery");
		JDBCUtils dbUtils = new JDBCUtils(dbURL,dbUser,dbPassword);
		dbUtils.initDB();
		Configuration.getLogger().log(Level.FINE,"Extract Query Started");
		dbUtils.executeQuery(dbQuery);
		Configuration.getLogger().log(Level.FINE,"Extract Query Finished");
		if(dbUtils.getResultset() != null)
		{
			mongoUtils.addCollection(mongoCollectionName);
			if(mongoInsertMode == 'w')
				mongoUtils.clearCollection(mongoCollectionName);

			ResultSet rs = dbUtils.getResultset();
			long count = mongoUtils.getCollectionCount(mongoCollectionName) + 1;
			try
			{
				while(rs.next())
				{
					BasicDBObject dbObject = createMongoObject(rs,count);
					if(dbObject != null)
					{
						mongoUtils.addToCollection(mongoCollectionName,dbObject);
						count++;
					}
				}
			} 
			catch (SQLException e)
			{
				Configuration.getLogger().log(Level.SEVERE,e.getMessage(),e);
			}
		}
		dbUtils.closeDB();
	}

	private static BasicDBObject createMongoObject(ResultSet rs, long count)
	{
		String mongoCollectionSource = Configuration.getStringValue("mongoCollectionSource");
		String mongoCollectionFields = Configuration.getStringValue("mongoCollectionFields");
		if(mongoCollectionSource == null || mongoCollectionSource.isEmpty())
			mongoCollectionSource = "NA";
		if(mongoCollectionFields == null)
			return null;
		StringTokenizer tokenizer = new StringTokenizer(mongoCollectionFields,",");
		BasicDBObject dbObject = new BasicDBObject("_id",count).append("source",mongoCollectionSource);
		EntityResolver resolver = new EntityResolver();
		while(tokenizer.hasMoreTokens())
		{
			String field = tokenizer.nextToken();
			try
			{
				String value = rs.getString(field);
				if(field.endsWith("address"))
					value = resolver.resolveAddress(value);
				if(field.endsWith("telephone_number"))
					value = resolver.resolvePhoneNumber(value);
				dbObject = dbObject.append(field, value);
			} catch (SQLException e)
			{
				Configuration.getLogger().log(Level.WARNING,"Could not extract field "+field);
			}
		}
		return dbObject;
	}
}
