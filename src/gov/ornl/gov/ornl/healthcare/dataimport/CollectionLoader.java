/**
 * 
 */
package gov.ornl.healthcare.dataimport;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
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
		long time_s, time_e;
		time_s = System.currentTimeMillis();
		Configuration.getLogger().log(Level.FINE, "Creating primary collection");
		createEntityCollection(mongoUtils);
		Configuration.getLogger().log(Level.FINE, "Finished creating primary collection");
		time_e = System.currentTimeMillis();

		System.out.println("Primary collection created in "+(time_e - time_s)/1000+" seconds.	");

		String mongoCollectionFields = Configuration.getStringValue("mongoCollectionFields");
		if(mongoCollectionFields != null)
		{
			time_s = System.currentTimeMillis();
			Configuration.getLogger().log(Level.FINE, "Creating secondary collections");
			StringTokenizer tokenizer = new StringTokenizer(mongoCollectionFields,",");
			Random random = new Random();
			String tempId = new BigInteger(130,random).toString(32);
			while(tokenizer.hasMoreTokens())
			{
				String field = tokenizer.nextToken();
				if(mongoUtils.hasCollection(field))
				{
					//create a temporary field collection
					mongoUtils.createFieldCollection(field, tempId);
					//merge it with existing collection
					mongoUtils.mergeCollections(field,tempId);				
					//delete temporary field collection
					mongoUtils.dropCollection(tempId);
				}
				else
				{
					mongoUtils.createFieldCollection(field, field);
				}
			}
			time_e = System.currentTimeMillis();
			System.out.println("Secondary collections created in "+(time_e - time_s)/1000+" seconds.");

			Configuration.getLogger().log(Level.FINE, "Finished creating secondary collections");
		}
		mongoUtils.closeDB();
	}

	private static void createEntityCollection(MongoUtils mongoUtils)
	{
		String source = Configuration.getStringValue("source");
		if(source.compareToIgnoreCase("db") == 0)
		{
			createEntityCollectionFromDB(mongoUtils);
		}
		if(source.compareToIgnoreCase("file") == 0)
		{
			createEntityCollectionFromFile(mongoUtils);
		}
	}


	private static void createEntityCollectionFromDB(MongoUtils mongoUtils)
	{
		String dbURL = Configuration.getStringValue("dbURL");
		String dbUser = Configuration.getStringValue("dbUser");
		String dbPassword = Configuration.getStringValue("dbPassword");
		char mongoInsertMode =  Configuration.getStringValue("mongoInsertMode").charAt(0);
		String mongoCollectionName = Configuration.getStringValue("mongoCollectionName");
		String mongoCollectionFields = Configuration.getStringValue("mongoCollectionFields");
		String[] fieldNames = getTokens(mongoCollectionFields);
		String dbQuery = Configuration.getStringValue("dbQuery");
		JDBCUtils dbUtils = new JDBCUtils(dbURL,dbUser,dbPassword);
		dbUtils.initDB();
		Configuration.getLogger().log(Level.FINE,"Extract Query Started");
		dbUtils.executeQuery(dbQuery);
		Configuration.getLogger().log(Level.FINE,"Extract Query Finished");
		if(dbUtils.getResultset() != null)
		{
			mongoUtils.setCollection(mongoCollectionName);
			if(mongoInsertMode == 'w')
				mongoUtils.clearCollection();

			ResultSet rs = dbUtils.getResultset();
			try
			{
				while(rs.next())
				{
					BasicDBObject dbObject = createMongoObject(rs,fieldNames);
					if(dbObject != null)
					{
						mongoUtils.addToCollection(dbObject);
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

	private static void createEntityCollectionFromFile(MongoUtils mongoUtils)
	{
		String dataFile = Configuration.getStringValue("dataFile");
		BufferedReader br;
		try
		{
			br = new BufferedReader(new FileReader(dataFile));
			char mongoInsertMode =  Configuration.getStringValue("mongoInsertMode").charAt(0);
			String mongoCollectionName = Configuration.getStringValue("mongoCollectionName");
			String mongoCollectionFields = Configuration.getStringValue("mongoCollectionFields");
			String[] fieldNames = getTokens(mongoCollectionFields);

			mongoUtils.setCollection(mongoCollectionName);
			if(mongoInsertMode == 'w')
				mongoUtils.clearCollection();
			String line;
			while ((line = br.readLine()) != null) {
				BasicDBObject dbObject = createMongoObject(line,fieldNames);
				if(dbObject != null)
				{
					mongoUtils.addToCollection(dbObject);
				}
			}
			br.close();
		} catch (FileNotFoundException e)
		{
			Configuration.getLogger().log(Level.SEVERE,e.getMessage(),e);
		} catch (IOException e)
		{
			Configuration.getLogger().log(Level.SEVERE,e.getMessage(),e);		
		}
	}

	private static BasicDBObject createMongoObject(ResultSet rs, String [] fieldNames)
	{
		String mongoCollectionSource = Configuration.getStringValue("mongoCollectionSource");
		if(mongoCollectionSource == null || mongoCollectionSource.isEmpty())
			mongoCollectionSource = "NA";
		BasicDBObject dbObject = new BasicDBObject("source",mongoCollectionSource);
		EntityResolver resolver = new EntityResolver();
		for(int i = 0; i < fieldNames.length; i++)
		{
			String field = fieldNames[i];
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

	private static BasicDBObject createMongoObject(String line, String [] fieldNames)
	{
		String mongoCollectionSource = Configuration.getStringValue("mongoCollectionSource");
		if(mongoCollectionSource == null || mongoCollectionSource.isEmpty())
			mongoCollectionSource = "NA";
		StringTokenizer lineTokenizer = new StringTokenizer(line,",");
		BasicDBObject dbObject = new BasicDBObject("source",mongoCollectionSource);
		EntityResolver resolver = new EntityResolver();
		int i = 0;
		while(lineTokenizer.hasMoreTokens())
		{
			String field = fieldNames[i];
			i++;
			String value = lineTokenizer.nextToken();
			if(field.endsWith("address"))
				value = resolver.resolveAddress(value);
			if(field.endsWith("telephone_number"))
				value = resolver.resolvePhoneNumber(value);
			dbObject = dbObject.append(field, value);
		}
		return dbObject;
	}

	private static String[] getTokens(String line)
	{
		StringTokenizer tokenizer = new StringTokenizer(line,",");
		String[] tokens = new String[tokenizer.countTokens()];
		int i = 0;
		while(tokenizer.hasMoreTokens())
		{
			tokens[i] = tokenizer.nextToken().trim();
			i++;
		}
		return tokens;
	}
}
