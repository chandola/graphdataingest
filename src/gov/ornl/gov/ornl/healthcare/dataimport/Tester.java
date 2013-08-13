package gov.ornl.healthcare.dataimport;

import java.util.logging.Level;
import gov.ornl.healthcare.Configuration;
import gov.ornl.healthcare.dataimport.dbutils.MongoUtils;

public class Tester
{

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		String configurationURL = args[0];
		Configuration.getLogger().setLevel(Level.FINEST);
		Configuration.addConfigDocument(configurationURL);

		String mongoURL = Configuration.getStringValue("mongoURL");
		int mongoPort = Configuration.getIntegerValue("mongoPort");
		String mongoDatabase = Configuration.getStringValue("mongoDatabase");
		MongoUtils mongoUtils;
		if(mongoURL == null)
			mongoUtils = new MongoUtils(mongoDatabase);
		else
			mongoUtils = new MongoUtils(mongoURL,mongoPort,mongoDatabase);
		mongoUtils.initDB();
		String mongoCollectionName = Configuration.getStringValue("mongoCollectionName");
		mongoUtils.setCollection(mongoCollectionName);
		mongoUtils.createFieldCollection("name","name");
		mongoUtils.closeDB();
	}

}
