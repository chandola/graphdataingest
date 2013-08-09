package gov.ornl.healthcare.dataimport.providers;

import java.util.logging.Level;

import gov.ornl.healthcare.Configuration;
import gov.ornl.healthcare.dataimport.CollectionLoader;

public class NPPESProviderLoader
{
	public static void main(String args[])
	{
		String configurationURL = args[0];
		Configuration.getLogger().setLevel(Level.FINE);
		Configuration.addConfigDocument(configurationURL);
		CollectionLoader.run();		
	}

}
