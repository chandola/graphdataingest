package gov.ornl.healthcare.dataimport;

import gov.ornl.healthcare.Configuration;

public class Tester
{

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		// TODO Auto-generated method stub
		String prop = Configuration.getStringValue("postgres.url");
		System.out.println(prop);
	}

}
