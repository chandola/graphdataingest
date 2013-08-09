/**
 * 
 */
package gov.ornl.healthcare;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Manages configuration information. Loads configuration properties through XML config file.
 * 
 * @author chandola
 *
 */
public class Configuration // Singleton
{
	public static final String DEFAULT_LOGGER_NAME = "gov.ornl.healthcare";	
	private static final String CONFIG_DOCUMENT_NAME = "config/configuration.xml";

	private static Configuration ourInstance = new Configuration();

	private final Logger logger;
	private final Properties properties;

	private static Configuration getInstance()
	{
		return ourInstance;
	}

	/**
	 * Private configuration invoked only internally.
	 */
	private Configuration()
	{
		this.properties = new Properties();
		this.logger = Logger.getLogger(DEFAULT_LOGGER_NAME);
		try
		{
			this.loadDefaultConfigDocument();
		}
		catch(Exception e)
		{
			Logger.getLogger(DEFAULT_LOGGER_NAME).log(Level.SEVERE, "Configuration.ConfigNotFound");
		}
	}

	/**
	 * Return the default logger
	 * @return
	 */
	public static Logger getLogger()
	{
		return getInstance().logger;
	}

	private void loadDefaultConfigDocument()
	{
		loadConfigDocument(CONFIG_DOCUMENT_NAME);
	}
	
	public static void addConfigDocument(String fileName)
	{
		getInstance().loadConfigDocument(fileName);
	}
	
	private void loadConfigDocument(String fileName)
	{
		InputStream inputStream;
		File file = new File(fileName);
		try
		{
			if(file.exists())
				inputStream = new FileInputStream(file);
			else
				inputStream = Configuration.class.getResourceAsStream(CONFIG_DOCUMENT_NAME);
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder;
			documentBuilder = documentBuilderFactory.newDocumentBuilder();
			Document doc = documentBuilder.parse(inputStream);
			this.loadConfigProperties(doc);
		} 
		catch (SAXException | IOException | ParserConfigurationException e)
		{
			Logger.getLogger(DEFAULT_LOGGER_NAME).log(Level.SEVERE, e.getMessage(),e);
		}
	}
	
	private void loadConfigProperties(Document doc)
    {
        try
        {
        	XPath xPath = XPathFactory.newInstance().newXPath();

            NodeList nodes = (NodeList) xPath.evaluate("/Configuration/Property", doc, XPathConstants.NODESET);
            if (nodes == null || nodes.getLength() == 0)
                return;

            for (int i = 0; i < nodes.getLength(); i++)
            {
                Node node = nodes.item(i);
                String prop = xPath.evaluate("@name", node);
                String value = xPath.evaluate("@value", node);
                if (prop.isEmpty())                                   
                    continue;

		this.properties.setProperty(prop, value);
            }
        }
        catch (XPathExpressionException e)
        {
            Logger.getLogger(DEFAULT_LOGGER_NAME).log(Level.WARNING, "XML.ParserConfigurationException");
        }
    }
	   /**                                                                                                                                                                                                                                                                         
     * Return as a string the value associated with a specified key.                                                                                                                                                                                                            
     *                                                                                                                                                                                                                                                                          
     * @param key          the key for the desired value.                                                                                                                                                                                                                       
     * @param defaultValue the value to return if the key does not exist.                                                                                                                                                                                                       
     *                                                                                                                                                                                                                                                                          
     * @return the value associated with the key, or the specified default value if the key does not exist.                                                                                                                                                                     
     */
    public static synchronized String getStringValue(String key, String defaultValue)
    {
        String v = getStringValue(key);
        return v != null ? v : defaultValue;
    }

    /**                                                                                                                                                                                                                                                                         
     * Return as a string the value associated with a specified key.                                                                                                                                                                                                            
     *                                                                                                                                                                                                                                                                          
     * @param key the key for the desired value.                                                                                                                                                                                                                                
     *                                                                                                                                                                                                                                                                          
     * @return the value associated with the key, or null if the key does not exist.                                                                                                                                                                                            
     */
    public static synchronized String getStringValue(String key)
    {
        Object o = getInstance().properties.getProperty(key);
        return o != null ? o.toString() : null;
    }

    /**                                                                                                                                                                                                                                                                         
     * Return as an Integer the value associated with a specified key.                                                                                                                                                                                                          
     *                                                                                                                                                                                                                                                                          
     * @param key          the key for the desired value.                                                                                                                                                                                                                       
     * @param defaultValue the value to return if the key does not exist.                                                                                                                                                                                                       
     *                                                                                                                                                                                                                                                                          
     * @return the value associated with the key, or the specified default value if the key does not exist or is not an                                                                                                                                                         
     *         Integer or string representation of an Integer.                                                                                                                                                                                                                  
     */
    public static synchronized Integer getIntegerValue(String key, Integer defaultValue)
    {
        Integer v = getIntegerValue(key);
        return v != null ? v : defaultValue;
    }
    /**                                                                                                                                                                                                                                                                         
     * Return as an Integer the value associated with a specified key.                                                                                                                                                                                                          
     *                                                                                                                                                                                                                                                                          
     * @param key the key for the desired value.                                                                                                                                                                                                                                
     *                                                                                                                                                                                                                                                                          
     * @return the value associated with the key, or null if the key does not exist or is not an Integer or string                                                                                                                                                              
     *         representation of an Integer.                                                                                                                                                                                                                                    
     */
    public static synchronized Integer getIntegerValue(String key)
    {
        String v = getStringValue(key);
        if (v == null)
            return null;

        try
        {
            return Integer.parseInt(v);
        }
        catch (NumberFormatException e)
        {
        	Logger.getLogger(DEFAULT_LOGGER_NAME).log(Level.SEVERE, "Configuration.ConversionError", v);
            return null;
        }
    }

    /**                                                                                                                                                                                                                                                                         
     * Return as an Long the value associated with a specified key.                                                                                                                                                                                                             
     *                                                                                                                                                                                                                                                                          
     * @param key          the key for the desired value.                                                                                                                                                                                                                       
     * @param defaultValue the value to return if the key does not exist.                                                                                                                                                                                                       
     *                                                                                                                                                                                                                                                                          
     * @return the value associated with the key, or the specified default value if the key does not exist or is not a                                                                                                                                                          
     *         Long or string representation of a Long.                                                                                                                                                                                                                         
     */
    public static synchronized Long getLongValue(String key, Long defaultValue)
    {
        Long v = getLongValue(key);
        return v != null ? v : defaultValue;
    }

    /**                                                                                                                                                                                                                                                                         
     * Return as an Long the value associated with a specified key.                                                                                                                                                                                                             
     *                                                                                                                                                                                                                                                                          
     * @param key the key for the desired value.                                                                                                                                                                                                                                
     *                                                                                                                                                                                                                                                                          
     * @return the value associated with the key, or null if the key does not exist or is not a Long or string                                                                                                                                                                  
     *         representation of a Long.                                                                                                                                                                                                                                        
     */
    public static synchronized Long getLongValue(String key)
    {
        String v = getStringValue(key);
        if (v == null)
            return null;

	try
        {
            return Long.parseLong(v);
        }
        catch (NumberFormatException e)
        {
        	Logger.getLogger(DEFAULT_LOGGER_NAME).log(Level.SEVERE, "Configuration.ConversionError", v);
            return null;
        }
    }
    /**                                                                                                                                                                                                                                                                         
     * Return as an Double the value associated with a specified key.                                                                                                                                                                                                           
     *                                                                                                                                                                                                                                                                          
     * @param key          the key for the desired value.                                                                                                                                                                                                                       
     * @param defaultValue the value to return if the key does not exist.                                                                                                                                                                                                       
     *                                                                                                                                                                                                                                                                          
     * @return the value associated with the key, or the specified default value if the key does not exist or is not an                                                                                                                                                         
     *         Double or string representation of an Double.                                                                                                                                                                                                                    
     */
    public static synchronized Double getDoubleValue(String key, Double defaultValue)
    {
        Double v = getDoubleValue(key);
        return v != null ? v : defaultValue;
    }

    /**                                                                                                                                                                                                                                                                         
     * Return as an Double the value associated with a specified key.                                                                                                                                                                                                           
     *                                                                                                                                                                                                                                                                          
     * @param key the key for the desired value.                                                                                                                                                                                                                                
     *                                                                                                                                                                                                                                                                          
     * @return the value associated with the key, or null if the key does not exist or is not an Double or string                                                                                                                                                               
     *         representation of an Double.                                                                                                                                                                                                                                     
     */
    public static synchronized Double getDoubleValue(String key)
    {
        String v = getStringValue(key);
        if (v == null)
            return null;

        try
        {
            return Double.parseDouble(v);
        }
        catch (NumberFormatException e)
        {
        	Logger.getLogger(DEFAULT_LOGGER_NAME).log(Level.SEVERE, "Configuration.ConversionError", v);
            return null;
        }
    }

    /**                                                                                                                                                                                                                                                                         
     * Return as a Boolean the value associated with a specified key.                                                                                                                                                                                                           
     * <p/>                                                                                                                                                                                                                                                                     
     * Valid values for true are '1' or anything that starts with 't' or 'T'. ie. 'true', 'True', 't' Valid values for                                                                                                                                                          
     * false are '0' or anything that starts with 'f' or 'F'. ie. 'false', 'False', 'f'                                                                                                                                                                                         
     *                                                                                                                                                                                                                                                                          
     * @param key          the key for the desired value.                                                                                                                                                                                                                       
     * @param defaultValue the value to return if the key does not exist.                                                                                                                                                                                                       
     *                                                                                                                                                                                                                                                                          
     * @return the value associated with the key, or the specified default value if the key does not exist or is not a                                                                                                                                                          
     *         Boolean or string representation of an Boolean.                                                                                                                                                                                                                  
     */
    public static synchronized Boolean getBooleanValue(String key, Boolean defaultValue)
    {
        Boolean v = getBooleanValue(key);
        return v != null ? v : defaultValue;
    }
    /**                                                                                                                                                                                                                                                                         
     * Return as a Boolean the value associated with a specified key.                                                                                                                                                                                                           
     * <p/>                                                                                                                                                                                                                                                                     
     * Valid values for true are '1' or anything that starts with 't' or 'T'. ie. 'true', 'True', 't' Valid values for                                                                                                                                                          
     * false are '0' or anything that starts with 'f' or 'F'. ie. 'false', 'False', 'f'                                                                                                                                                                                         
     *                                                                                                                                                                                                                                                                          
     * @param key the key for the desired value.                                                                                                                                                                                                                                
     *                                                                                                                                                                                                                                                                          
     * @return the value associated with the key, or null if the key does not exist or is not a Boolean or string                                                                                                                                                               
     *         representation of an Boolean.                                                                                                                                                                                                                                    
     */
    public static synchronized Boolean getBooleanValue(String key)
    {
        String v = getStringValue(key);
        if (v == null)
            return null;

	if (v.trim().toUpperCase().startsWith("T") || v.trim().equals("1"))
        {
            return true;
        }
        else if (v.trim().toUpperCase().startsWith("F") || v.trim().equals("0"))
        {
            return false;
        }
        else
        {
            Logger.getLogger(DEFAULT_LOGGER_NAME).log(Level.SEVERE, "Configuration.ConversionError", v);
            return null;
        }
    }

    /**                                                                                                                                                                                                                                                                         
     * Determines whether a key exists in the configuration.                                                                                                                                                                                                                    
     *                                                                                                                                                                                                                                                                          
     * @param key the key of interest.                                                                                                                                                                                                                                          
     *                                                                                                                                                                                                                                                                          
     * @return true if the key exists, otherwise false.                                                                                                                                                                                                                         
     */
    public static synchronized boolean hasKey(String key)
    {
        return getInstance().properties.contains(key);
    }

    /**                                                                                                                                                                                                                                                                         
     * Removes a key and its value from the configuration if the configuration contains the key.                                                                                                                                                                                
     *                                                                                                                                                                                                                                                                          
     * @param key the key of interest.                                                                                                                                                                                                                                          
     */
    public static synchronized void removeKey(String key)
    {
        getInstance().properties.remove(key);
    }

    /**                                                                                                                                                                                                                                                                         
     * Adds a key and value to the configuration, or changes the value associated with the key if the key is already in                                                                                                                                                         
     * the configuration.                                                                                                                                                                                                                                                       
     *                                                                                                                                                                                                                                                                          
     * @param key   the key to set.                                                                                                                                                                                                                                             
     * @param value the value to associate with the key.                                                                                                                                                                                                                        
     */
    public static synchronized void setValue(String key, Object value)
    {
        getInstance().properties.put(key, value.toString());
    }
}
