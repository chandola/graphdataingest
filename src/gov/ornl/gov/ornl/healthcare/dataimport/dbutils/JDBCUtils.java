package gov.ornl.healthcare.dataimport.dbutils;

import gov.ornl.healthcare.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;

public class JDBCUtils
{
	private String url;
	private String user;
	private String password;

	Connection connection = null;
	Statement statement = null;
	ResultSet resultset = null;
	
	public JDBCUtils(String url, String user, String password)
	{
		this.url = url;
		this.user = user;
		this.password = password;
	}
	
	public void initDB()
	{
		try
		{
			connection = DriverManager.getConnection(url,user,password);			
		}
		catch(SQLException e)
		{
			Configuration.getLogger().log(Level.SEVERE,e.getMessage(),e);
		}
	}

	public void closeDB()
	{
		try
		{
			if(resultset != null)
				resultset.close();
			if(statement != null)
				statement.close();
			if(connection != null)
				connection.close();
		}
		catch(SQLException e)
		{
			Configuration.getLogger().log(Level.WARNING, e.getMessage(), e);
		}
	}

	public void executeQuery(String query)
	{
		try
		{
			statement = connection.createStatement();
			resultset = statement.executeQuery(query);
		} catch (SQLException e)
		{
			Configuration.getLogger().log(Level.WARNING, e.getMessage(), e);
		}
	}
	
	public ResultSet getResultset()
	{
		return resultset;
	}

}
