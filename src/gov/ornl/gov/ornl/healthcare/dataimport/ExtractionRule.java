/**
 * 
 */
package gov.ornl.healthcare.dataimport;

import java.util.Vector;

/**
 * Wrapper class for encoding rules to combine fieldnames
 * @author chandola
 *
 */
public class ExtractionRule
{
	public static enum TYPE
	{
		SINGLE, COMPOUND
	};
	private ExtractionRule.TYPE ruleType;
	private Vector<String> fieldNames;
	
	public ExtractionRule(ExtractionRule.TYPE type)
	{
		this.ruleType = type;
		this.fieldNames = new Vector<String>();
	}
	
	public void setFieldNames(Vector<String> fieldNames)
	{
		this.fieldNames = fieldNames;
	}
	
	public void append(String fieldName)
	{
		this.fieldNames.add(fieldName);
	}
	
	public String getQuery()
	{
		if(ruleType == ExtractionRule.TYPE.SINGLE)
			return fieldNames.firstElement();
		if(fieldNames.size() == 0)
			return "";
		
		StringBuffer buf = new StringBuffer();
		for(int i = 0; i < fieldNames.size() - 1; i++)
		{
			buf.append("trim(");
			buf.append(fieldNames.get(i));
			buf.append(")||\' \'||");
		}
		buf.append("trim(");
		buf.append(fieldNames.get(fieldNames.size() - 1));
		buf.append(")");
		return buf.toString();
	}
}
