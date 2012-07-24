package com.alvazan.orm.api.spi.layer2;

import java.util.HashSet;
import java.util.Set;

import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.ManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlEntity;

@NoSqlEntity
public class DboColumnMeta {

	@Id(usegenerator=false)
	private String columnName;
	
	/**
	 * null for FK relationships.  Contains primitive type..
	 */
	private String columnType;
	
	/**
	 * This field may be referencing another entity in another table so here is the meta data
	 * on that table as well, but for now, I don't think we need it until we have joins
	 */
	@ManyToOne
	private DboTableMeta fkToColumnFamily;
	
	private boolean isToManyColumn;
	
	@SuppressWarnings("rawtypes")
	private static Set<Class> supportedRawTypes = new HashSet<Class>();
	
	static {
		supportedRawTypes.add(Long.class);
		supportedRawTypes.add(Integer.class);
		supportedRawTypes.add(Short.class);
		supportedRawTypes.add(Byte.class);
		supportedRawTypes.add(Double.class);
		supportedRawTypes.add(Float.class);
		supportedRawTypes.add(Boolean.class);
		supportedRawTypes.add(String.class);
		supportedRawTypes.add(Character.class);
		supportedRawTypes.add(byte[].class);
	}
	
	public String getColumnName() {
		return columnName;
	}

	@Override
	public String toString() {
		return "Field["+columnName+"]";
	}

	@SuppressWarnings("rawtypes")
	public void setup(String colName, DboTableMeta fkToTable, Class classType, boolean isToManyColumn) {
		if(fkToTable == null && isToManyColumn)
			throw new IllegalArgumentException("isToManyColumn must be false if there is no fk");
		else if(classType != null && fkToTable != null)
			throw new IllegalArgumentException("classType should not be specified when this column is an FK column to another table");
		
		Class newType = translateType(classType);
		this.columnName = colName;
		this.fkToColumnFamily = fkToTable;
		if(newType != null)
			this.columnType = newType.getName();
		this.isToManyColumn = isToManyColumn;
	}

	@SuppressWarnings("rawtypes")
	protected static Class translateType(Class classType) {
		Class newType = convertIfPrimitive(classType);
		if(!supportedRawTypes.contains(newType))
			newType = byte[].class; //if it is not a supported type, we always support a straight byte[] as the type
		return newType;
	}

	@SuppressWarnings({ "rawtypes" })
	public static Class convertIfPrimitive(Class fieldType) {
		Class c = fieldType;
		if(long.class.equals(fieldType))
			c = Long.class;
		else if(int.class.equals(fieldType))
			c = Integer.class;
		else if(short.class.equals(fieldType))
			c = Short.class;
		else if(byte.class.equals(fieldType))
			c = Byte.class;
		else if(double.class.equals(fieldType))
			c = Double.class;
		else if(float.class.equals(fieldType))
			c = Float.class;
		else if(boolean.class.equals(fieldType))
			c = Boolean.class;
		else if(char.class.equals(fieldType))
			c = Character.class;
		return c;
	}
	
	@SuppressWarnings("rawtypes")
	public Class getClassType() {
		if(columnType == null)
			return null;
		
		return classForName(columnType);
	}

	@SuppressWarnings("rawtypes")
	protected static Class classForName(String columnType) {
		try {
			return Class.forName(columnType);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public DboTableMeta getFkToColumnFamily() {
		return fkToColumnFamily;
	}

	public boolean isToManyColumn() {
		return isToManyColumn;
	}

	public byte[] convertToStorage(String value) {
		if(fkToColumnFamily != null) {
			return fkToColumnFamily.getIdColumnMeta().convertToStorage(value);
		}
		AdhocToolConverter converter = StandardConverters.get(getClassType());
		if(converter == null)
			throw new IllegalArgumentException("type="+getClassType()+" is not supported at this point");
		return converter.convertToNoSqlFromString(value);
	}
	
	@SuppressWarnings("rawtypes")
	public String convertToValue(byte[] dbValue) {
		if(fkToColumnFamily != null)
			return fkToColumnFamily.getIdColumnMeta().convertToValue(dbValue);
		Class type = getClassType();
		AdhocToolConverter converter = StandardConverters.get(type);
		if(converter == null)
			throw new IllegalArgumentException("type="+type+" is not supported at this point");
		return converter.convertFromNoSqlToString(dbValue)+"";		
	}
}
