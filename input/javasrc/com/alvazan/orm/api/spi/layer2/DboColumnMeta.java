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
	private static Set<Class> primitives = new HashSet<Class>();
	
	static {
		primitives.add(Long.class);
		primitives.add(Integer.class);
		primitives.add(Short.class);
		primitives.add(Byte.class);
		primitives.add(Double.class);
		primitives.add(Float.class);
		primitives.add(Boolean.class);
		primitives.add(String.class);
		primitives.add(Character.class);
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
		Class newType = convertIfPrimitive(classType);
		if(!primitives.contains(newType))
			newType = Byte.class;
		this.columnName = colName;
		this.fkToColumnFamily = fkToTable;
		if(newType != null)
			this.columnType = newType.getName();
		this.isToManyColumn = isToManyColumn;
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

}
