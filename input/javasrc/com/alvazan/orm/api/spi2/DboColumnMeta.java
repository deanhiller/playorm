package com.alvazan.orm.api.spi2;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;

import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.ManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlTransient;
import com.alvazan.orm.api.spi3.db.conv.AdhocToolConverter;
import com.alvazan.orm.api.spi3.db.conv.Converter;
import com.alvazan.orm.api.spi3.db.conv.StandardConverters;

@NoSqlEntity
@SuppressWarnings("rawtypes")
public class DboColumnMeta {

	@Id(usegenerator=false)
	private String id;
	
	/**
	 * This is either listOfFk, fk, generic, or id
	 */
	private String classType;
	
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
	
	private String foreignKeyToExtensions;

	private String indexPrefix;

	@ManyToOne
	private DboTableMeta owner;

	@NoSqlTransient
	private transient byte[] columnAsBytes;
	
	public String getColumnName() {
		return columnName;
	}

	@Override
	public String toString() {
		return "Field["+columnName+"]";
	}

	public void setup(String colName, DboTableMeta fkToTable, Class classType, boolean isToManyColumn, String indexPrefix) {
		if(fkToTable == null && isToManyColumn)
			throw new IllegalArgumentException("isToManyColumn must be false if there is no fk");
		else if(classType != null && fkToTable != null)
			throw new IllegalArgumentException("classType should not be specified when this column is an FK column to another table");
		else if(isToManyColumn && indexPrefix != null)
			throw new IllegalArgumentException("Cannot have a *ToMany and Index that column as well");
		
		Class newType = translateType(classType);
		this.columnName = colName;
		this.fkToColumnFamily = fkToTable;
		if(newType != null)
			this.columnType = newType.getName();
		this.isToManyColumn = isToManyColumn;
		this.indexPrefix = indexPrefix;
	}

	protected static Class translateType(Class classType) {
		Class finalType = classType;
		if(!StandardConverters.containsConverterFor(classType))
			finalType = byte[].class; //if it is not a supported type, we always support a straight byte[] as the type
		
		finalType = convertIfPrimitive(finalType);

		return finalType;
	}

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
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public DboTableMeta getOwner() {
		return owner;
	}

	public void setOwner(DboTableMeta owner) {
		this.owner = owner;
		if(columnName == null)
			throw new IllegalStateException("Please call setup method before adding to another entity");
		id = owner.getColumnFamily()+":"+columnName;
	}

	public Class getClassType() {
		if(columnType == null)
			return null;
		
		return classForName(columnType);
	}

	public StorageTypeEnum getStorageType() {
		Class fieldType = getClassType();
		return getStorageType(fieldType);
	}

	public static StorageTypeEnum getStorageType(Class fieldType) {
		StorageTypeEnum type = null;
		if(byte[].class.equals(fieldType))
			type = StorageTypeEnum.BYTES;
		else if(Long.class.equals(fieldType))
			type = StorageTypeEnum.INTEGER;
		else if(Integer.class.equals(fieldType))
			type = StorageTypeEnum.INTEGER;
		else if(Short.class.equals(fieldType))
			type = StorageTypeEnum.INTEGER;
		else if(Byte.class.equals(fieldType))
			type = StorageTypeEnum.INTEGER;
		else if(Double.class.equals(fieldType))
			type = StorageTypeEnum.DECIMAL;
		else if(Float.class.equals(fieldType))
			type = StorageTypeEnum.DECIMAL;
		else if(Boolean.class.equals(fieldType))
			type = StorageTypeEnum.INTEGER;
		else if(Character.class.equals(fieldType))
			type = StorageTypeEnum.STRING;
		else if(String.class.equals(fieldType))
			type = StorageTypeEnum.STRING;
		else if(BigDecimal.class.equals(fieldType))
			type = StorageTypeEnum.DECIMAL;
		else if(BigInteger.class.equals(fieldType))
			type = StorageTypeEnum.INTEGER;
		
		return type;
	}
	
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

	public boolean isIndexed() {
		return indexPrefix != null;
	}
	public String getIndexPrefix() {
		return indexPrefix;
	}

	public String getForeignKeyToExtensions() {
		return foreignKeyToExtensions;
	}

	public void setForeignKeyToExtensions(String foreignKeyToExtensions) {
		this.foreignKeyToExtensions = foreignKeyToExtensions;
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
	
	public String convertToValue(byte[] dbValue) {
		if(fkToColumnFamily != null)
			return fkToColumnFamily.getIdColumnMeta().convertToValue(dbValue);
		Class type = getClassType();
		AdhocToolConverter converter = StandardConverters.get(type);
		if(converter == null)
			throw new IllegalArgumentException("type="+type+" is not supported at this point");
		return converter.convertFromNoSqlToString(dbValue)+"";		
	}

	public byte[] convertToStorage2(Object value) {
		if(fkToColumnFamily != null) {
			return fkToColumnFamily.getIdColumnMeta().convertToStorage2(value);
		}
		Converter converter = StandardConverters.get(getClassType());
		if(converter == null)
			throw new IllegalArgumentException("type="+getClassType()+" is not supported at this point");
		
		return converter.convertToNoSql(value);
	}

	public Object convertFromStorage2(byte[] data) {
		if(fkToColumnFamily != null) {
			return fkToColumnFamily.getIdColumnMeta().convertFromStorage2(data);
		}
		Converter converter = StandardConverters.get(getClassType());
		if(converter == null)
			throw new IllegalArgumentException("type="+getClassType()+" is not supported at this point");
		
		return converter.convertFromNoSql(data);		
	}
	
	public byte[] getColumnNameAsBytes() {
		if(columnAsBytes == null)
			columnAsBytes = toUTF8(columnName);
		return columnAsBytes;
	}

	private byte[] toUTF8(String columnName) {
		try {
			return columnName.getBytes("UTF8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
}
