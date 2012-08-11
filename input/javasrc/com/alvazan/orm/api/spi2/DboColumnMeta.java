package com.alvazan.orm.api.spi2;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;

import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.ManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlInheritance;
import com.alvazan.orm.api.base.anno.NoSqlInheritanceType;
import com.alvazan.orm.api.base.anno.NoSqlTransient;
import com.alvazan.orm.api.spi3.db.conv.Converters.BaseConverter;
import com.alvazan.orm.api.spi3.db.conv.StandardConverters;

@NoSqlEntity
@NoSqlInheritance(subclassesToScan={DboColumnCommonMeta.class, DboColumnToOneMeta.class, DboColumnToManyMeta.class, DboColumnIdMeta.class},
		strategy=NoSqlInheritanceType.SINGLE_TABLE, discriminatorColumnName="classType")
public abstract class DboColumnMeta {

	@Id
	private String id;
	
	protected String columnName;

	private String foreignKeyToExtensions;
	
	@ManyToOne
	private DboTableMeta owner;
	
	@NoSqlTransient
	private transient byte[] columnAsBytes;
	
	protected transient BaseConverter converter;
	
	public String getId() {
		return id;
	}

	public String getColumnName() {
		return columnName;
	}

	public abstract String getIndexPrefix();
	public abstract boolean isIndexed();
	/**
	 * This is the more detailed type for programs to know what types the values fit into.  This would be
	 * of type long.class, short.class, float, etc. etc.
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public abstract Class getClassType();
	/**
	 * This is the raw database type of String, BigInteger or BigDecimal
	 * @return
	 */
	public abstract StorageTypeEnum getStorageType();
	//public abstract String convertToValue(byte[] dbValue);

	protected synchronized void initConverter() {
		converter = StandardConverters.get(getClassType());
		if(converter == null)
			throw new IllegalArgumentException("type="+getClassType()+" is not supported at this point");		
	}
	
	public byte[] convertToStorage(String value) {
		initConverter();
		return converter.convertToNoSqlFromString(value);
	}
	
	public byte[] convertToStorage2(Object value) {
		initConverter();
		return converter.convertToNoSql(value);
	}

	public Object convertFromStorage2(byte[] data) {
		initConverter();
		return converter.convertFromNoSql(data);		
	}
	
	@SuppressWarnings("rawtypes")
	protected static Class translateType(Class classType) {
		Class finalType = classType;
		if(!StandardConverters.containsConverterFor(classType))
			finalType = byte[].class; //if it is not a supported type, we always support a straight byte[] as the type
		
		finalType = convertIfPrimitive(finalType);

		return finalType;
	}

	@SuppressWarnings("rawtypes")
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
	
	@SuppressWarnings("rawtypes")
	protected static Class classForName(String columnType) {
		try {
			return Class.forName(columnType);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
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

	public void setOwner(DboTableMeta owner) {
		this.owner = owner;
		if(columnName == null)
			throw new IllegalStateException("Please call setup method on this DboColumnxxxMeta before adding to another entity");
		id = owner.getColumnFamily()+":"+columnName;		
	}

	public String getForeignKeyToExtensions() {
		return foreignKeyToExtensions;
	}

	public void setForeignKeyToExtensions(String foreignKeyToExtensions) {
		this.foreignKeyToExtensions = foreignKeyToExtensions;
	}
	
}
