package com.alvazan.orm.api.spi2.meta;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlInheritance;
import com.alvazan.orm.api.base.anno.NoSqlInheritanceType;
import com.alvazan.orm.api.base.anno.NoSqlTransient;
import com.alvazan.orm.api.spi2.IndexData;
import com.alvazan.orm.api.spi2.InfoForIndex;
import com.alvazan.orm.api.spi2.NoSqlTypedRowProxy;
import com.alvazan.orm.api.spi2.RowToPersist;
import com.alvazan.orm.api.spi2.TypedRow;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.api.spi3.db.conv.Converters.BaseConverter;
import com.alvazan.orm.api.spi3.db.conv.StandardConverters;

@NoSqlEntity
@NoSqlInheritance(subclassesToScan={DboColumnCommonMeta.class, DboColumnToOneMeta.class, DboColumnToManyMeta.class, DboColumnIdMeta.class},
		strategy=NoSqlInheritanceType.SINGLE_TABLE, discriminatorColumnName="classType")
public abstract class DboColumnMeta {

	@NoSqlId
	protected String id;
	
	protected String columnName;

	private String foreignKeyToExtensions;
	
	@NoSqlManyToOne
	protected DboTableMeta owner;
	
	@NoSqlTransient
	private transient byte[] columnAsBytes;
	
	protected transient BaseConverter converter;
	
	public String getId() {
		return id;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setup(DboTableMeta owner2, String colName) {
		if(owner2.getColumnFamily() == null)
			throw new IllegalArgumentException("The owner passed in must have a non-null column family name");
		else if(colName == null)
			throw new IllegalStateException("colName parameter must not be null");
		this.owner = owner2;
		this.columnName = colName;
		owner2.addColumnMeta(this);
		id = owner.getColumnFamily()+":"+columnName;	
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
		converter = StandardConverters.get(getStorageTypeAsClass());
		if(converter == null)
			throw new IllegalArgumentException("type="+getClassType()+" is not supported at this point");		
	}
	
	@SuppressWarnings("rawtypes")
	private Class getStorageTypeAsClass() {
		switch (getStorageType()) {
		case STRING:
			return String.class;
		case INTEGER:
			return BigInteger.class;
		case DECIMAL:
			return BigDecimal.class;
		case BYTES:
			return byte[].class;
		default:
			throw new UnsupportedOperationException("type not supported="+getStorageType());
		}
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

	public String getForeignKeyToExtensions() {
		return foreignKeyToExtensions;
	}

	public void setForeignKeyToExtensions(String foreignKeyToExtensions) {
		this.foreignKeyToExtensions = foreignKeyToExtensions;
	}

	public abstract void translateToColumn(InfoForIndex<TypedRow> info);

	protected void removeIndexInfo(InfoForIndex<TypedRow> info, Object value, byte[] byteVal, StorageTypeEnum storageType) {
		RowToPersist row = info.getRow();
		String columnFamily = info.getColumnFamily();
		Map<String, Object> fieldToValue = info.getFieldToValue();
		
		if(!isIndexed())
			return;
		else if(storageType == StorageTypeEnum.BYTES)
			throw new IllegalArgumentException("Cannot do indexing for types that are stored as bytes at this time");
		else if(fieldToValue == null)
			return;
		
		addIndexRemoves(row, columnFamily, value, byteVal, storageType, fieldToValue);
	}
	
	private void addIndexRemoves(RowToPersist row, String columnFamily,
			Object value, byte[] byteVal, StorageTypeEnum storageType, Map<String, Object> fieldToValue) {
		//if we are here, we are indexed, BUT if fieldToValue is null, then it is a brand new entity and not a proxy
		Object originalValue = fieldToValue.get(columnName);
		if(originalValue == null)
			return;
		else if(originalValue.equals(value))
			return; //previous value is the same, yeah, nothing to do here!!!

		byte[] oldIndexedVal = this.convertToStorage2(originalValue);
		byte[] pk = row.getKey();
		//original value and current value differ so we need to remove from the index
		IndexData data = createAddIndexData(columnFamily, oldIndexedVal, storageType, pk );
		row.addIndexToRemove(data);
	}
	
	protected void removingThisEntity(InfoForIndex<TypedRow> info,
			List<IndexData> indexRemoves, byte[] pk, StorageTypeEnum storageType) {
		String columnFamily = info.getColumnFamily();
		Map<String, Object> fieldToValue = info.getFieldToValue();
		Object valueInDatabase = fieldToValue.get(columnName);
		if(valueInDatabase == null)
			return;

		byte[] oldIndexedVal = convertToStorage2(valueInDatabase);
		IndexData data = createAddIndexData(columnFamily, oldIndexedVal, storageType, pk);
		indexRemoves.add(data);
	}
	
	protected void addIndexInfo(InfoForIndex<TypedRow> info, Object value, byte[] byteVal, StorageTypeEnum storageType) {
		TypedRow entity = info.getEntity();
		RowToPersist row = info.getRow();
		String columnFamily = info.getColumnFamily();
		Map<Field, Object> fieldToValue = info.getFieldToValue();
		
		if(!isIndexed())
			return;
		else if(storageType == StorageTypeEnum.BYTES)
			throw new IllegalArgumentException("Cannot do indexing for types that are stored as bytes at this time");
		
		if(!isNeedPersist(entity, value, fieldToValue))
			return;
		
		//original value and current value differ so we need to persist new value
		byte[] pk = row.getKey();
		IndexData data = createAddIndexData(columnFamily, byteVal, storageType, pk);
		row.addIndexToPersist(data);
	}

	private boolean isNeedPersist(TypedRow entity, Object value, Map<Field, Object> fieldToValue) {
		if(!(entity instanceof NoSqlTypedRowProxy))
			return true;
		Object originalValue = fieldToValue.get(columnName);
		if(value == null) //new value is null so nothing to persist 
			return false;
		else if(value.equals(originalValue))
			return false; //previous value is the same, yeah, nothing to do here!!!
		
		return true;
	}
	private IndexData createAddIndexData(String columnFamily,
			byte[] byteVal, StorageTypeEnum storageType, byte[] pk) {
		IndexData data = new IndexData();
		data.setColumnFamilyName(storageType.getIndexTableName());
		data.setRowKey("/"+columnFamily+"/"+getColumnName());
		data.getIndexColumn().setIndexedValue(byteVal);
		data.getIndexColumn().setPrimaryKey(pk);
		return data;
	}

	public abstract void translateFromColumn(Row row, TypedRow inst);
	
}
