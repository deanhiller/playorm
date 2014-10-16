package com.alvazan.orm.api.z8spi.meta;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.base.anno.NoSqlEmbedded;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlInheritance;
import com.alvazan.orm.api.base.anno.NoSqlInheritanceType;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlTransient;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.conv.Converters.BaseConverter;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;

@SuppressWarnings("rawtypes")
@NoSqlEntity
@NoSqlInheritance(subclassesToScan = { DboColumnCommonMeta.class,
		DboColumnToOneMeta.class, DboColumnToManyMeta.class,
		DboColumnIdMeta.class, DboColumnEmbedMeta.class, 
		DboColumnEmbedSimpleMeta.class, DboColumnTTLMeta.class
		}, strategy = NoSqlInheritanceType.SINGLE_TABLE, discriminatorColumnName = "classType")
public abstract class DboColumnMeta {

	@NoSqlId(usegenerator = false)
	protected String id;

	@NoSqlIndexed
	private String columnName;

	protected boolean isIndexed;

	private String foreignKeyToExtensions;

	@NoSqlIndexed
	@NoSqlManyToOne
	protected DboTableMeta owner;

	@NoSqlTransient
	private transient byte[] columnAsBytes;

	protected transient BaseConverter converter;
	

	/** 
	 * Specifically not for playorm but for clients of playorm that want to add meta data
	 * to the DboTableMeta object
	 */
	@NoSqlEmbedded
	private Map<String, String> extensions = new HashMap<String, String>();
	
	public String getId() {
		return id;
	}

	@Override
	public String toString() {
		return "[" + columnName + ",ind=" + isIndexed + ",type="
				+ getClass().getSimpleName() + "]";
	}

	public String getColumnName() {
		return columnName;
	}

	protected void setup(DboTableMeta owner2, String colName, boolean isIndexed) {
		if (owner2.getColumnFamily() == null)
			throw new IllegalArgumentException(
					"The owner passed in must have a non-null column family name");
		else if (colName == null)
			throw new IllegalStateException(
					"colName parameter must not be null");
		this.owner = owner2;
		setColumnName(colName);
		owner2.addColumnMeta(this);
		id = owner.getColumnFamily() + ":" + columnName;
		this.isIndexed = isIndexed;
	}

	protected void setColumnName(String colName) {
		if (!DboTableMeta.NAME_PATTERN.matcher(colName).matches())
			throw new IllegalArgumentException(
					"Column name must match regular expression='[a-zA-Z_][a-zA-Z_0-9\\-]*'");
		this.columnName = colName;
	}

	public static boolean isValidColumnName(String colName) {
		if(DboTableMeta.NAME_PATTERN.matcher(colName).matches())
			return true;
		return false;
	}
	
	public abstract boolean isPartitionedByThisColumn();

	public abstract String getIndexTableName();

	public String getIndexRowKey(String partitionedBy, String partitionId) {
		String firstPart = "/" + owner.getColumnFamily() + "/" + columnName;
		if (partitionedBy == null)
			return firstPart;
		firstPart += "/" + partitionedBy;
		if (partitionId == null)
			return firstPart;
		return firstPart + "/" + partitionId;
	}

	public boolean isIndexed() {
		return isIndexed;
	}

	/**
	 * This is the more detailed type for programs to know what types the values
	 * fit into. This would be of type long.class, short.class, float, etc. etc.
	 * 
	 * @return <code>Class</code> type of the Column
	 */
	public abstract Class getClassType();

	/**
	 * This is the raw database type of String, BigInteger or BigDecimal
	 * 
	 * @return the storage type
	 */
	public abstract StorageTypeEnum getStorageType();

	// public abstract String convertToValue(byte[] dbValue);

	protected synchronized void initConverter() {
		converter = StandardConverters.get(getStorageTypeAsClass());
		if (converter == null)
			throw new IllegalArgumentException("type=" + getClassType()
					+ " is not supported at this point");
	}

	private Class getStorageTypeAsClass() {
		switch (getStorageType()) {
		case STRING:
			return String.class;
		case INTEGER:
			return BigInteger.class;
		case DECIMAL:
			return BigDecimal.class;
		case BOOLEAN:
			return Boolean.class;
		case BYTES:
			return byte[].class;
		default:
			throw new UnsupportedOperationException("type not supported="
					+ getStorageType());
		}
	}

	public String convertTypeToString(Object value) {
		initConverter();
		// BIG NOTE: This seems pretty stupid as long+"" is the STring, etc. etc
		// and BigDecimal+"" is the STring
		// BUT it unfortunately, this does not work for byte[] which needs to be
		// output in hex and translated back from
		// hex when put in the gui and such. other more complex types would
		// probably not work as well.
		return converter.convertTypeToString(value);
	}

	public Object convertStringToType(String value) {
		initConverter();
		return converter.convertStringToType(value);
	}

	public byte[] convertToStorage2(Object value) {
		initConverter();
		return converter.convertToNoSql(value);
	}

	public Object convertFromStorage2(byte[] data) {
		initConverter();
		return converter.convertFromNoSql(data);
	}

	

	public byte[] getColumnNameAsBytes() {
		if (columnAsBytes == null)
			columnAsBytes = StandardConverters.convertToBytes(columnName);
		return columnAsBytes;
	}

	public String getForeignKeyToExtensions() {
		return foreignKeyToExtensions;
	}

	public void setForeignKeyToExtensions(String foreignKeyToExtensions) {
		this.foreignKeyToExtensions = foreignKeyToExtensions;
	}

	public abstract void translateToColumn(InfoForIndex<TypedRow> info);

	@SuppressWarnings("unchecked")
	protected void removeIndexInfo(InfoForIndex<TypedRow> info, Object value, byte[] byteVal) {
		StorageTypeEnum storageType = getStorageType();
		Map<String, Object> fieldToValue = info.getFieldToValue();
		if (!isIndexed())
			return;
		else if (storageType == StorageTypeEnum.BYTES)
			throw new IllegalArgumentException(
					"Cannot do indexing for types that are stored as bytes at this time");
		else if (fieldToValue == null)
			return;

		addIndexRemoves(info, value, byteVal, storageType);
	}

	@SuppressWarnings("unchecked")
	private void addIndexRemoves(InfoForIndex<TypedRow> info, Object value,
			byte[] byteVal, StorageTypeEnum storageType) {
		RowToPersist row = info.getRow();
		Map<String, Object> fieldToValue = info.getFieldToValue();
		// if we are here, we are indexed, BUT if fieldToValue is null, then it
		// is a brand new entity and not a proxy
		Object originalValue = fieldToValue.get(columnName);
		if (originalValue == null)
			return;
		else if (originalValue.equals(value))
			return; // previous value is the same, yeah, nothing to do here!!!

		byte[] oldIndexedVal = this.convertToStorage2(originalValue);
		byte[] pk = row.getKey();
		List<IndexData> indexList = row.getIndexToRemove();
		// original value and current value differ so we need to remove from the
		// index
		addToIndexList(info, oldIndexedVal, storageType, pk, indexList);
	}

	@SuppressWarnings("unchecked")
	protected void removingThisEntity(InfoForIndex<TypedRow> info,
			List<IndexData> indexRemoves, byte[] pk) {
		StorageTypeEnum storageType = getStorageType();
		Map<Field, Object> fieldToValue = info.getFieldToValue();
		Object valueInDatabase = fieldToValue.get(columnName);
		byte[] oldIndexedVal = convertToStorage2(valueInDatabase);

		addToIndexList(info, oldIndexedVal, storageType, pk, indexRemoves);
	}

	private void addToIndexList(InfoForIndex<TypedRow> info,
			byte[] oldIndexedVal, StorageTypeEnum storageType, byte[] pk,
			List<IndexData> indexList) {
		List<PartitionTypeInfo> partTypes = info.getPartitions();
		for (PartitionTypeInfo part : partTypes) {
			// NOTE: Here if we partition by account and security both AND we
			// index both of those to, we only
			// want indexes of /entityCF/account/security/<securityid>
			// and /entityCF/security/account/<accountid>
			// It would not be useful at all to have
			// /entityCF/account/account/<accountid> since all the account ids
			// in that index row would be the same!!!!
			if (part.getColMeta() == this)
				continue;
			IndexData data = createAddIndexData(info.getColumnFamily(),
					oldIndexedVal, storageType, pk, part);
			indexList.add(data);
		}
	}

	@SuppressWarnings("unchecked")
	protected void addIndexInfo(InfoForIndex<TypedRow> info, Object value,
			byte[] byteVal) {
		StorageTypeEnum storageType = this.getStorageType();
		TypedRow entity = info.getEntity();
		RowToPersist row = info.getRow();
		Map<Field, Object> fieldToValue = info.getFieldToValue();

		if (!isIndexed())
			return;
		else if (storageType == StorageTypeEnum.BYTES)
			throw new IllegalArgumentException(
					"Cannot do indexing for types that are stored as bytes at this time");

		if (!isNeedPersist(entity, value, fieldToValue))
			return;

		// original value and current value differ so we need to persist new
		// value
		byte[] pk = row.getKey();
		List<IndexData> indexList = row.getIndexToAdd();

		addToIndexList(info, byteVal, storageType, pk, indexList);
	}

	private boolean isNeedPersist(TypedRow entity, Object value,
			Map<Field, Object> fieldToValue) {
		if (!(entity instanceof NoSqlTypedRowProxy))
			return true;
		Object originalValue = fieldToValue.get(columnName);
		if (value == null) // new value is null so nothing to persist
			return false;
		else if (value.equals(originalValue))
			return false; // previous value is the same, yeah, nothing to do
							// here!!!

		return true;
	}

	private IndexData createAddIndexData(String columnFamily, byte[] byteVal,
			StorageTypeEnum storageType, byte[] pk, PartitionTypeInfo part) {
		IndexData data = new IndexData();
		data.setColumnFamilyName(storageType.getIndexTableName());
		String rowKey = getIndexRowKey(part.getPartitionBy(),
				part.getPartitionId());
		data.setRowKey(rowKey);
		data.getIndexColumn().setIndexedValue(byteVal);
		data.getIndexColumn().setPrimaryKey(pk);
		data.getIndexColumn().setColumnName(getColumnName());
		return data;
	}

	public abstract void translateFromColumn(Row row, TypedRow inst);

	public abstract String fetchColumnValueAsString(TypedRow row);

	public DboTableMeta getOwner() {
		return owner;
	}

	public boolean isJodaType() {
		if (this.getClassType().getName().equals("org.joda.time.DateTime")
					|| this.getClassType().getName().equals("org.joda.time.LocalDateTime")
					|| this.getClassType().getName().equals("org.joda.time.LocalDate")
					|| this.getClassType().getName().equals("org.joda.time.LocalTime"))
			return true;

		return false;
	}

	public Map<String, String> getExtensions() {
		return extensions;
	}
}
