package com.alvazan.orm.api.z8spi.meta;

import java.util.Arrays;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;

@SuppressWarnings({ "rawtypes" })
@NoSqlDiscriminatorColumn(value="id")
public class DboColumnIdMeta extends DboColumnMeta {

	protected String columnValueType;

	protected transient byte[] cachedKeyPrefix;
	
	public byte[] formVirtRowKey(byte[] byteVal) {
		if(!owner.isVirtualCf())
			return byteVal;
		else if(cachedKeyPrefix == null) {
			initCachedKey();
		}

		byte[] newKey = new byte[cachedKeyPrefix.length+byteVal.length];
		for(int i = 0; i < cachedKeyPrefix.length; i++) {
			newKey[i] = cachedKeyPrefix[i];
		}
		
		for(int i = 0; i < byteVal.length; i++) {
			newKey[i+cachedKeyPrefix.length] = byteVal[i];
		}
		return newKey;
	}
	
	public byte[] unformVirtRowKey(byte[] virtKey) {
		if(!owner.isVirtualCf())
			return virtKey;
		else if(cachedKeyPrefix == null)
			initCachedKey();
		
		int virtKeyLen = virtKey[0] & 0xFF;
		byte[] realKey = new byte[virtKey.length-1-virtKeyLen];
		for(int i = 0; i < realKey.length;i++) {
			realKey[i] = virtKey[1+virtKeyLen+i];
		}
		return realKey;
	}
	
	public static String fetchTableNameIfVirtual(byte[] virtKey) {
		int tableLen = virtKey[0] & 0xFF;
		byte[] bytesName = Arrays.copyOfRange(virtKey, 1, tableLen+1);
		String s = new String(bytesName);
		return s;
	}

	private void initCachedKey() {
		String cf = owner.getColumnFamily();
		byte[] asBytes = StandardConverters.convertToBytes(cf);
		
		int len = asBytes.length;
		byte prefix = (byte) len;
		cachedKeyPrefix = new byte[asBytes.length+1];
		cachedKeyPrefix[0] = prefix;
		for(int i = 0; i < asBytes.length; i++) {
			cachedKeyPrefix[i+1] = asBytes[i];
		}
	}
	
	@Override
	public String getIndexTableName() {
		return getStorageType().getIndexTableName();
	}

	public Class getClassType() {
		return ConverterUtil.classForName(columnValueType);
	}

	public StorageTypeEnum getStorageType() {
		Class fieldType = getClassType();
		return ConverterUtil.getStorageType(fieldType);
	}
	
	public void setup(DboTableMeta owner, String colName, Class valuesType, boolean isIndexed) {
		if(owner.getColumnFamily() == null)
			throw new IllegalArgumentException("The owner passed in must have a non-null column family name");
		else if(colName == null)
			throw new IllegalStateException("colName parameter must not be null");
		byte[] asBytes = StandardConverters.convertToBytes(owner.getColumnFamily());
		if(asBytes.length > 255)
			throw new IllegalArgumentException("Column family names converted using utf8 must be less than 255 bytes.  your col family="+owner.getColumnFamily()+" is too long");

		this.owner = owner;
		setColumnName(colName);
		//NOTE: We don't call super.setup here BECAUSE super.setup calls owner.addColumn and this is the row key
		//and so we call owner.setRowKeyMeta here instead
		owner.setRowKeyMeta(this);
		id = owner.getColumnFamily()+":"+getColumnName();
		
		Class newType = ConverterUtil.translateType(valuesType);
		this.columnValueType = newType.getName();
		this.isIndexed = isIndexed;
	}

	@Override
	public void translateToColumn(InfoForIndex<TypedRow> info) {
		TypedRow entity = info.getEntity();
		RowToPersist row = info.getRow();

		Object id = entity.getRowKey();
		if(id == null)
			throw new IllegalArgumentException("rowKey cannot be null");
		
		byte[] byteVal = convertToStorage2(id);
		byte[] virtualKey = formVirtRowKey(byteVal);
		row.setKeys(byteVal, virtualKey);
		
		addIndexInfo(info, id, byteVal);
		//NOTICE: there is no call to remove because if an id is changed, it is a new entity and we only need to add index not remove since
		//the old entity was not deleted during this save....we remove from index on remove of old entity only
	}

	public void translateFromColumn(Row row, TypedRow entity) {
		byte[] virtualKey = row.getKey();
		byte[] notVirtKey = unformVirtRowKey(virtualKey);
		Object pk = convertFromStorage2(notVirtKey);
		entity.setRowKey(pk);
	}

	@Override
	public boolean isPartitionedByThisColumn() {
		return false;
	}

	@Override
	public String fetchColumnValueAsString(TypedRow row) {
		throw new UnsupportedOperationException("only used by partitioning and can't partition a primary key column....that is kind of useless");
	}
}
