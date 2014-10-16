package com.alvazan.orm.api.z8spi.meta;

import java.util.Collection;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;

@NoSqlDiscriminatorColumn(value="embedsimple")
public class DboColumnEmbedSimpleMeta extends DboColumnMeta {

	private String collectionType;
	private String itemType;
	private String valueType;
	
	@SuppressWarnings("rawtypes")
	public void setup(DboTableMeta t, String colPrefix, Class collectionType, Class itemType, Class valueType, boolean isIndexed) {
		super.setup(t, colPrefix, isIndexed);
		this.collectionType = collectionType.getName();
		Class newType = ConverterUtil.translateType(itemType);
		this.itemType = newType.getName();
		if(valueType != null) {
			Class newValType = ConverterUtil.translateType(valueType);
			this.valueType = newValType.getName();
		}
	}
	
	@Override
	public boolean isPartitionedByThisColumn() {
		return false;
	}

	@Override
	public String getIndexTableName() {
		return getStorageType().getIndexTableName();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class getClassType() {
	    return ConverterUtil.classForName(this.itemType);
	}

	@Override
	public StorageTypeEnum getStorageType() {
		Class storageTypeClass = ConverterUtil.classForName(itemType);
		StorageTypeEnum storageType = ConverterUtil.getStorageType(storageTypeClass);
		return storageType;
	}

	@Override
	public void translateToColumn(InfoForIndex<TypedRow> info) {
		TypedRow entity = info.getEntity();
		RowToPersist row = info.getRow();
		Column col = new Column();
		TypedColumn column = entity.getColumn(getColumnName());

		byte[] byteVal = convertToStorage2(column.getValue());
		byte[] prefix = StandardConverters.convertToBytes(getColumnName());

		byte[] pkData = byteVal;
		byte[] name = new byte[prefix.length + pkData.length];
		for(int i = 0; i < name.length; i++) {
			if(i < prefix.length)
				name[i] = prefix[i];
			else
				name[i] = pkData[i-prefix.length];
		}
		col.setName(name);
		row.getColumns().add(col);
		Object primaryKey = column.getValue();
		addIndexInfo(info, primaryKey, byteVal);
		removeIndexInfo(info, primaryKey, byteVal);
	}

	@Override
	public void translateFromColumn(Row row, TypedRow inst) {
		translateFromColumnList(row, inst);
	}

	private void translateFromColumnList(Row row, TypedRow entity) {
		parseOutKeyList(row, entity);
	}

	private void parseOutKeyList(Row row, TypedRow entity) {
		String columnName = getColumnName();
		byte[] namePrefix = StandardConverters.convertToBytes(columnName);
		Collection<Column> columns = row.columnByPrefix(namePrefix);
		for(Column col : columns) {
			byte[] fullName = col.getName();
			int pkLen = fullName.length-namePrefix.length;
			byte[] fk = new byte[pkLen];
			for(int i = namePrefix.length; i < fullName.length; i++) {
				fk[i-namePrefix.length] =  fullName[i];
			}
			entity.addColumn(this, fullName, namePrefix, fk, col.getValue(), col.getTimestamp());
		}
	}

	@Override
	public String fetchColumnValueAsString(TypedRow row) {
        TypedColumn typedCol = row.getColumn(getColumnName());
        Object value = typedCol.getValue();
        return convertTypeToString(value);
	}
}
