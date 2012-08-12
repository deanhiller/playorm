package com.alvazan.orm.api.spi2;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.spi3.db.Column;

@NoSqlDiscriminatorColumn(value="generic")
public class DboColumnCommonMeta extends DboColumnMeta {

	private String columnValueType;
	private String indexPrefix;

	@SuppressWarnings("rawtypes")
	public void setup(String colName, Class valuesType, String indexPrefix) {
		Class newType = translateType(valuesType);
		this.columnName = colName;
		this.columnValueType = newType.getName();
		this.indexPrefix = indexPrefix;
	}
	
	public String getIndexPrefix() {
		return indexPrefix;
	}

	@Override
	public boolean isIndexed() {
		if(indexPrefix == null)
			return false;
		return true;
	}
	
	@SuppressWarnings("rawtypes")
	public Class getClassType() {
		return classForName(columnValueType);
	}

	@SuppressWarnings("rawtypes")
	public StorageTypeEnum getStorageType() {
		Class fieldType = getClassType();
		return getStorageType(fieldType);
	}

	public void translateToColumn(InfoForIndex<TypedRow> info) {
		TypedRow typedRow = info.getEntity();
		RowToPersist row = info.getRow();
		
		Column col = new Column();
		row.getColumns().add(col);

		TypedColumn typedCol = typedRow.getColumn(columnName);
		Object value = typedCol.getValue();
		byte[] byteVal = convertToStorage2(value);
		col.setName(getColumnNameAsBytes());
		col.setValue(byteVal);
		
		StorageTypeEnum storageType = this.getStorageType();
		addIndexInfo(info, value, byteVal, storageType);
		removeIndexInfo(info, value, byteVal, storageType);
	}
}
