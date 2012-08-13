package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.List;

import com.alvazan.orm.api.spi2.IndexData;
import com.alvazan.orm.api.spi2.InfoForIndex;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi2.ReflectionUtil;
import com.alvazan.orm.api.spi2.RowToPersist;
import com.alvazan.orm.api.spi2.meta.DboColumnCommonMeta;
import com.alvazan.orm.api.spi2.meta.DboColumnMeta;
import com.alvazan.orm.api.spi2.meta.StorageTypeEnum;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.api.spi3.db.conv.Converter;

public class MetaCommonField<OWNER> extends MetaAbstractField<OWNER> {
	
	private Converter converter;
	private DboColumnCommonMeta metaDbo = new DboColumnCommonMeta();
	
	public DboColumnMeta getMetaDbo() {
		return metaDbo;
	}
	
	@Override
	public String toString() {
		return "MetaCommonField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType()+ "), columnName=" + columnName + "]";
	}

	public void translateFromColumn(Row row, OWNER entity, NoSqlSession session) {
		Column column = row.getColumn(getMetaDbo().getColumnNameAsBytes());
		
		if(column == null) {
			column = new Column();
		}
		
		Object value = converter.convertFromNoSql(column.getValue());
		ReflectionUtil.putFieldValue(entity, field, value);
	}
	@Override
	public void translateToColumn(InfoForIndex<OWNER> info) {
		OWNER entity = info.getEntity();
		RowToPersist row = info.getRow();
		
		Column col = new Column();
		row.getColumns().add(col);

		Object value = ReflectionUtil.fetchFieldValue(entity, field);
		byte[] byteVal = translateValue(value);
		col.setName(columnName.getBytes());
		col.setValue(byteVal);
		
		StorageTypeEnum storageType = metaDbo.getStorageType();
		addIndexInfo(info, value, byteVal, storageType);
		removeIndexInfo(info, value, byteVal, storageType);
	}
	
	@Override
	public void removingEntity(InfoForIndex<OWNER> info, List<IndexData> indexRemoves, byte[] pk) {
		StorageTypeEnum storageType = metaDbo.getStorageType();
		removingThisEntity(info, indexRemoves, pk, storageType);
	}
	
	@Override
	public byte[] translateValue(Object value) {
		return converter.convertToNoSql(value);
	}
	
	public void setup(Field field2, String colName, Converter converter, String indexPrefix) {
		metaDbo.setup(colName, field2.getType(), indexPrefix);
		super.setup(field2, colName);
		this.converter = converter;
	}

	@Override
	protected Object unwrapIfNeeded(Object value) {
		return value; //no need to unwrap common fields
	}

}
