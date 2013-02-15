package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.List;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.Converter;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;
import com.alvazan.orm.api.z8spi.meta.DboColumnCommonMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;
import com.alvazan.orm.api.z8spi.meta.ReflectionUtil;
import com.alvazan.orm.api.z8spi.meta.RowToPersist;

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
		byte[] colBytes = StandardConverters.convertToBytes(columnName);
		col.setName(colBytes);
		col.setValue(byteVal);
		
		StorageTypeEnum storageType = metaDbo.getStorageType();
		addIndexInfo(info, value, byteVal, storageType);
		removeIndexInfo(info, value, byteVal, storageType);
	}
	
	@Override
	public Object fetchField(Object entity) {
		return ReflectionUtil.fetchFieldValue(entity, field);
	}

	@Override
	public String translateToString(Object fieldsValue) {
		return converter.convertTypeToString(fieldsValue);
	}
	
	@Override
	public void removingEntity(InfoForIndex<OWNER> info, List<IndexData> indexRemoves, byte[] pk) {
		removingThisEntity(info, indexRemoves, pk);
	}
	
	@Override
	public byte[] translateValue(Object value) {
		return converter.convertToNoSql(value);
	}
	
	public void setup(DboTableMeta tableMeta, Field field2, String colName, Converter converter, boolean isIndexed, boolean isPartitioned) {
		metaDbo.setup(tableMeta, colName, field2.getType(), isIndexed, isPartitioned);
		super.setup(field2, colName);
		this.converter = converter;
	}

	@Override
	protected Object unwrapIfNeeded(Object value) {
		return value; //no need to unwrap common fields
	}

}
