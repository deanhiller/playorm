package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.List;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.Converters.IntConverter;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnTTLMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;
import com.alvazan.orm.api.z8spi.meta.ReflectionUtil;

public class MetaTTLField<OWNER> extends MetaAbstractField<OWNER> {

	private DboColumnTTLMeta metaDbo = new DboColumnTTLMeta();
	protected Field field;
	protected String columnName;
	private IntConverter converter = new IntConverter();

	public void setup(DboTableMeta owner, Field field, String colName, boolean indexed) {
		metaDbo.setup(owner, colName, indexed);
		this.field = field;
		this.field.setAccessible(true);
		this.columnName = colName;
	}

	@Override
	public DboColumnMeta getMetaDbo() {
		return metaDbo;
	}

	@Override
	public String toString() {
		return "MetaTTL ["+ field.getDeclaringClass().getName()+"."+field.getName()+"]";
	}	

	@Override
	public void translateFromColumn(Row row, OWNER entity, NoSqlSession session) {
		if( row.getColumns().isEmpty() )
			ReflectionUtil.putFieldValue(entity, field, new Integer(0));
		else {
			Column col = row.getColumns().iterator().next();
			ReflectionUtil.putFieldValue(entity, field, col.getTtl());
		}
	}

	@Override
	public void translateToColumn(InfoForIndex<OWNER> info) {
		OWNER entity = info.getEntity();
		Object value = ReflectionUtil.fetchFieldValue(entity, field);

		info.getRow().setTtl((Integer)value);
	}

	@Override
	public void removingEntity(InfoForIndex<OWNER> info,
			List<IndexData> indexRemoves, byte[] rowKey) {
		// we do not allow index on TTL, nothing needs be done
	}

	@Override
	public byte[] translateValue(Object value) {
		return converter.convertToNoSql(value);
	}

	@Override
	public Object fetchField(Object entity) {
		return ReflectionUtil.fetchFieldValue(entity, field);
	}

	@Override
	public String translateToString(Object fieldsValue) {
		return fieldsValue.toString();
	}

	@Override
	protected Object unwrapIfNeeded(Object value) {
		return value;
	}
}
