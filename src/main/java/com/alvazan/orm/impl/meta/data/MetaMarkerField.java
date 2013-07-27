package com.alvazan.orm.impl.meta.data;

import java.util.List;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.meta.DboColumnMarkerMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;

/** field type used for entity marker support. It has constant empty value */ 
public class MetaMarkerField<OWNER> extends MetaAbstractField<OWNER> {

	/** field value */
	private final static byte[] marker = new byte[0];
	/** column name */
	private final static byte[] column = new byte[] { '_' };

	@Override
	public void translateFromColumn(Row column, OWNER entity,
			NoSqlSession session) {
		/* we do not load field from database because marker field is constant */
		/* and we do not modify entity because marker field is virtual */
	}

	@Override
	public void translateToColumn(InfoForIndex<OWNER> info) {
		Column col = new Column();
		col.setName(column);
		col.setValue(marker);
		info.getRow().getColumns().add(col);
	}

	@Override
	public void removingEntity(InfoForIndex<OWNER> info,
			List<IndexData> indexRemoves, byte[] rowKey) {
		/* since there are no indexes, nothing needs to be done */
	}

	@Override
	public byte[] translateValue(Object value) {
		return marker;
	}

	@Override
	public Object fetchField(Object entity) {
		/* this field is virtual */
		return marker;
	}

	@Override
	public String translateToString(Object fieldsValue) {
		return "";
	}

	@Override
	public DboColumnMeta getMetaDbo() {
		DboColumnMarkerMeta meta = new DboColumnMarkerMeta();
		return meta;
	}

	@Override
	protected Object unwrapIfNeeded(Object value) {
		// no need to unwrap
		return value;
	}

}
