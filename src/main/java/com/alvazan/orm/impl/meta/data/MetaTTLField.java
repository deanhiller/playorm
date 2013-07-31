package com.alvazan.orm.impl.meta.data;

import java.util.List;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;

public class MetaTTLField<OWNER> extends MetaAbstractField<OWNER> {

	@Override
	public void translateFromColumn(Row column, OWNER entity,
			NoSqlSession session) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void translateToColumn(InfoForIndex<OWNER> info) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removingEntity(InfoForIndex<OWNER> info,
			List<IndexData> indexRemoves, byte[] rowKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] translateValue(Object value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object fetchField(Object entity) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String translateToString(Object fieldsValue) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DboColumnMeta getMetaDbo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Object unwrapIfNeeded(Object value) {
		// TODO Auto-generated method stub
		return null;
	}

}
