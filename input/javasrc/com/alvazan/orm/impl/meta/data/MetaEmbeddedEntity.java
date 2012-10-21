package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.List;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.meta.DboColumnEmbedMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;

public class MetaEmbeddedEntity<OWNER, PROXY> extends MetaAbstractField<OWNER> {

	private DboColumnEmbedMeta metaDbo = new DboColumnEmbedMeta();
	private MetaAbstractClass<PROXY> classMeta;

	public void setup(DboTableMeta t, Field field, String colName, MetaAbstractClass<PROXY> fkMeta) {
		DboTableMeta fkToTable = fkMeta.getMetaDbo();
		metaDbo.setup(t, colName, fkToTable);
		super.setup(field, colName);
		this.classMeta = fkMeta;
		
	}

	@Override
	public void translateFromColumn(Row column, OWNER entity,
			NoSqlSession session) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void translateToColumn(InfoForIndex<OWNER> info) {
		
	}

	@Override
	public void removingEntity(InfoForIndex<OWNER> info,
			List<IndexData> indexRemoves, byte[] rowKey) {
		throw new UnsupportedOperationException();		
	}

	@Override
	public byte[] translateValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object fetchField(Object entity) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String translateToString(Object fieldsValue) {
		throw new UnsupportedOperationException();
	}

	@Override
	public DboColumnMeta getMetaDbo() {
		return metaDbo;
	}

	@Override
	protected Object unwrapIfNeeded(Object value) {
		throw new UnsupportedOperationException();
	}
	
	

}
