package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.Converter;
import com.alvazan.orm.api.z8spi.meta.DboColumnEmbedSimpleMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;
import com.alvazan.orm.api.z8spi.meta.ReflectionUtil;
import com.alvazan.orm.api.z8spi.meta.RowToPersist;
import com.alvazan.orm.impl.meta.data.collections.OurAbstractCollection;
import com.alvazan.orm.impl.meta.data.collections.SimpleAbstractCollection;
import com.alvazan.orm.impl.meta.data.collections.SimpleList;

@SuppressWarnings("rawtypes")
public class MetaEmbeddedSimple<OWNER, T> extends MetaAbstractField<OWNER> {

	private Converter converter;
	private DboColumnEmbedSimpleMeta metaDbo = new DboColumnEmbedSimpleMeta();
	
//	public void setup(DboTableMeta tableMeta, Field field2, String colName, Converter converter, boolean isIndexed, boolean isPartitioned) {
//		metaDbo.setup(tableMeta, colName, field2.getType(), isIndexed, isPartitioned);
//		super.setup(field2, colName);
//		this.converter = converter;
//	}
	public void setup(DboTableMeta t, Field field, String colName, Converter converter, Class<?> type) {
		super.setup(field, colName);
		metaDbo.setup(t, colName, field.getType(), type);
		this.converter = converter;
	}
	
	@Override
	public void translateFromColumn(Row row, OWNER entity,
			NoSqlSession session) {
		Object proxy;
		if(field.getType().equals(Map.class))
			proxy = translateFromColumnMap(row, entity, session);
		else if(field.getType().equals(Collection.class)
				|| field.getType().equals(List.class))
			proxy = translateFromColumnList(row, entity, session);
		else if(field.getType().equals(Set.class))
			proxy = translateFromColumnSet(row, entity, session);
		else
			throw new RuntimeException("bug, we do not support type="+field.getType());
			
		ReflectionUtil.putFieldValue(entity, field, proxy);
	}

	private Object translateFromColumnSet(Row row, OWNER entity,
			NoSqlSession session) {
		List<byte[]> values = MetaToManyField.parseColNamePostfix(columnName, row);
		return null;
	}

	@SuppressWarnings({ "rawtypes" })
	private Map translateFromColumnMap(Row row,
			OWNER entity, NoSqlSession session) {
		List<byte[]> values = MetaToManyField.parseColNamePostfix(columnName, row);
		return null;
	}
	
	private List<T> translateFromColumnList(Row row,
			OWNER entity, NoSqlSession session) {
		List<byte[]> values = MetaToManyField.parseColNamePostfix(columnName, row);
		List<T> retVal = new SimpleList<T>(converter, values);
		return retVal;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void translateToColumn(InfoForIndex<OWNER> info) {
		OWNER entity = info.getEntity();
		RowToPersist row = info.getRow();
		
		Collection<T> values = (Collection<T>) ReflectionUtil.fetchFieldValue(entity, field);
		Collection<T> toBeAdded = values; //all values in the list get added if not an OurAbstractCollection
		Collection<T> toBeRemoved = new ArrayList<T>();
		if(values instanceof SimpleAbstractCollection) {
			SimpleAbstractCollection<T> coll = (SimpleAbstractCollection<T>)values;
			toBeRemoved = coll.getToBeRemoved();
			toBeAdded = coll.getToBeAdded();
		}

		translateToColumnImpl(toBeAdded, row, toBeRemoved);
	}

	private void translateToColumnImpl(Collection<T> toBeAdded, RowToPersist row, Collection<T> toBeRemoved) {
		//removes first
		for(T p : toBeRemoved) {
			byte[] name = formTheName(p);
			row.addEntityToRemove(name);
		}
		
		//now process all the existing columns (we can add same entity as many times as we like and it does not
		//get duplicated)
		for(T val : toBeAdded) {
			byte[] name = formTheName(val);
			Column c = new Column();
			c.setName(name);
			
			row.getColumns().add(c);
		}
	}
	
	private byte[] formTheName(Object value) {
		byte[] postFix = converter.convertToNoSql(value);
		return MetaToManyField.formTheNameImpl(columnName, postFix);
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

	@Override
	public void removingEntity(InfoForIndex<OWNER> info,
			List<IndexData> indexRemoves, byte[] rowKey) {
	}

}
