package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.Converter;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.meta.DboColumnEmbedSimpleMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;
import com.alvazan.orm.api.z8spi.meta.ReflectionUtil;
import com.alvazan.orm.api.z8spi.meta.RowToPersist;
import com.alvazan.orm.impl.meta.data.collections.SimpleAbstractCollection;
import com.alvazan.orm.impl.meta.data.collections.SimpleList;
import com.alvazan.orm.impl.meta.data.collections.SimpleMap;
import com.alvazan.orm.impl.meta.data.collections.SimpleSet;

@SuppressWarnings("rawtypes")
public class MetaEmbeddedSimple<OWNER, T> extends MetaAbstractField<OWNER> {

	private Converter converter;
	private Converter valueConverter;
	private DboColumnEmbedSimpleMeta metaDbo = new DboColumnEmbedSimpleMeta();
	
//	public void setup(DboTableMeta tableMeta, Field field2, String colName, Converter converter, boolean isIndexed, boolean isPartitioned) {
//		metaDbo.setup(tableMeta, colName, field2.getType(), isIndexed, isPartitioned);
//		super.setup(field2, colName);
//		this.converter = converter;
//	}
	public void setup(DboTableMeta t, Field field, String colName, Converter converter, Converter valConverter, Class<?> type, Class<?> valueType) {
		super.setup(field, colName);
		metaDbo.setup(t, colName, field.getType(), type, valueType);
		this.converter = converter;
		this.valueConverter = valConverter;
	}
	
	@Override
	public String toString() {
		return "MetaEmbeddedSimple [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType().getName()+ "), columnName=" + columnName + "]";
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
        Set<T> retVal = new SimpleSet<T>(converter, values);
        return retVal;
    }

	@SuppressWarnings({ "rawtypes" })
	private Map translateFromColumnMap(Row row,
			OWNER entity, NoSqlSession session) {
		byte[] bytes = StandardConverters.convertToBytes(columnName);
		Collection<Column> columns = row.columnByPrefix(bytes);

		Map<byte[], byte[]> theCols = new HashMap<byte[], byte[]>();
		//NOTE: Current implementation is just like a Set not a List in that it
		//cannot have repeats right now.  We could take the approach the column name
		//would be <prefix><index><pk> such that duplicates are allowed and when loaded
		//we would have to keep the index in the proxy so if removed, we could put the col name
		//back together so we could remove it and add the new one.  This is very complex though when 
		//it comes to removing one item and shifting all other column names by one(ie. lots of removes/adds)
		//so for now, just make everything Set like.
		for(Column col : columns) {
			byte[] colNameData = col.getName();
			byte[] value = col.getValue();
			//strip off the prefix to get the foreign key
			int pkLen = colNameData.length-bytes.length;
			byte[] pk = new byte[pkLen];
			for(int i = bytes.length; i < colNameData.length; i++) {
				pk[i-bytes.length] =  colNameData[i];
			}
			theCols.put(pk, value);
		}

		return new SimpleMap(converter, valueConverter, theCols);
	}
	
	private List<T> translateFromColumnList(Row row,
			OWNER entity, NoSqlSession session) {
		List<byte[]> values = MetaToManyField.parseColNamePostfix(columnName, row);
		List<T> retVal = new SimpleList<T>(converter, values);
		return retVal;
	}

	@Override
	public void translateToColumn(InfoForIndex<OWNER> info) {
		OWNER entity = info.getEntity();
		RowToPersist row = info.getRow();
		if(field.getType().equals(Map.class))
			translateToColumnMap(entity, row);
		else
			translateToColumnList(entity, row);
	}
	
	@SuppressWarnings("unchecked")
	private void translateToColumnMap(OWNER entity, RowToPersist row) {
		Map values = (Map) ReflectionUtil.fetchFieldValue(entity, field);
		if (values == null)
			values = new HashMap();
		Map toBeAdded = values; //all values in the list get added if not an OurAbstractCollection
		Collection<T> toBeRemoved = new ArrayList<T>();
		if(values instanceof SimpleMap) {
			SimpleMap coll = (SimpleMap)values;
			toBeRemoved = coll.getToBeRemoved();
			toBeAdded = coll.getToBeAdded();
		}

		translateToColumnMapImpl(toBeAdded, row, toBeRemoved);
	}

	private void translateToColumnMapImpl(Map toBeAdded, RowToPersist row, Collection<T> toBeRemoved) {
		//removes first
		for(T p : toBeRemoved) {
			byte[] name = formTheName(p);
			row.addEntityToRemove(name);
		}
		
		//now process all the existing columns (we can add same entity as many times as we like and it does not
		//get duplicated)
		for(Object key : toBeAdded.keySet()) {
			Object value = toBeAdded.get(key);
			
			byte[] name = formTheName(key);
			byte[] byteVal = valueConverter.convertToNoSql(value);
			Column c = new Column();
			c.setName(name);
			c.setValue(byteVal);
			
			row.getColumns().add(c);
		}
	}

	@SuppressWarnings("unchecked")
	private void translateToColumnList(OWNER entity, RowToPersist row) {
		Collection<T> values = (Collection<T>) ReflectionUtil.fetchFieldValue(entity, field);
		if (values == null)
			values = new ArrayList<T>();
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
