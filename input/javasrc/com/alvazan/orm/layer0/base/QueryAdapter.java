package com.alvazan.orm.layer0.base;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.exc.TooManyResultException;
import com.alvazan.orm.api.exc.TypeMismatchException;
import com.alvazan.orm.api.spi2.KeyValue;
import com.alvazan.orm.api.spi2.SpiQueryAdapter;
import com.alvazan.orm.api.spi2.meta.DboColumnMeta;
import com.alvazan.orm.api.spi2.meta.MetaQuery;
import com.alvazan.orm.api.spi2.meta.TypeInfo;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaField;
import com.alvazan.orm.impl.meta.data.MetaInfo;

public class QueryAdapter<T> implements Query<T> {

	@Inject
	private MetaInfo metaInfo;
	
	private MetaQuery<T> meta;
	private SpiQueryAdapter indexQuery;
	private BaseEntityManagerImpl mgr;
	private MetaClass<T> metaClass;

	public void setup(MetaQuery<T> meta, SpiQueryAdapter indexQuery, BaseEntityManagerImpl entityMgr, MetaClass<T> metaClass) {
		this.meta = meta;
		this.indexQuery = indexQuery;
		this.mgr = entityMgr;
		this.metaClass = metaClass;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void setParameter(String name, Object value) {
		//check parameter 
		
		TypeInfo typeInfo = meta.getMetaFieldByParameter(name);
		if(typeInfo==null){
			throw new IllegalArgumentException("parameter='" + name
					+ "' is not found in the query="+meta.getQuery());
		} else if(typeInfo.getConstantType() != null)
			throw new UnsupportedOperationException("not done here yet, need to validate constant type");

		DboColumnMeta metaFieldDbo = typeInfo.getColumnInfo();
		
		String columnName = metaFieldDbo.getColumnName();
		MetaField metaField = metaClass.getMetaFieldByCol(columnName);
		
		Field field = metaField.getField();
		Class fieldType = field.getType();
		//Are actual type will never be a primitive because of autoboxing.  When the param
		//is passed in, it becomes an Long, Integer, etc. so we need to convert here
		Class objectFieldType = DboColumnMeta.convertIfPrimitive(fieldType);
		if(value!=null){
			Class actualType = value.getClass();

			if(!objectFieldType.isAssignableFrom(actualType)){
				throw new TypeMismatchException("value [" + value
						+ "] is not the correct type for the parameter='"+name+"' from inspecting the Entity.  Type should be=["
						+ fieldType + "]");
			} 		
		}
	
		byte[] data = metaField.translateValue(value);
		
		indexQuery.setParameter(name, data);
	}

	@Override
	public T getSingleObject() {
		List<KeyValue<T>> results = getResultKeyValueList();
		if(results.size() > 1)
			throw new TooManyResultException("Too many results to call getSingleObject...call getResultList instead");
		else if(results.size() == 0)
			return null;
		return results.get(0).getValue();
	}

	@SuppressWarnings({"unchecked" })
	@Override
	public List<KeyValue<T>> getResultKeyValueList() {
		List<byte[]> primaryKeys = indexQuery.getResultList();
		//HERE we need to query the nosql database with the primary keys from the index
		List<KeyValue<T>> all = mgr.findAllImpl(metaClass, null, primaryKeys, meta.getQuery());
		return all;
	}

	@Override
	public List<T> getResultList() {
		List<KeyValue<T>> all = getResultKeyValueList();
		return getEntities(all);
	}
	
	private List<T> getEntities(List<KeyValue<T>> keyValues){
		List<T> entities = new ArrayList<T>();
		for(KeyValue<T> keyVal : keyValues) {
			if(keyVal.getValue() != null)
				entities.add(keyVal.getValue());
		}

		return entities;
	}

}
