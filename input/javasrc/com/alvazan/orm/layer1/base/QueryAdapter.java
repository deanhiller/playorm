package com.alvazan.orm.layer1.base;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import com.alvazan.orm.api.base.KeyValue;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.exc.TooManyResultException;
import com.alvazan.orm.api.base.exc.TypeMismatchException;
import com.alvazan.orm.api.spi.index.SpiQueryAdapter;
import com.alvazan.orm.api.spi.layer2.MetaColumnDbo;
import com.alvazan.orm.api.spi.layer2.MetaQuery;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaField;
import com.alvazan.orm.impl.meta.data.MetaInfo;

public class QueryAdapter<T> implements Query<T> {

	@Inject
	private MetaInfo metaInfo;
	
	private MetaQuery<T> meta;
	private SpiQueryAdapter indexQuery;
	private BaseEntityManagerImpl session;
	private MetaClass<T> metaClass;

	public void setup(MetaQuery<T> meta, SpiQueryAdapter indexQuery, BaseEntityManagerImpl entityMgr, MetaClass<T> metaClass) {
		this.meta = meta;
		this.indexQuery = indexQuery;
		this.session = entityMgr;
		this.metaClass = metaClass;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void setParameter(String name, Object value) {
		//check parameter 
		MetaColumnDbo metaFieldDbo = meta.getMetaFieldByParameter(name);
		if(metaFieldDbo==null){
			throw new IllegalArgumentException("parameter='" + name
					+ "' is not found in query="+meta.getQuery());
		}
		
		MetaField metaField = metaClass.getMetaField(metaFieldDbo.getColumnName());
		Class fieldType = metaField.getFieldType();
		//Are actual type will never be a primitive because of autoboxing.  When the param
		//is passed in, it becomes an Long, Integer, etc. so we need to convert here
		Class objectFieldType = convertIfPrimitive(fieldType);
		Class actualType = value.getClass();
		
		if(!objectFieldType.isAssignableFrom(actualType)){
			throw new TypeMismatchException("value [" + value
					+ "] is not the correct type for the parameter='"+name+"'.  Type should be=["
					+ fieldType + "]");
		} 		
		
		String newValue = metaField.translateIfEntity(value);
		
		indexQuery.setParameter(name, newValue);
	}

	@SuppressWarnings("rawtypes")
	private Class convertIfPrimitive(Class fieldType) {
		if(long.class.equals(fieldType))
			return Long.class;
		else if(int.class.equals(fieldType))
			return Integer.class;
		else if(short.class.equals(fieldType))
			return Short.class;
		else if(byte.class.equals(fieldType))
			return Byte.class;
		else if(double.class.equals(fieldType))
			return Double.class;
		else if(float.class.equals(fieldType))
			return Float.class;
		else if(boolean.class.equals(fieldType))
			return Boolean.class;
		return fieldType;
	}

	@Override
	public T getSingleObject() {
		List<T> results = getResultList();
		if(results.size() > 1)
			throw new TooManyResultException("Too many results to call getSingleObject...call getResultList instead");
		else if(results.size() == 0)
			return null;
		return results.get(0);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List<T> getResultList() {
		List primaryKeys = indexQuery.getResultList();
		
		//HERE we need to query the nosql database with the primary keys from the index
		List<KeyValue<T>> all = session.findAll(metaClass.getMetaClass(), primaryKeys);

		List<T> entities = new ArrayList<T>();
		for(KeyValue<T> keyVal : all) {
			entities.add(keyVal.getValue());
		}
		
		return entities;
	}
}
