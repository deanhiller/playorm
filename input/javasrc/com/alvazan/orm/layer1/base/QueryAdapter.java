package com.alvazan.orm.layer1.base;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.api.KeyValue;
import com.alvazan.orm.api.Query;
import com.alvazan.orm.api.TooManyResultException;
import com.alvazan.orm.api.TypeMismatchException;
import com.alvazan.orm.impl.meta.MetaQuery;
import com.alvazan.orm.impl.meta.MetaQueryFieldInfo;
import com.alvazan.orm.layer3.spi.index.SpiQueryAdapter;

public class QueryAdapter<T> implements Query<T> {

	private MetaQuery<T> meta;
	private SpiQueryAdapter indexQuery;
	private BaseEntityManagerImpl session;

	public void setup(MetaQuery<T> meta, SpiQueryAdapter indexQuery, BaseEntityManagerImpl entityMgr) {
		this.meta = meta;
		this.indexQuery = indexQuery;
		this.session = entityMgr;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void setParameter(String name, Object value) {
		//check parameter 
		MetaQueryFieldInfo metaField = meta.getMetaFieldByParameter(name);
		if(metaField==null){
			throw new IllegalArgumentException("paraMeterName [" + name
					+ "] is not find for ");
		}
		Class fieldType = metaField.getFieldType();
		
		if(!fieldType.isAssignableFrom(value.getClass())){
			throw new TypeMismatchException("value [" + value
					+ "] is not match for paraMeterName which should be ["
					+ fieldType + "]");
		} 		
		
		//We need to just get the entities id for the query if it is an
		//entity...
		String newValue = metaField.translateIfEntity(value);
		
		indexQuery.setParameter(name, newValue);
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
		List<KeyValue<T>> all = session.findAll(meta.getMetaClass().getMetaClass(), primaryKeys);

		List<T> entities = new ArrayList<T>();
		for(KeyValue<T> keyVal : all) {
			entities.add(keyVal.getValue());
		}
		
		return entities;
	}
}
