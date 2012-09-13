package com.alvazan.orm.layer0.base;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.exc.StorageMissingEntitesException;
import com.alvazan.orm.api.exc.TooManyResultException;
import com.alvazan.orm.api.exc.TypeMismatchException;
import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.SpiMetaQuery;
import com.alvazan.orm.api.z5api.SpiQueryAdapter;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.TypeInfo;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaField;
import com.alvazan.orm.impl.meta.data.MetaInfo;
import com.alvazan.orm.layer3.typed.IterableIndex;
import com.alvazan.orm.layer3.typed.IterableProxy;

public class QueryAdapter<T> implements Query<T> {

	private static final Logger log = LoggerFactory.getLogger(QueryAdapter.class);
	@Inject
	private MetaInfo metaInfo;
	
	private SpiMetaQuery meta;
	private SpiQueryAdapter indexQuery;
	private BaseEntityManagerImpl mgr;
	private Integer batchSize = 500;
	private MetaClass<T> mainMetaClass;
	private ViewInfo mainView;

	public void setup(MetaClass<T> target, SpiMetaQuery metaQuery, SpiQueryAdapter indexQuery, BaseEntityManagerImpl entityMgr) {
		this.mainMetaClass = target;
		this.meta = metaQuery;
		this.indexQuery = indexQuery;
		this.mgr = entityMgr;
		this.mainView = metaQuery.getTargetViews().get(0);
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
		String colFamily = metaFieldDbo.getOwner().getColumnFamily();
		String columnName = metaFieldDbo.getColumnName();
		MetaClass metaClass = metaInfo.getMetaClass(colFamily);
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
		Cursor<KeyValue<T>> results = getResults();
		if(!results.next())
			return null;
		KeyValue<T> kv = results.getCurrent();
		if(results.next())
			throw new TooManyResultException("Too many results to call getSingleObject...call getResultList instead");
		return kv.getValue();
	}

	@Override
	public Cursor<KeyValue<T>> getResults() {
		DirectCursor<IndexColumnInfo> indice = indexQuery.getResultList();
		
		//BIG NOTE: Here, we could return all the keys from the join so we can eagerly fetch other entities as well
		//instead of waiting for the user to loop through those entities AND if user accesses those entites, we could block
		//while the load is happening too in the background
		Iterable<byte[]> keys = new IterableIndex(mainView, indice);
		
		String query = meta.getQuery();
		AbstractCursor<KeyValue<T>> results = mgr.findAllImpl2(mainMetaClass, keys, query, batchSize);

		return results;
	}

	@Override
	public List<T> getResultList(int firstResult, Integer maxResults) {
		AbstractCursor<KeyValue<T>> all = (AbstractCursor<KeyValue<T>>) getResults();
		List<T> foundElements = new ArrayList<T>();
		try {
			return getEntities(all, foundElements, firstResult, maxResults);
		} catch(RowNotFoundException e) {
			log.trace("converting row not found into stored entities missing", e);
			throw new StorageMissingEntitesException(foundElements, "Your index refers to rows that no longer exist in the nosql store", e);
		}
	}
	
	private List<T> getEntities(AbstractCursor<KeyValue<T>> all, List<T> foundElements, int firstResult, Integer maxResults){
		List<T> entities = new ArrayList<T>();
		RowNotFoundException exc = null;
		
		int counter = 0;
		while(true) {
			Holder<KeyValue<T>> holder = all.nextImpl();
			if(holder == null)
				break;
			KeyValue<T> keyVal = holder.getValue();
			if(counter < firstResult)
				continue; //skip it
			else if(maxResults != null && counter >= firstResult+maxResults)
				break; //we are done with filling in our list
			
			try {
				entities.add(keyVal.getValue());
				foundElements.add(keyVal.getValue());
			} catch(RowNotFoundException e) {
				if(exc == null)
					exc = e;//set the first one only
			}
			counter++;
		}

		if(exc != null) {
			log.trace("converting row not found into stored entities missing", exc);
			throw new StorageMissingEntitesException(foundElements, "Your index refers to rows that no longer exist in the nosql store(use getResults method instead to avoid this exception until you call getValue on actual element)", exc);
		}
		return entities;
	}

	@Override
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
		this.indexQuery.setBatchSize(batchSize);
	}

	@Override
	public Iterable<KeyValue<T>> getResultsIter() {
		Cursor<KeyValue<T>> cursor = getResults();
		Iterable<KeyValue<T>> proxy = new IterableProxy<T>(cursor);
		return proxy;
	}

}
