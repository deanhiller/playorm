package com.alvazan.orm.layer1.base;

import java.util.List;

import com.alvazan.orm.api.Query;
import com.alvazan.orm.api.TooManyResultException;
import com.alvazan.orm.impl.meta.MetaQuery;
import com.alvazan.orm.layer3.spi.index.SpiIndexQuery;

public class QueryAdapter<T> implements Query<T> {

	private MetaQuery<T> meta;
	private SpiIndexQuery indexQuery;

	public void setup(MetaQuery<T> meta, SpiIndexQuery indexQuery) {
		this.meta = meta;
		this.indexQuery = indexQuery;
	}
	
	@Override
	public void setParameter(String name, Object value) {
		indexQuery.setParameter(name, value);
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

	@SuppressWarnings("unchecked")
	@Override
	public List<T> getResultList() {
		return indexQuery.getResultList();
	}
}
