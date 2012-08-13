package com.alvazan.orm.api.spi2.meta;


@SuppressWarnings("rawtypes")
public class MetaAndIndexTuple {

	private MetaQuery metaQuery;
	private String indexName;
	public MetaQuery getMetaQuery() {
		return metaQuery;
	}
	public void setMetaQuery(MetaQuery metaQuery) {
		this.metaQuery = metaQuery;
	}
	public String getIndexName() {
		return indexName;
	}
	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}
	
	
}
