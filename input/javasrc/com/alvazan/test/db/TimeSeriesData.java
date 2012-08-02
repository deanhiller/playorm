package com.alvazan.test.db;

import com.alvazan.orm.api.base.Index;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.Indexed;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;

@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findById", query="select t from TABLE t where t.key = :key"),
	@NoSqlQuery(name="findByTemp", query="select t from TABLE t where t.temp = :temp")
})
public class TimeSeriesData {

	@Id(usegenerator=false)
	@Indexed
	private Long key;
	
	private String someName;

	@Indexed
	private float temp;
	
	public Long getKey() {
		return key;
	}

	public void setKey(Long key) {
		this.key = key;
	}

	public String getSomeName() {
		return someName;
	}

	public void setSomeName(String someName) {
		this.someName = someName;
	}

	public float getTemp() {
		return temp;
	}

	public void setTemp(float number) {
		this.temp = number;
	}

	public static TimeSeriesData findById(NoSqlEntityManager mgr, Long id) {
		Index<TimeSeriesData> index = mgr.getIndex(TimeSeriesData.class, "");
		Query<TimeSeriesData> query = index.getNamedQuery("findById");
		query.setParameter("key", id);
		return query.getSingleObject();
	}

	public static TimeSeriesData findByTemp(NoSqlEntityManager mgr, float f) {
		Index<TimeSeriesData> index = mgr.getIndex(TimeSeriesData.class, "");
		Query<TimeSeriesData> query = index.getNamedQuery("findByTemp");
		query.setParameter("temp", f);
		return query.getSingleObject();		
	}
	
}
