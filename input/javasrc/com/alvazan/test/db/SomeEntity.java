package com.alvazan.test.db;

import com.alvazan.orm.api.Index;
import com.alvazan.orm.api.Query;
import com.alvazan.orm.api.anno.Id;
import com.alvazan.orm.api.anno.NoSqlEntity;
import com.alvazan.orm.api.anno.NoSqlQuery;

@NoSqlEntity
@NoSqlQuery(name="findById", query="select ENTITY e FROM TABLE where e.id=:id")
public class SomeEntity {

	@Id
	private String id;
	
	private String name;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public static SomeEntity findByKey(Index<SomeEntity> index, String key) {
		Query<SomeEntity> query = index.getNamedQuery("findById");
		query.setParameter("id", key);
		return query.getSingleObject();
	}
}
