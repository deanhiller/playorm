package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;

@NoSqlEntity
/** Marker entity. It has no fields except primary key. */
public class Marker {

	@NoSqlId
	private String id; 
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
