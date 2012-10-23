package com.alvazan.test.db;


import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.eaio.uuid.UUID;

@NoSqlEntity
public class EntityWithUUIDKey {

	@NoSqlId
	private UUID id;
	
	private String something;

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getSomething() {
		return something;
	}

	public void setSomething(String something) {
		this.something = something;
	}
	
	
}
