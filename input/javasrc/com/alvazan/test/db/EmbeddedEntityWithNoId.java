package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlEmbeddable;

@NoSqlEmbeddable
public class EmbeddedEntityWithNoId {

	private String id;

	private String name;

	private String type;

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

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

}
