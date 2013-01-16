package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlEmbeddable;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;

@NoSqlEmbeddable
public class EmbeddedEmail {
	private static final String MAIN = "main";

	@NoSqlId
	private String id;

	@NoSqlIndexed
	private String name;

	@NoSqlIndexed
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
