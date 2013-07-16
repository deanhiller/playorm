package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;

public class DGPrefixedAttribute implements Cloneable {
	@NoSqlId(usegenerator = false)
	@NoSqlIndexed
	// @Field
	private String id;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
