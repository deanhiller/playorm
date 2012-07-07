package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.Indexed;

public class AccountSuper {

	@Indexed
	private int someField;
	@Indexed
	private Boolean isActive;
	
	public int getSomeField() {
		return someField;
	}

	public void setSomeField(int someField) {
		this.someField = someField;
	}

	public Boolean getIsActive() {
		return isActive;
	}

	public void setIsActive(Boolean indexedColumn) {
		this.isActive = indexedColumn;
	}
	
}
