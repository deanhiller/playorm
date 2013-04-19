package com.alvazan.test.db;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;
@NoSqlQueries({
	   @NoSqlQuery(name ="findByField", query = "select u from TABLE as u where :someField = u.someField")
	})
public class AccountSuper {

	@NoSqlIndexed
	private int someField;
	@NoSqlIndexed
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
	
	public static AccountSuper findByField(NoSqlEntityManager mgr, int field) {
		Query<AccountSuper> query = mgr.createNamedQuery(AccountSuper.class, "findByField");
		query.setParameter("someField", field);
		return query.getSingleObject();
	}
}
