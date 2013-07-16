package com.alvazan.test.db;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;

@NoSqlDiscriminatorColumn(value="sub1")
@NoSqlQueries({
   @NoSqlQuery(name="findAll", query="select s from TABLE as s"),
   @NoSqlQuery(name ="findById", query = "select u from TABLE as u where :id = u.id"),
   @NoSqlQuery(name="findByName", query="select s from TABLE as s where s.name = :name")
})
public class NonVirtSub1 extends NonVirtSuper {

	@NoSqlIndexed
	private String name;
	
	private String diff;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDiff() {
		return diff;
	}

	public void setDiff(String diff) {
		this.diff = diff;
	}
	
	public static Cursor<KeyValue<NonVirtSub1>> findAll(NoSqlEntityManager mgr) {
		Query<NonVirtSub1> query = mgr.createNamedQuery(NonVirtSub1.class, "findAll");
		return query.getResults("name");
	}

	public static NonVirtSub1 findByName(NoSqlEntityManager mgr, String name) {
		Query<NonVirtSub1> query = mgr.createNamedQuery(NonVirtSub1.class, "findByName");
		query.setParameter("name", name);
		return query.getSingleObject();
	}
}
