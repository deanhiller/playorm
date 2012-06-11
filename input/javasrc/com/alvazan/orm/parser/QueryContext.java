package com.alvazan.orm.parser;

import com.alvazan.orm.impl.meta.MetaClass;
import com.alvazan.orm.parser.tree.FromClause;
import com.alvazan.orm.parser.tree.SelectClause;
import com.alvazan.orm.parser.tree.WhereClause;

/**
 * 
 * @author Huai Jiang
 *
 */
public class QueryContext {

	
	private SelectClause selectClause;
	private FromClause fromClause;
	private WhereClause whereClause;
	private MetaClass metaClass;

	
	

	public SelectClause getSelectClause() {
		return selectClause;
	}

	public FromClause getFromClause() {
		return fromClause;
	}

	public WhereClause getWhereClause() {
		return whereClause;
	}

	public void setSelectClause(SelectClause select) {
		this.selectClause = select;
		
	}

	public void setFromClause(FromClause from) {
		this.fromClause = from;
		
	}

	public void setWhereClause(WhereClause where) {
		this.whereClause = where;
		
	}

	public void setup(MetaClass metaClass) {
		
		
		
		
	}
	
}
