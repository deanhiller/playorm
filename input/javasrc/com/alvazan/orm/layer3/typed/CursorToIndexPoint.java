package com.alvazan.orm.layer3.typed;

import com.alvazan.orm.api.z5api.IndexPoint;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;

public class CursorToIndexPoint extends AbstractCursor<IndexPoint> {

	private AbstractCursor<IndexColumn> indCol;
	private DboColumnIdMeta idMeta;
	private DboColumnMeta valueMeta;
	
	public CursorToIndexPoint(DboColumnIdMeta idMeta,
			DboColumnMeta colMeta, AbstractCursor<IndexColumn> indCol) {
		this.idMeta = idMeta;
		this.valueMeta = colMeta;
		this.indCol = indCol;
	}

	@Override
	public void beforeFirst() {
		indCol.beforeFirst();
	}

	@Override
	public Holder<IndexPoint> nextImpl() {
		Holder<IndexColumn> next = indCol.nextImpl();
		if(next == null)
			return null;
		
		IndexColumn col = next.getValue();
		IndexPoint p = new IndexPoint(idMeta, col, valueMeta);
		return new Holder<IndexPoint>(p);
	}

}
