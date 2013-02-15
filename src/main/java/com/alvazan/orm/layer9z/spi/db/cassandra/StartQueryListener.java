package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.List;
import java.util.concurrent.Future;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;

public interface StartQueryListener {

	List<Future<OperationResult<ColumnList<byte[]>>>> start();

}
