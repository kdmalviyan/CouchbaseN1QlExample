package com.kd.example.couchbase.service;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;

public class N1QLService {
	Bucket bucket;

	public N1QLService(Bucket bucket) {
		this.bucket = bucket;
	}

	public N1qlQueryResult executeSelect(N1qlQuery n1qlSelectQuery) {
		return bucket.query(n1qlSelectQuery);
	}
}
