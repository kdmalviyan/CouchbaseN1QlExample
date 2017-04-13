package com.kd.example.couchbase.helpers;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;

public class QueryBuilder {

	public static N1qlQuery createSelectQuery(String statement, JsonObject namedParams) {
		N1qlQuery n1qlQuery = null;
		if (namedParams == null) {
			n1qlQuery = N1qlQuery.simple(statement);
		} else {
			n1qlQuery = N1qlQuery.parameterized(statement, namedParams);
		}
		return n1qlQuery;
	}
}
