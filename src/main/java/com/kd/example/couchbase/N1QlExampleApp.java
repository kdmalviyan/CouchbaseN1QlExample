package com.kd.example.couchbase;

import org.apache.log4j.Logger;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.kd.example.couchbase.connection.ConnectionManager;
import com.kd.example.couchbase.helpers.QueryBuilder;
import com.kd.example.couchbase.service.N1QLService;

public class N1QlExampleApp {
	static String[] nodes = { "localhost", "127.0.0.1" };
	static Logger logger = Logger.getLogger(N1QlExampleApp.class);
	// create new bucket if not exists
	static boolean createnew = true;
	static Bucket bucket = ConnectionManager.getCouchbaseClient("CRUD_BUCKET", createnew, nodes);
	static N1QLService n1qlService = new N1QLService(bucket);

	public static void main(String[] args) {
		String queryString = "SELECT * FROM CRUD_BUCKET";
		N1qlQuery n1qlQuery = QueryBuilder.createSelectQuery(queryString, null);
		N1qlQueryResult n1qlQueryResult = n1qlService.executeSelect(n1qlQuery);
		n1qlQueryResult.allRows().forEach(row -> {
			//logger.info(row.value());
		});

		JsonObject params = JsonObject.create().put("name", "Singh");

		n1qlQuery = QueryBuilder.createSelectQuery(queryString, params);
		n1qlQueryResult = n1qlService.executeSelect(n1qlQuery);
		n1qlQueryResult.allRows().forEach(row -> {
			logger.info(row.value());
		});
	}
}
