package com.inspur;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * User: YANG
 * Date: 2019/7/3-0:18
 * Description: No Description
 *
 * 本操作是针对mongodb 第3版本的
 *
 */
public class MongoDemo {
	public static void main(String[] args) throws InterruptedException {
		//这里因为有权限的问题,所以这里必须要设置连接哪个数据库
		MongoClientURI uri = new MongoClientURI("mongodb://root:123456@192.168.120.150:27017/yang");
		MongoClient client = new MongoClient(uri);

		MongoDatabase db = client.getDatabase("yang");
		System.out.println(db.getCollection("mongo3_coll").count());

		MongoCollection collection = db.getCollection("mongo3_coll");

		//************************************插入数据************************************
//		List<Document> documents = new ArrayList<Document>();
//
//		for(int i = 0; i < 50; i++){
//			Document document = new Document();
//			document.append("studentID", i);
//			document.append("name", "姓名-" + i);
//			document.append("sex", i % 2);
//			documents.add(document);
//		}
//		collection.insertMany(documents);

		//************************************查询数据************************************
		//全部查询
//		MongoCursor cursor = collection.find().iterator();

		//范围查询
//		BasicDBObject queryINObject = new BasicDBObject("studentID", new BasicDBObject("$in", new int[] {2, 4, 6, 8}));
//		MongoCursor cursor = collection.find(queryINObject).iterator();


		//模糊查询
//		BasicDBObject queryMHObject = new BasicDBObject("name", new BasicDBObject("$regex", Pattern.compile("0")).append("$options", "i"));
//		MongoCursor cursor = collection.find(queryMHObject).iterator();

		//分页查询:
		//设置分页条件
//		BasicDBObject queryObject = new BasicDBObject("studentID", new BasicDBObject("$gte", 0).append("$lte", 10));
//		MongoCursor cursor = collection.find(queryObject).skip(0).limit(5).iterator();

//		while(cursor.hasNext()){
//			System.out.println(cursor.next());
//		}

		//************************************数据修改************************************
		//修改条件
//		BasicDBObject sourceObject = new BasicDBObject("studentID", 1);
//		BasicDBObject sourceObject = new BasicDBObject("studentID", new BasicDBObject("$gte", 1).append("$lte", 3));

		//修改一个字段的数据的操作
//		BasicDBObject destinObject = new BasicDBObject("$set", new BasicDBObject("name" , "superMan"));
		//修改多个字段的数据
//		BasicDBObject destinObject = new BasicDBObject("$set", new BasicDBObject("name" , "superMan000000").append("sex", -1));

		//修改单行
//		UpdateResult updateResult = collection.updateOne(sourceObject, destinObject);
//		System.out.println(updateResult.getModifiedCount());
		//修改多行的操作
//		UpdateResult updateResult = collection.updateMany(sourceObject, destinObject);
//		System.out.println(updateResult.getModifiedCount());


		//**********************数据的删除**********************
//		BasicDBObject sourceObject = new BasicDBObject("studentID", new BasicDBObject("$gte", 1).append("$lte", 3));
//		DeleteResult deleteResult = collection.deleteMany(sourceObject);
//		System.out.println(deleteResult.getDeletedCount());

		//聚合操作
		MongoDatabase yang = client.getDatabase("yang");
		MongoCollection empCollection = yang.getCollection("emp");

		//设置各种聚合条件
		BasicDBObject sourceObject = new BasicDBObject("$group", new BasicDBObject("_id", "$sex")
				.append("count", new BasicDBObject("$sum", 1))
				.append("sal_avg", new BasicDBObject("$avg", "$salary")));

		List<BasicDBObject> list = new ArrayList<BasicDBObject>();
		list.add(sourceObject);

		MongoCursor<Document> cursor = empCollection.aggregate(list).iterator();
		while(cursor.hasNext()){
			System.out.println(cursor.next().toString());
		}

		client.close();
	}
}
