package com.dixn.zookeeper.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dixn.zookeeper.utils.MongoServiceUtils;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Description：MongoDB new feature 0.1.0 persistence layer for MongoDB 
 * to increase, delete, change, check operation
 *
 * @projectName：TCMONGO
 * @ClassName：MongoService
 * @author：tan10ler
 * @createTime：Feb 20, 2019 3:57:52 PM
 * @update：tan10ler
 * @updateDate：Feb 20, 2019 3:57:52 PM
 * @version v1.0
 */
public class MongoService {

	@Autowired
	private MongoTemplate mongoTemplate;

	/**
	 * @Description:Insert one record
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 3:58:52 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 3:58:52 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> boolean insert(T object) throws Exception {

		long beforSave = mongoTemplate.count(new Query(), object.getClass());
		mongoTemplate.insert(object);
		long afterSave = mongoTemplate.count(new Query(), object.getClass());
		if (afterSave > beforSave) {
			return true;
		}
		return false;
	}

	/**
	 * @Description:Save a piece of data to the collection in json
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:01:10 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:01:10 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public boolean insert(String json, String collectionName) throws Exception {

		long beforSave = mongoTemplate.count(new Query(), collectionName);
		mongoTemplate.insert(json, collectionName);
		long afterSave = mongoTemplate.count(new Query(), collectionName);
		if (afterSave > beforSave) {
			return true;
		}
		return false;
	}

	/**
	 * @Description:Save a piece of data to the collection in json
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:02:05 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:02:05 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> boolean insert(Map<String, T> map, String collectionName) throws Exception {

		String json = null;
		if (map != null && map.size() > 0) {
			json = JSONObject.parseObject(JSON.toJSONString(map)).toString();
		}
		return insert(json, collectionName);
	}

	/**
	 * @Description:Save multiple pieces of data to the collection as a List map
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:02:38 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:02:38 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> boolean batchInsert(List<T> list) throws Exception {

		Class<? extends Object> entityClass = null;
		if (list != null && list.size() > 0) {
			entityClass = list.get(0).getClass();
		}
		long beforSave = mongoTemplate.count(new Query(), entityClass);
		mongoTemplate.insertAll(list);
		long afterSave = mongoTemplate.count(new Query(), entityClass);
		if (afterSave > beforSave) {
			return true;
		}
		return false;
	}

	/**
	 * @Description:Save multiple pieces of data to the collection in sjon format
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:06:58 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:06:58 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public boolean batchInsert(String json, String collectionName) throws Exception {
		List<Document> documents = new ArrayList<Document>();
		JSONArray array = JSONArray.parseArray(json);
		for (int i = 0; i < array.size(); i++) {
			documents.add(Document.parse(array.get(i).toString()));
		}
		long beforSave = mongoTemplate.count(new Query(), collectionName);
		mongoTemplate.insert(documents, collectionName);
		long afterSave = mongoTemplate.count(new Query(), collectionName);
		if (afterSave > beforSave) {
			return true;
		}
		return false;
	}

	/**
	 * @Description:Update single data content based on ID
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:07:25 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:07:25 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public boolean updateById(String id, Update update, String collectionName)
						throws Exception {

		UpdateResult result = mongoTemplate.updateFirst(Query.query(Criteria.where("_id").is(id)),
							update, collectionName);

		if (result != null && result.getModifiedCount() >= 0) {
			return true;
		}
		return false;
	}

	/**
	 * @Description:Update single data content based on ID
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:18:42 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:18:42 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> boolean updateById(String id, Update update, Class<T> entityClass)
						throws Exception {

		UpdateResult result = mongoTemplate.updateFirst(Query.query(Criteria.where("_id").is(id)),
							update, entityClass);

		if (result != null && result.getModifiedCount() >= 0) {
			return true;
		}
		return false;
	}

	/**
	 * @Description:Modify data in batch or single according to query conditions
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:18:56 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:18:56 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> boolean update(Query query, Update update, String collectionName)
						throws Exception {

		UpdateResult result = mongoTemplate.updateMulti(query, update, collectionName);

		if (result != null && result.getModifiedCount() >= 0) {
			return true;
		}
		return false;
	}

	/**
	 * @Description:Modify data in batch or single according to query conditions
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:19:42 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:19:42 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> boolean update(Query query, Update update, Class<T> entityClass)
						throws Exception {

		UpdateResult result = mongoTemplate.updateMulti(query, update, entityClass);

		if (result != null && result.getModifiedCount() >= 0) {
			return true;
		}
		return false;
	}

	/**
	 * @Description:Find unique data based on unique _id
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:20:00 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:20:00 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public Document findById(String id, String collectionName) throws Exception {

		Document result = mongoTemplate.findById(id, Document.class, collectionName);
		if (result != null) {
			return result;
		} else {
			return null;
		}
	}

	/**
	 * @Description:Find unique data based on unique _id
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:20:07 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:20:07 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> T findById(String id, Class<T> entityClass) throws Exception {

		T result = mongoTemplate.findById(id, entityClass);

		if (result != null) {
			return result;
		} else {
			return null;
		}
	}

	/**
	 * @Description:Find unique data based on unique _id
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:20:23 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:20:23 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public Document findOneData(Query query, String collectionName) throws Exception {

		Document result = mongoTemplate.findOne(query, Document.class, collectionName);

		if (result != null) {
			return result;
		} else {
			return null;
		}
	}

	/**
	 * @Description:Find unique data based on unique _id
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:20:31 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:20:31 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> T findOneData(Query query, Class<T> entityClass) throws Exception {

		T result = mongoTemplate.findOne(query, entityClass);

		if (result != null) {
			return result;
		} else {
			return null;
		}
	}

	/**
	 * @Description:Query all data collections in the collection
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:20:48 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:20:48 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public long findCollectionCount(String collectionName) throws Exception {

		long count = mongoTemplate.count(new Query(), collectionName);

		return count;
	}

	/**
	 * @Description:Query all data collections in the collection
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:20:56 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:20:56 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> long findCollectionCount(Class<T> entityClass) throws Exception {

		long count = mongoTemplate.count(new Query(), entityClass);

		return count;
	}

	/**
	 * @Description:Query all data collections in the collection
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:21:06 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:21:06 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public long findCollectionCount(Query query, String collectionName) throws Exception {

		long count = mongoTemplate.count(query, collectionName);

		return count;
	}

	/**
	 * @Description:Query all data collections in the collection
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:23:13 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:23:13 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> long findCollectionCount(Query query, Class<T> entityClass) throws Exception {

		long count = mongoTemplate.count(query, entityClass);

		return count;
	}

	/**
	 * @Description:Query all data collections in the collection
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:23:24 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:23:24 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public List<Document> findAll(String collectionName) throws Exception {

		List<Document> result = mongoTemplate.findAll(Document.class, collectionName);

		if (result != null && result.size() > 0) {
			return result;
		} else {
			return null;
		}

	}

	/**
	 * @Description:Query all data of the current collection and map it to javaBean
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:23:36 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:23:36 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> List<T> findAll(Class<T> entityClass) throws Exception {

		List<T> result = mongoTemplate.findAll(entityClass);

		if (result != null && result.size() > 0) {
			return result;
		} else {
			return null;
		}
	}

	/**
	 * @Description:Query data in the current collection that meets the query criteria based on criteria
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:23:49 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:23:49 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public List<Document> findByQuery(Query query, String collectionName) throws Exception {

		List<Document> result = mongoTemplate.find(query, Document.class, collectionName);

		if (result != null && result.size() > 0) {
			return result;
		} else {
			return null;
		}

	}

	/**
	 * @Description:Query data in the current collection that meets the query criteria based on criteria
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:24:04 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:24:04 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> List<T> findByQuery(Query query, Class<T> entityClass) throws Exception {

		List<T> result = mongoTemplate.find(query, entityClass);

		if (result != null && result.size() > 0) {
			return result;
		} else {
			return null;
		}
	}

	/**
	 * @Description:根据条件查询当前集合中符合查询条件的数据
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:24:17 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:24:17 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public List<Document> findByPage(String collectionName, Integer pageNo, Integer pageSize) throws Exception {

		return findByPage(collectionName, null, pageNo, pageSize, "", "");
	}

	/**
	 * @Description:根据条件查询当前集合中符合查询条件的数据
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:24:28 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:24:28 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public List<Document> findByPage(String collectionName, Query query, Integer pageNo,
						Integer pageSize) throws Exception {

		return findByPage(collectionName, query, pageNo, pageSize, "", "");
	}

	/**
	 * @Description:根据条件查询当前集合中符合查询条件的数据
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:24:34 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:24:34 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public List<Document> findByPage(String collectionName, Integer pageNo, Integer pageSize,
						String direction, String... sortFiled) throws Exception {

		return findByPage(collectionName, null, pageNo, pageSize, direction, sortFiled);
	}

	/**
	 * @Description:根据条件查询当前集合中符合查询条件的数据
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:24:41 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:24:41 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public List<Document> findByPage(String collectionName, Query query, Integer pageNo,
						Integer pageSize, String direction, String... sortFiled) throws Exception {

		query = MongoServiceUtils.createPageQuery(query, pageNo, pageSize, direction, sortFiled);

		List<Document> result = mongoTemplate.find(query, Document.class, collectionName);

		if (result != null && result.size() > 0) {
			return result;
		} else {
			return null;
		}
	}

	/**
	 * @Description:根据条件查询当前集合中符合查询条件的数据
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:24:49 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:24:49 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> List<T> findByPage(Class<T> entityClass, Integer pageNo, Integer pageSize) throws Exception {

		return findByPage(entityClass, null, pageNo, pageSize, "", "");
	}

	/**
	 * @Description:根据条件查询当前集合中符合查询条件的数据
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:24:55 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:24:55 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> List<T> findByPage(Class<T> entityClass, Query query, Integer pageNo,
						Integer pageSize) throws Exception {

		return findByPage(entityClass, query, pageNo, pageSize, "", "");
	}

	/**
	 * @Description:根据条件查询当前集合中符合查询条件的数据
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:25:03 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:25:03 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> List<T> findByPage(Class<T> entityClass, Integer pageNo, Integer pageSize,
						String direction, String... sortFiled) throws Exception {

		return findByPage(entityClass, null, pageNo, pageSize, direction, sortFiled);
	}

	/**
	 * @Description:根据条件查询当前集合中符合查询条件的数据
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:25:11 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:25:11 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> List<T> findByPage(Class<T> entityClass, Query query, Integer pageNo,
						Integer pageSize, String direction, String... sortFiled) throws Exception {

		query = MongoServiceUtils.createPageQuery(query, pageNo, pageSize, direction, sortFiled);

		List<T> result = mongoTemplate.find(query, entityClass);

		if (result != null && result.size() > 0) {
			return result;
		} else {
			return null;
		}
	}

	/**
	 * @Description:Delete this data based on the data id
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:25:22 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:25:22 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> boolean deleteById(String id, Class<T> entityClass) throws Exception {
		long beforDel = mongoTemplate.count(new Query(), entityClass);
		mongoTemplate.remove(Query.query(Criteria.where("_id").is(id)), entityClass);
		long afterDel = mongoTemplate.count(new Query(), entityClass);
		if (beforDel > afterDel) {
			return true;
		}
		return false;
	}

	/**
	 * @Description:Delete this data based on the data id
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:25:28 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:25:28 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public <T> boolean delete(Query query, Class<T> entityClass) throws Exception {
		long beforDel = mongoTemplate.count(new Query(), entityClass);
		mongoTemplate.remove(query, entityClass);
		long afterDel = mongoTemplate.count(new Query(), entityClass);
		if (beforDel > afterDel) {
			return true;
		}
		return false;
	}

	/**
	 * @Description:Delete this data according to the data query conditions
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:25:45 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:25:45 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public boolean deleteById(String id, String collectionName) throws Exception {
		long beforDel = mongoTemplate.count(new Query(), collectionName);
		mongoTemplate.remove(Query.query(Criteria.where("_id").is(id)), collectionName);
		long afterDel = mongoTemplate.count(new Query(), collectionName);
		if (beforDel > afterDel) {
			return true;
		}
		return false;
	}

	/**
	 * @Description:Delete this data according to the data query conditions
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:25:52 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:25:52 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public boolean delete(Query query, String collectionName) throws Exception {
		long beforDel = mongoTemplate.count(new Query(), collectionName);
		mongoTemplate.remove(query, collectionName);
		long afterDel = mongoTemplate.count(new Query(), collectionName);
		if (beforDel > afterDel) {
			return true;
		}
		return false;
	}
}
