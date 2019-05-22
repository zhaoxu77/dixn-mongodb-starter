package com.dixn.zookeeper.utils;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Arrays;

/**
 * @Description：Helper class
 *
 * @projectName：TCMONGO
 * @ClassName：MongoRepositoryUtils
 * @author：tan10ler
 * @createTime：Feb 20, 2019 3:57:26 PM
 * @update：tan10ler
 * @updateDate：Feb 20, 2019 3:57:26 PM
 * @version v1.0
 */
public class MongoServiceUtils {

	/**Asc sort*/
	public static final String MONGO_SORT_ASC = "ASC";

	/**Desc sort*/
	public static final String MONGO_SORT_DESC = "DESC";

	/**
	 * @Description:Verify that the incoming paging parameters are valid and correct, 
	 * and if there are any incorrect conditions, modify them.
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:29:31 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:29:31 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	public static Query createPageQuery(Query query, Integer pageNo, Integer pageSize, String direction,
						String[] sortFiled) {

		// 检测传入查询条件query对象是否存在
		if (query == null) {
			// 不存在则创建
			query = new Query();
		}

		// 设置分页属性
		query = createQueryPaging(query, pageNo, pageSize);

		// 设置排序属性
		query = createQueryDirection(query, direction, sortFiled);

		return query;

	}

	/**
	 * @Description:Set paging properties
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:29:20 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:29:20 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	private static Query createQueryPaging(Query query, Integer pageNo, Integer pageSize) {

		if (pageNo != null && pageSize != null) {
			pageNo = pageNo < 1 ? 1 : pageNo;
			query.skip((pageNo - 1) * pageSize).limit(pageSize);
		} else {
			query.skip(0).limit(pageSize);
		}
		return query;
	}

	/**
	 * @Description:Set sort properties
	 *
	 * @author：tan10ler
	 * @createTime：Feb 20, 2019 4:29:05 PM
	 * @updateUser：tan10ler
	 * @UpdateTime：Feb 20, 2019 4:29:05 PM
	 * @throws Exception
	 *
	 * @version：1.0
	 *
	 */
	private static Query createQueryDirection(Query query, String direction, String[] sortFiled) {

		if (direction != null && !"".equals(direction) && sortFiled != null && sortFiled.length > 0
							&& !"".equals(sortFiled)) {
			if (direction.toUpperCase().equals(MongoServiceUtils.MONGO_SORT_DESC)) {
				query.with(new Sort(Direction.DESC, Arrays.asList(sortFiled)));
			} else {
				query.with(new Sort(Direction.ASC, Arrays.asList(sortFiled)));
			}
		}
		return query;

	}
}
