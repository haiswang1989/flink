package org.apache.flink.redis.api;

import org.apache.commons.collections.map.LRUMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.redis.api.util.RedisConnectionUtils;
import org.apache.flink.redis.api.util.RedisValueType;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;

public class RedisLookupFunction extends TableFunction<Row> {

	//缓存的最大大小
	private static final int DEFAULT_MAX_SIZE = 10000000;

	/**
	 * Redis的IP
	 */
	private String ip;

	/**
	 * Redis的Port
	 */
	private int port;

	/**
	 * Redis的KEY的存储值的类型
	 */
	private String redisValueType;

	/**
	 * KEY的存储的value的类型(内部类型)
	 */
	private RedisValueType type;

	/**
	 * 缓存的大小
	 */
	private int cacheMaxSize;

	/**
	 * Row的字段名
	 */
	private String[] fieldNames;

	/**
	 * Row的字段类型
	 */
	private DataType[] fieldTypes;

	private transient JedisPoolConfig config;

	private transient JedisPool jedisPool;

	private transient LRUMap cache;

	public RedisLookupFunction(String ip, int port, String[] fieldNames, DataType[] fieldTypes, String[] redisJoinKeys, String redisValueType, int cacheMaxSize) {
		this.ip = ip;
		this.port = port;
		this.redisValueType = redisValueType;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		this.cacheMaxSize = cacheMaxSize;
		this.type = RedisValueType.getRedisValueType(this.redisValueType);
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		config = RedisConnectionUtils.getJedisPoolConfig();
		jedisPool = RedisConnectionUtils.getJedisPool(config, ip, port);
		if(cacheMaxSize <= 0 || cacheMaxSize > DEFAULT_MAX_SIZE ) {
			cacheMaxSize = DEFAULT_MAX_SIZE;
		}
		cache = new LRUMap(this.cacheMaxSize);
	}

	@Override
	public TypeInformation<Row> getResultType() {
		DataType dataType = TableSchema.builder().fields(fieldNames, fieldTypes).build().toRowDataType();
		return (TypeInformation<Row>)TypeConversions.fromDataTypeToLegacyInfo(dataType);
	}

	@Override
	public void close() throws Exception {
		RedisConnectionUtils.closeJedisPool(jedisPool);
	}

	//计算
	public void eval(Object... params) {
		String redisKey = String.valueOf(params[0]);
		Object value = getObjectFromRedis(redisKey);
		collect(Row.of(redisKey, value));
	}

	/**
	 *
	 * @param redisKey
	 * @return
	 */
	public Object getObjectFromRedis(String redisKey) {
		Object result = cache.get(redisKey);
		if(null == result) {
			switch (type) {
				case REDIS_VALUE_TYPE_STRING:
					result = getStringValue(redisKey);
					break;
				case REDIS_VALUE_TYPE_HASH:
					result = getHashValue(redisKey);
					break;
				case REDIS_VALUE_TYPE_LIST:
					result = getListValue(redisKey);
					break;
				case REDIS_VALUE_TYPE_SET:
					result = getSetValue(redisKey);
					break;
				case REDIS_VALUE_TYPE_ZSET:
					result = getZSetValue(redisKey);
					break;
				default:
					//can not reach!!!
					throw new RuntimeException(String.format("unknow redis value type [%s]", type));
			}

			cache.put(redisKey, result);
		}

		return result;
	}

	private String getStringValue(String redisKey) {
		try(Jedis jedis = jedisPool.getResource()) {
			String value = jedis.get(redisKey);
			return null == value ? "" : value;
		}
	}

	private Map<String, String> getHashValue(String redisKey) {
		try(Jedis jedis = jedisPool.getResource()) {
			return jedis.hgetAll(redisKey);
		}
	}

	private String[] getListValue(String redisKey) {
		try(Jedis jedis = jedisPool.getResource()) {
			Long listLength = jedis.llen(redisKey);
			if(0 == listLength) {
				return new String[0];
			} else {
				return jedis.lrange(redisKey, 0L, listLength-1).toArray(new String[0]);
			}
		}
	}

	private String[] getSetValue(String redisKey) {
		try(Jedis jedis = jedisPool.getResource()) {
			return jedis.smembers(redisKey).toArray(new String[0]);
		}
	}

	private String[] getZSetValue(String redisKey) {
		try(Jedis jedis = jedisPool.getResource()) {
			Long length = jedis.zcard(redisKey);
			if(0 == length) {
				return new String[0];
			} else {
				return jedis.zrange(redisKey, 0, length-1).toArray(new String[0]);
			}

		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private String ip;

		private int port;

		private String redisValueType;

		private int cacheMaxSize = -1;

		private String[] fieldNames;

		private DataType[] fieldTypes;

		private String[] redisJoinKeys;

		private Builder() {}

		public Builder setIp(String ip) {
			this.ip = ip;
			return this;
		}

		public Builder setPort(int port) {
			this.port = port;
			return this;
		}

		public Builder setRedisValueType(String redisValueType) {
			this.redisValueType = redisValueType;
			return this;
		}

		public Builder setFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		public Builder setFieldTypes(DataType[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return this;
		}

		public Builder setRedisJoinKeys(String[] redisJoinKeys) {
			this.redisJoinKeys = redisJoinKeys;
			return this;
		}

		public Builder setCacheMaxSize(int cacheMaxSize) {
			this.cacheMaxSize = cacheMaxSize;
			return this;
		}

		public RedisLookupFunction build() {
			//TODO do check
			return new RedisLookupFunction(ip, port, fieldNames, fieldTypes, redisJoinKeys, redisValueType, cacheMaxSize);
		}
	}
}
