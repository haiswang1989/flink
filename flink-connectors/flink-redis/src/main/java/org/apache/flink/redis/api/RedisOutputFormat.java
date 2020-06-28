package org.apache.flink.redis.api;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.redis.api.util.RedisConnectionUtils;
import org.apache.flink.redis.api.util.RedisValueType;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.Map;

/**
 * @author hswang
 * @Date 2020-06-23 16:53
 */
public class RedisOutputFormat extends RichOutputFormat<Tuple2<Boolean, Row>> {

	private String redisIp;

	private int redisPort;

	private String redisValueType;

	private RedisValueType type;

	private transient JedisPoolConfig jedisPoolConfig;

	private transient JedisPool jedisPool;

	public RedisOutputFormat(String redisIp, int redisPort, String redisValueType) {
		this.redisIp = redisIp;
		this.redisPort = redisPort;
		this.redisValueType = redisValueType;
		this.type = RedisValueType.getRedisValueType(this.redisValueType);
	}

	@Override
	public void configure(Configuration parameters) {
	}



	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		jedisPoolConfig = RedisConnectionUtils.getJedisPoolConfig();
		jedisPool = RedisConnectionUtils.getJedisPool(jedisPoolConfig, redisIp, redisPort);
	}

	@Override
	public void writeRecord(Tuple2<Boolean, Row> record) throws IOException {
		writeRedis(record.f1);
	}

	private void writeRedis(Row record) {
		String redisKey = String.valueOf(record.getField(0));
		switch (type) {
			case REDIS_VALUE_TYPE_STRING:
				String stringValue = (String) record.getField(1);
				writeStringValue(redisKey, stringValue);
				break;
			case REDIS_VALUE_TYPE_HASH:
				Map<String, String> hashValue = (Map<String, String>) record.getField(1);
				writeHashValue(redisKey, hashValue);
				break;
			case REDIS_VALUE_TYPE_LIST:
				String[] listValue = (String[]) record.getField(1);
				writeListValue(redisKey, listValue);
				break;
			case REDIS_VALUE_TYPE_SET:
				String[] setValue = (String[]) record.getField(1);
				writeSetValue(redisKey, setValue);
				break;
			case REDIS_VALUE_TYPE_ZSET:
				throw new RuntimeException("value type [sorted set] unsupport now.");
			default:
				throw new RuntimeException(String.format("unknow redis value type [%s]", type));

		}
	}

	private void writeStringValue(String redisKey, String stringValue) {
		try(Jedis jedis = jedisPool.getResource()) {
			jedis.set(redisKey, stringValue);
		}
	}

	private void writeHashValue(String redisKey, Map<String, String> hashValue) {
		try(Jedis jedis = jedisPool.getResource()) {
			jedis.hmset(redisKey, hashValue);
		}
	}

	private void writeListValue(String redisKey, String[] listValue) {
		try(Jedis jedis = jedisPool.getResource()) {
			jedis.lpush(redisKey, listValue);
		}
	}

	private void writeSetValue(String redisKey, String[] setValue) {
		try(Jedis jedis = jedisPool.getResource()) {
			jedis.sadd(redisKey, setValue);
		}
	}

	private void writeZSetValue() {

	}

	@Override
	public void close() throws IOException {
		RedisConnectionUtils.closeJedisPool(jedisPool);
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * RedisOutputFormat 的Builder
	 */
	public static class Builder {

		private String redisIp;

		private int redisPort;

		private String redisValueType;

		public Builder setRedisIp(String redisIp) {
			this.redisIp = redisIp;
			return this;
		}

		public Builder setRedisPort(int redisPort) {
			this.redisPort = redisPort;
			return this;
		}

		public Builder setRedisValueType(String redisValueType) {
			this.redisValueType = redisValueType;
			return this;
		}

		public RedisOutputFormat build() {
			return new RedisOutputFormat(redisIp, redisPort, redisValueType);
		}
	}
}
