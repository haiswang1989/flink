package org.apache.flink.redis.api.util;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisConnectionUtils {

	public static JedisPoolConfig getJedisPoolConfig() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(5);
		config.setMaxWaitMillis(1000L);
		config.setTestOnBorrow(false);
		return config;
	}

	public static JedisPool getJedisPool(JedisPoolConfig config, String ip, int port) {
		return new JedisPool(config, ip ,port);
	}

	public static void closeJedisPool(JedisPool jedisPool) {
		if(null != jedisPool && !jedisPool.isClosed()) {
			jedisPool.close();
		}
	}
}
