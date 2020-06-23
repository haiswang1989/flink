package org.apache.flink.redis.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

@Internal
public class RedisValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE_REDIS = "redis";

	public static final String REDIS_IP = "redis.ip";

	public static final String REDIS_PORT = "redis.port";

	public static final String REDIS_VALUE_TYPE = "value.type";

	public static final String CACHE_SIZE = "cache.size";

	@Override
	public void validate(DescriptorProperties properties) {
		//TODO 验证支持的配置信息
		super.validate(properties);
	}
}
