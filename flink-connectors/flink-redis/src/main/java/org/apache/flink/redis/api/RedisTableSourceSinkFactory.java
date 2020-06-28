package org.apache.flink.redis.api;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.redis.table.descriptors.RedisValidator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.Schema.*;

public class RedisTableSourceSinkFactory implements
	StreamTableSourceFactory<Row>,
	StreamTableSinkFactory<Tuple2<Boolean, Row>> {

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
		String redisValueType = descriptorProperties.getString(RedisValidator.REDIS_VALUE_TYPE);
		int redisPort = descriptorProperties.getInt(RedisValidator.REDIS_PORT);
		String redisIp = descriptorProperties.getString(RedisValidator.REDIS_IP);
		return RedisTableSink.builder()
					.setRedisIp(redisIp)
					.setRedisPort(redisPort)
					.setRedisValueType(redisValueType)
					.setTableSchema(tableSchema)
					.build();
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
		String redisIp = descriptorProperties.getString(RedisValidator.REDIS_IP);
		int redisPort = descriptorProperties.getInt(RedisValidator.REDIS_PORT);
		String redisValueType = descriptorProperties.getString(RedisValidator.REDIS_VALUE_TYPE);
		int maxCacheSize = descriptorProperties.getInt(RedisValidator.CACHE_SIZE);
		return new RedisTableSource(tableSchema.getFieldNames(), tableSchema.getFieldDataTypes(), redisIp, redisPort, redisValueType, maxCacheSize);
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, RedisValidator.CONNECTOR_TYPE_VALUE_REDIS);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		properties.add(RedisValidator.REDIS_IP);
		properties.add(RedisValidator.REDIS_PORT);
		properties.add(RedisValidator.REDIS_VALUE_TYPE);
		properties.add(RedisValidator.CACHE_SIZE);

		properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		return properties;
	}

	/**
	 *
	 * @param properties
	 * @return
	 */
	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		new RedisValidator().validate(descriptorProperties);
		return descriptorProperties;
	}
}
