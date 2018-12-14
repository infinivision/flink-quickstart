/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.infinivision.flink.streaming.entity;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * The serialization schema for the {@link AdFeature} type. This class defines how to transform a
 * Kafka record's bytes to a {@link AdFeature}, and vice-versa.
 */
public class AdFeatureSchema implements DeserializationSchema<AdFeature>, SerializationSchema<AdFeature> {

	private static final long serialVersionUID = 6154188370181669758L;

	@Override
	public byte[] serialize(AdFeature event) {
		return event.toString().getBytes();
	}

	@Override
	public AdFeature deserialize(byte[] message) throws IOException {
		return AdFeature.fromString(new String(message));
	}

	@Override
	public boolean isEndOfStream(AdFeature nextElement) {
		return false;
	}

	@Override
	public TypeInformation<AdFeature> getProducedType() {
		return TypeInformation.of(AdFeature.class);
	}
}
