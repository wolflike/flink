/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * @param <IN> The type of the record that can be read with this record reader.
 */
@Internal
//可以看到这是专门处理只有单个输入的task的处理器
//可以看到他有四个组件
public final class StreamOneInputProcessor<IN> implements StreamInputProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(StreamOneInputProcessor.class);

	//专门处理输入的
	//点进去看他的属性就知道
	//它包含了反序列化组件，这是很合理的
	//因为数据从上游发送过来都是序列化的组件
	private final StreamTaskInput<IN> input;
	//显然，他需要包含操作算子，因为从输入得到的反序列化对象后
	//需要operator包装的用户逻辑去计算他
	//所以把operator算子归到output组件中
	//在output的operator算子中完成了计算之后
	//就需要序列化成字节数组放到partition中的bufferpool（partition暂时没有分析
	// 不能说的这么仔细）中
	private final DataOutput<IN> output;

	//可以看到这把锁是创建者传入的、
	//从代码中可以看到lock锁住的资源是operatorchain（暂时没搞懂为啥要锁住他）
	private final Object lock;

	//operatorchain也是创建者传入的，我记得没错的话，operatorchain是由streamTask传进来的
	private final OperatorChain<?, ?> operatorChain;

	public StreamOneInputProcessor(
			StreamTaskInput<IN> input,
			DataOutput<IN> output,
			Object lock,
			OperatorChain<?, ?> operatorChain) {

		this.input = checkNotNull(input);
		this.output = checkNotNull(output);
		this.lock = checkNotNull(lock);
		this.operatorChain = checkNotNull(operatorChain);
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return input.getAvailableFuture();
	}

	@Override
	public InputStatus processInput() throws Exception {
		//这步操作非常重要，咱们进去看看
		InputStatus status = input.emitNext(output);

		//如果输入上游数据消费完了
		if (status == InputStatus.END_OF_INPUT) {
			synchronized (lock) {
				//那就终结链上的头节点
				operatorChain.endHeadOperatorInput(1);
			}
		}

		return status;
	}

	@Override
	public void close() throws IOException {
		input.close();
	}
}
