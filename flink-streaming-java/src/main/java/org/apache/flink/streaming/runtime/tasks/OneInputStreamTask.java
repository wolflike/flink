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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.AbstractDataOutput;
import org.apache.flink.streaming.runtime.io.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.InputGateUtil;
import org.apache.flink.streaming.runtime.io.InputProcessorUtil;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link StreamTask} for executing a {@link OneInputStreamOperator}.
 */
@Internal
public class OneInputStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

	private final WatermarkGauge inputWatermarkGauge = new WatermarkGauge();

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * @param env The task environment for this task.
	 */
	public OneInputStreamTask(Environment env) {
		super(env);
	}

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * <p>This constructor accepts a special {@link TimerService}. By default (and if
	 * null is passes for the time provider) a {@link SystemProcessingTimeService DefaultTimerService}
	 * will be used.
	 *
	 * @param env The task environment for this task.
	 * @param timeProvider Optionally, a specific time provider to use.
	 */
	@VisibleForTesting
	public OneInputStreamTask(
			Environment env,
			@Nullable TimerService timeProvider) {
		super(env, timeProvider);
	}

	@Override
	//这是在执行前要初始化的地方，每个task都不一样
	//可以看到OneInputStreamTask主要是初始化了inputProcessor
	public void init() throws Exception {
		StreamConfig configuration = getConfiguration();
		int numberOfInputs = configuration.getNumberOfInputs();

		if (numberOfInputs > 0) {
			CheckpointedInputGate inputGate = createCheckpointedInputGate();
			TaskIOMetricGroup taskIOMetricGroup = getEnvironment().getMetricGroup().getIOMetricGroup();
			taskIOMetricGroup.gauge("checkpointAlignmentTime", inputGate::getAlignmentDurationNanos);

			DataOutput<IN> output = createDataOutput();
			StreamTaskInput<IN> input = createTaskInput(inputGate, output);
			inputProcessor = new StreamOneInputProcessor<>(
				input,
				output,
				getCheckpointLock(),
				operatorChain);
		}
		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge);
		// wrap watermark gauge since registered metrics must be unique
		getEnvironment().getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge::getValue);
	}

	private CheckpointedInputGate createCheckpointedInputGate() throws IOException {
		InputGate[] inputGates = getEnvironment().getAllInputGates();
		InputGate inputGate = InputGateUtil.createInputGate(inputGates);

		return InputProcessorUtil.createCheckpointedInputGate(
			this,
			configuration.getCheckpointMode(),
			getEnvironment().getIOManager(),
			inputGate,
			getEnvironment().getTaskManagerInfo().getConfiguration(),
			getTaskNameWithSubtaskAndId());
	}

	private DataOutput<IN> createDataOutput() {
		return new StreamTaskNetworkOutput<>(
			headOperator,
			getStreamStatusMaintainer(),
			getCheckpointLock(),
			inputWatermarkGauge,
			setupNumRecordsInCounter(headOperator));
	}

	private StreamTaskInput<IN> createTaskInput(CheckpointedInputGate inputGate, DataOutput<IN> output) {
		int numberOfInputChannels = inputGate.getNumberOfInputChannels();
		StatusWatermarkValve statusWatermarkValve = new StatusWatermarkValve(numberOfInputChannels, output);

		TypeSerializer<IN> inSerializer = configuration.getTypeSerializerIn1(getUserCodeClassLoader());
		return new StreamTaskNetworkInput<>(
			inputGate,
			inSerializer,
			getEnvironment().getIOManager(),
			statusWatermarkValve,
			0);
	}

	/**
	 * The network data output implementation used for processing stream elements
	 * from {@link StreamTaskNetworkInput} in one input processor.
	 */
	//还是说说这个类吧
		//大概还记得这个类的operator是由streamTask传过来的headOperator
		//这样的话，就很好说得通了，在链上的节点首先肯定先是headOperator先处理数据
		//处理完之后他可以给下个节点，如果到末尾那就输出

		//其实StreamTaskNetworkOutput只对应headOperator
		//ChainingOutput对应链上普通节点


		//这里其实是有严格的对应关系的
		//StreamTaskNetworkOutput有operator
		//operator 有 output（实现OutPut.collect方法）
		//output 也有operator（每一个ChainingOut就是一个output）
		//这样数据就迭代式传下去了
		//直到链端未处
	private static class StreamTaskNetworkOutput<IN> extends AbstractDataOutput<IN> {

		private final OneInputStreamOperator<IN, ?> operator;

		//这个是对watermark的度量
		private final WatermarkGauge watermarkGauge;
		//这个应该是保存到来的record个数
		private final Counter numRecordsIn;

		private StreamTaskNetworkOutput(
				OneInputStreamOperator<IN, ?> operator,
				StreamStatusMaintainer streamStatusMaintainer,
				Object lock,
				WatermarkGauge watermarkGauge,
				Counter numRecordsIn) {
			super(streamStatusMaintainer, lock);

			this.operator = checkNotNull(operator);
			this.watermarkGauge = checkNotNull(watermarkGauge);
			this.numRecordsIn = checkNotNull(numRecordsIn);
		}

		@Override
		public void emitRecord(StreamRecord<IN> record) throws Exception {
			synchronized (lock) {
				//处理一条数据就+1
				numRecordsIn.inc();
				//点进去看看就知道
				//他就是为这条record设置一个key
				//设置key用到了KeySelector
				//这个类就是一个函数对象
				//该函数对象就是根据用户提供的数据对象，返回值为key
				//还记得keyBy api吗，这个就是把数据流中的每一条数据当成参数
				//咱们传给keyBy就是咱们想要以那个参数为key的
				operator.setKeyContextElement1(record);
				//这里就是处理数据了
				operator.processElement(record);
			}
		}

		@Override
		public void emitWatermark(Watermark watermark) throws Exception {
			synchronized (lock) {
				watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
				operator.processWatermark(watermark);
			}
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
			synchronized (lock) {
				operator.processLatencyMarker(latencyMarker);
			}
		}
	}
}
