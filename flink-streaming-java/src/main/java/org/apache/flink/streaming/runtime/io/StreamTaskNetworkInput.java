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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of {@link StreamTaskInput} that wraps an input from network taken from {@link CheckpointedInputGate}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link StreamStatus} events, and forwards them to event subscribers once the
 * {@link StatusWatermarkValve} determines the {@link Watermark} from all inputs has advanced, or
 * that a {@link StreamStatus} needs to be propagated downstream to denote a status change.
 *
 * <p>Forwarding elements, watermarks, or status status elements must be protected by synchronizing
 * on the given lock object. This ensures that we don't call methods on a
 * {@link StreamInputProcessor} concurrently with the timer callback or other things.
 */
@Internal
public final class StreamTaskNetworkInput<T> implements StreamTaskInput<T> {

	//包含inputgate
	private final CheckpointedInputGate checkpointedInputGate;

	//反序列化代理对象，主要是将inputgate中二点inputchannel中的receivedBuffer中
	//的buffer中的segment中的字节数据进行反序列化
	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	//
	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	/** Valve that controls how watermarks and stream statuses are forwarded. */
	private final StatusWatermarkValve statusWatermarkValve;

	private final int inputIndex;

	private int lastChannel = UNSPECIFIED;

	//包装了DeserializationDelegate
	//执行getNextRecord的时候，对数据进行了反序列化
	//然后返回反序列化的结果，可以给后面的程序反馈信息

	//点进去看他的实现还是有点东西的
	//主要有DataInputView接口属性
	//还有一个Buffer属性
	//说明要通过RecordDeserializer从inputgate中拿数据，主要还是通过DataInputView来拿
	//我发现RecordDeserializer并没有和inputgate打交道
	//翻看了一下下面的代码，还是打交道了只不过是this对象直接从inputgate拿数据
	//然后给RecordDeserializer
	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer = null;

	@SuppressWarnings("unchecked")
	public StreamTaskNetworkInput(
			CheckpointedInputGate checkpointedInputGate,
			TypeSerializer<?> inputSerializer,
			IOManager ioManager,
			StatusWatermarkValve statusWatermarkValve,
			int inputIndex) {
		this.checkpointedInputGate = checkpointedInputGate;
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(
			new StreamElementSerializer<>(inputSerializer));

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[checkpointedInputGate.getNumberOfInputChannels()];
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		this.statusWatermarkValve = checkNotNull(statusWatermarkValve);
		this.inputIndex = inputIndex;
	}

	@VisibleForTesting
	StreamTaskNetworkInput(
		CheckpointedInputGate checkpointedInputGate,
		TypeSerializer<?> inputSerializer,
		StatusWatermarkValve statusWatermarkValve,
		int inputIndex,
		RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers) {

		this.checkpointedInputGate = checkpointedInputGate;
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(
			new StreamElementSerializer<>(inputSerializer));
		this.recordDeserializers = recordDeserializers;
		this.statusWatermarkValve = statusWatermarkValve;
		this.inputIndex = inputIndex;
	}

	@Override
	public InputStatus emitNext(DataOutput<T> output) throws Exception {

		while (true) {
			// get the stream element from the deserializer
			//根据翻译咱们知道，这是从反序列化器获取数据
			//刚才咱们分析了这个对象是有反序列化器的，而且还有inputgate
			//所以是可以拿到数据的
			if (currentRecordDeserializer != null) {
				//这里执行getNextRecord会让反序列化代理的反序列化组件反序列化字节数组
				//后放进代理中的instance对象，这个instance对象就是咱们要处理的数据

				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
				//如果buffer消费完了，那就回收了，简单来说
				//回收buffer就是把buffer中的segment的字节数组清零？
				if (result.isBufferConsumed()) {
					//这里的理解也很简单，一次从inputgate中消费一个buffer
					//消费完之后不能再次消费呀，原因是所有inputchannel都是公平排队的
					//你总不能消费完了一个inputchannel传过来的所有数据你才肯放手
					//这是不公平的，所以需要把currentRecordDeserializer置为null
					//当然也要把currentBuffer清空
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				//如果一整条记录（数据）都到位了那就开始处理呗
				//这里很有意思
				//为什么分bufferConsumed和fullrecord
				//如果深入分析过flink就知道
				//这里buffer维护的segment，是taskmanager的配置参数传入生成的
				//这里的参数是默认的，用户也可以自定义给出
				//默认参数一般为一个segment32KB，一个Taskmanager所对应的NetworkBufferpool
				//所拥有的内存大小为64MB，所以算下来，一般整个taskmanager一般有2048个segment，
				//所以咱们就清楚了，一条record可以是字符串，也有可能是对象，最终序列化成字节数组
				//可能在同一个segment中，也有可能不在同一segment中（我的理解是：这就是跨段吧）
				if (result.isFullRecord()) {
					processElement(deserializationDelegate.getInstance(), output);
					return InputStatus.MORE_AVAILABLE;
				}
			}

			//刚开始肯定得从inputgate拿buffer
			//这里拿buffer就可以与之前分析的代码对上了
			//在这之前inputgate会setup操作，给inputchannel（queuebuffer）分配buffer空间
			//然后让inputchannel跟上游partition通过netty沟通，传数据过来，一旦传过来数据
			//inputchannel就通知inputgate有数据了，然后inputgate就把inputchannel加到
			//inputgata得inputwithdata队列中，等待这个货消费他
			Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
			if (bufferOrEvent.isPresent()) {
				//拿到数据后，把数据保存到currentRecordDeserializer中
				//其实要讲细节的话
				processBufferOrEvent(bufferOrEvent.get());
			} else {
				//可以看到只有拿不到数据了，才说输入状态不可用，或者到末尾了
				if (checkpointedInputGate.isFinished()) {
					checkState(checkpointedInputGate.getAvailableFuture().isDone(), "Finished BarrierHandler should be available");
					if (!checkpointedInputGate.isEmpty()) {
						throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
					}
					return InputStatus.END_OF_INPUT;
				}
				return InputStatus.NOTHING_AVAILABLE;
			}
		}
	}

	private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
		if (recordOrMark.isRecord()){
			output.emitRecord(recordOrMark.asRecord());
		} else if (recordOrMark.isWatermark()) {
			statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);
		} else if (recordOrMark.isLatencyMarker()) {
			output.emitLatencyMarker(recordOrMark.asLatencyMarker());
		} else if (recordOrMark.isStreamStatus()) {
			statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel);
		} else {
			throw new UnsupportedOperationException("Unknown type of StreamElement");
		}
	}

	private void processBufferOrEvent(BufferOrEvent bufferOrEvent) throws IOException {
		if (bufferOrEvent.isBuffer()) {
			//当然第一步得确定是哪个channel
			lastChannel = bufferOrEvent.getChannelIndex();
			checkState(lastChannel != StreamTaskInput.UNSPECIFIED);

			//这里可以看到是每一个inputchannel对应一个反序列化RecordDeserializer对象
			//不过这个对象的实现只有这一个SpillingAdaptiveSpanningRecordDeserializer
			//这个类翻译过来没太搞懂（溢出自适应生成记录反序列化器？？？？？）
			currentRecordDeserializer = recordDeserializers[lastChannel];
			checkState(currentRecordDeserializer != null,
				"currentRecordDeserializer has already been released");

			//把buffer保存在RecordDeserializer中的currentBuffer属性中
			//这样一来，就有了buffer（里面有数据）了呀，
			//这里要讲细节的话，那就是先把buffer赋值给currentBuffer，
			//然后将buffer中的segment拿出来放到SpillingAdaptiveSpanningRecordDeserializer
			//中的DataInputView(这个对象就是取数据的入口呀，其实现是NonSpanningWrapper（翻译过来是非跨区包装
			// 这么说肯定有跨区包装，那就是SpanningWrapper，也是SpillingAdaptiveSpanningRecordDeserializer的属性
			// 看起来挺复杂的，暂不分析）)，这样一来DeserializationDelegate反序列就有数据了
			currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
		}
		else {
			// Event received
			final AbstractEvent event = bufferOrEvent.getEvent();
			// TODO: with checkpointedInputGate.isFinished() we might not need to support any events on this level.
			if (event.getClass() != EndOfPartitionEvent.class) {
				throw new IOException("Unexpected event: " + event);
			}

			// release the record deserializer immediately,
			// which is very valuable in case of bounded stream
			releaseDeserializer(bufferOrEvent.getChannelIndex());
		}
	}

	@Override
	public int getInputIndex() {
		return inputIndex;
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (currentRecordDeserializer != null) {
			return AVAILABLE;
		}
		return checkpointedInputGate.getAvailableFuture();
	}

	@Override
	public void close() throws IOException {
		// release the deserializers . this part should not ever fail
		for (int channelIndex = 0; channelIndex < recordDeserializers.length; channelIndex++) {
			releaseDeserializer(channelIndex);
		}

		// cleanup the resources of the checkpointed input gate
		checkpointedInputGate.cleanup();
	}

	private void releaseDeserializer(int channelIndex) {
		RecordDeserializer<?> deserializer = recordDeserializers[channelIndex];
		if (deserializer != null) {
			// recycle buffers and clear the deserializer.
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();

			recordDeserializers[channelIndex] = null;
		}
	}
}
