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
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * {@link StreamTask} for executing a {@link StreamSource}.
 *
 * <p>One important aspect of this is that the checkpointing and the emission of elements must never
 * occur at the same time. The execution must be serial. This is achieved by having the contract
 * with the {@link SourceFunction} that it must only modify its state or emit elements in
 * a synchronized block that locks on the lock Object. Also, the modification of the state
 * and the emission of elements must happen in the same block of code that is protected by the
 * synchronized block.
 *
 * @param <OUT> Type of the output elements of this source.
 * @param <SRC> Type of the source function for the stream source operator
 * @param <OP> Type of the stream source operator
 */
@Internal
public class SourceStreamTask<OUT, SRC extends SourceFunction<OUT>, OP extends StreamSource<OUT, SRC>>
	extends StreamTask<OUT, OP> {

	private final LegacySourceFunctionThread sourceThread;

	private volatile boolean externallyInducedCheckpoints;

	/**
	 * Indicates whether this Task was purposefully finished (by finishTask()), in this case we
	 * want to ignore exceptions thrown after finishing, to ensure shutdown works smoothly.
	 */
	private volatile boolean isFinished = false;

	public SourceStreamTask(Environment env) {
		super(env);
		this.sourceThread = new LegacySourceFunctionThread();
	}

	@Override
	//源头的任务，他并没有初始化inputProcessor
	protected void init() {
		// we check if the source is actually inducing the checkpoints, rather
		// than the trigger
		//我们检查源是否实际上在诱发检查点，而不是触发
		SourceFunction<?> source = headOperator.getUserFunction();
		if (source instanceof ExternallyInducedSource) {
			externallyInducedCheckpoints = true;

			ExternallyInducedSource.CheckpointTrigger triggerHook = new ExternallyInducedSource.CheckpointTrigger() {

				@Override
				public void triggerCheckpoint(long checkpointId) throws FlinkException {
					// TODO - we need to see how to derive those. We should probably not encode this in the
					// TODO -   source's trigger message, but do a handshake in this task between the trigger
					// TODO -   message from the master, and the source's trigger notification
					final CheckpointOptions checkpointOptions = CheckpointOptions.forCheckpointWithDefaultLocation();
					final long timestamp = System.currentTimeMillis();

					final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, timestamp);

					try {
						SourceStreamTask.super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, false)
							.get();
					}
					catch (RuntimeException e) {
						throw e;
					}
					catch (Exception e) {
						throw new FlinkException(e.getMessage(), e);
					}
				}
			};

			((ExternallyInducedSource<?, ?>) source).setCheckpointTrigger(triggerHook);
		}
	}

	@Override
	protected void advanceToEndOfEventTime() throws Exception {
		headOperator.advanceToEndOfEventTime();
	}

	@Override
	protected void cleanup() {
		// does not hold any resources, so no cleanup needed
	}

	@Override
	//可以大概知道，SourceTask，并不是用inputprocessor在处理数据
	//而是把sourcefunction中run代码用sourceThread跑起来（就是去connector拿数据）
	//1.把SourceThread跑起来
	//2.调用operator的run方法
	//3.调用function的run方法（比如FlinkKafkaConsumerBase）
	//4.创建FlinkKafkaConsumerBase创建kafkaFetcher（kafkaFetcher有两个重要的东西，handover（存数据的）和consumeThread（从kafka拿数据的））
	//5.执行kafkaFetcher的runFetchLoop
	//6.runFetchLoop循环中从handover拿数据，consumeThread从kafka拿数据
	protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {

		controller.suspendDefaultAction();

		// Against the usual contract of this method, this implementation is not step-wise but blocking instead for
		// compatibility reasons with the current source interface (source functions run as a loop, not in steps).
		//与此方法的通常约定相反，此实现不是逐步实现的，
		//而是由于与当前源接口的兼容性原因而阻塞的（源函数作为循环运行，而不是按步骤运行）。
		sourceThread.setTaskDescription(getName());
		//这里的sourceThread是专门处理无限数据流的
		sourceThread.start();
		sourceThread.getCompletionFuture().whenComplete((Void ignore, Throwable sourceThreadThrowable) -> {
			if (isCanceled() && ExceptionUtils.findThrowable(sourceThreadThrowable, InterruptedException.class).isPresent()) {
				mailboxProcessor.reportThrowable(new CancelTaskException(sourceThreadThrowable));
			} else if (!isFinished && sourceThreadThrowable != null) {
				mailboxProcessor.reportThrowable(sourceThreadThrowable);
			} else {
				//mailbox用来处理checkpoint和timer
				mailboxProcessor.allActionsCompleted();
			}
		});
	}

	@Override
	protected void cancelTask() {
		try {
			if (headOperator != null) {
				headOperator.cancel();
			}
		}
		finally {
			sourceThread.interrupt();
		}
	}

	@Override
	protected void finishTask() throws Exception {
		isFinished = true;
		cancelTask();
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	//SourceStreamTask就和父类StreamTask不一样
	@Override
	public Future<Boolean> triggerCheckpointAsync(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {
		if (!externallyInducedCheckpoints) {
			//还是交给父类执行？
			return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);
		}
		else {
			// we do not trigger checkpoints here, we simply state whether we can trigger them
			synchronized (getCheckpointLock()) {
				return CompletableFuture.completedFuture(isRunning());
			}
		}
	}

	@Override
	protected void declineCheckpoint(long checkpointId) {
		if (!externallyInducedCheckpoints) {
			super.declineCheckpoint(checkpointId);
		}
	}

	@Override
	protected void handleCheckpointException(Exception exception) {
		// For externally induced checkpoints, the exception would be passed via triggerCheckpointAsync future.
		if (!externallyInducedCheckpoints) {
			super.handleCheckpointException(exception);
		}
	}

	/**
	 * Runnable that executes the the source function in the head operator.
	 */
	private class LegacySourceFunctionThread extends Thread {

		private final CompletableFuture<Void> completionFuture;

		LegacySourceFunctionThread() {
			this.completionFuture = new CompletableFuture<>();
		}

		@Override
		public void run() {
			try {
				headOperator.run(getCheckpointLock(), getStreamStatusMaintainer(), operatorChain);
				completionFuture.complete(null);
			} catch (Throwable t) {
				// Note, t can be also an InterruptedException
				completionFuture.completeExceptionally(t);
			}
		}

		public void setTaskDescription(final String taskDescription) {
			setName("Legacy Source Thread - " + taskDescription);
		}

		CompletableFuture<Void> getCompletionFuture() {
			return completionFuture;
		}
	}
}
