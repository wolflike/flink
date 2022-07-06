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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;

/**
 * A <b>State Backend</b> defines how the state of a streaming application is stored and
 * checkpointed. Different State Backends store their state in different fashions, and use
 * different data structures to hold the state of a running application.
 *
 * <p>For example, the {@link org.apache.flink.runtime.state.memory.MemoryStateBackend memory state backend}
 * keeps working state in the memory of the TaskManager and stores checkpoints in the memory of the
 * JobManager. The backend is lightweight and without additional dependencies, but not highly available
 * and supports only small state.
 *
 * <p>The {@link org.apache.flink.runtime.state.filesystem.FsStateBackend file system state backend}
 * keeps working state in the memory of the TaskManager and stores state checkpoints in a filesystem
 * (typically a replicated highly-available filesystem, like <a href="https://hadoop.apache.org/">HDFS</a>,
 * <a href="https://ceph.com/">Ceph</a>, <a href="https://aws.amazon.com/documentation/s3/">S3</a>,
 * <a href="https://cloud.google.com/storage/">GCS</a>, etc).
 * 
 * <p>The {@code RocksDBStateBackend} stores working state in <a href="http://rocksdb.org/">RocksDB</a>,
 * and checkpoints the state by default to a filesystem (similar to the {@code FsStateBackend}).
 * 
 * <h2>Raw Bytes Storage and Backends</h2>
 * 
 * The {@code StateBackend} creates services for <i>raw bytes storage</i> and for <i>keyed state</i>
 * and <i>operator state</i>.
 * 
 * <p>The <i>raw bytes storage</i> (through the {@link CheckpointStreamFactory}) is the fundamental
 * service that simply stores bytes in a fault tolerant fashion. This service is used by the JobManager
 * to store checkpoint and recovery metadata and is typically also used by the keyed- and operator state
 * backends to store checkpointed state.
 *
 * <p>The {@link AbstractKeyedStateBackend} and {@link OperatorStateBackend} created by this state
 * backend define how to hold the working state for keys and operators. They also define how to checkpoint
 * that state, frequently using the raw bytes storage (via the {@code CheckpointStreamFactory}).
 * However, it is also possible that for example a keyed state backend simply implements the bridge to
 * a key/value store, and that it does not need to store anything in the raw byte storage upon a
 * checkpoint.
 * 
 * <h2>Serializability</h2>
 * 
 * State Backends need to be {@link java.io.Serializable serializable}, because they distributed
 * across parallel processes (for distributed execution) together with the streaming application code. 
 * 
 * <p>Because of that, {@code StateBackend} implementations (typically subclasses
 * of {@link AbstractStateBackend}) are meant to be like <i>factories</i> that create the proper
 * states stores that provide access to the persistent storage and hold the keyed- and operator
 * state data structures. That way, the State Backend can be very lightweight (contain only
 * configurations) which makes it easier to be serializable.
 *
 * <h2>Thread Safety</h2>
 * 
 * State backend implementations have to be thread-safe. Multiple threads may be creating
 * streams and keyed-/operator state backends concurrently.
 */
//<b>状态后端</ b>定义了流式应用程序状态的存储和检查点。不同的状态后端以不同的方式存储其状态，
//并使用不同的数据结构来保存正在运行的应用程序的状态。
//<p>例如，{@ link org.apache.flink.runtime.state.memory.MemoryStateBackend内存状态后端}
//在TaskManager的内存中保持工作状态，并在JobManager的内存中存储检查点。
//后端是轻量级的，没有其他依赖关系，但是可用性不高，并且仅支持小状态。
//<p> {@ link org.apache.flink.runtime.state.filesystem.FsStateBackend文件系统状态后端}
//将工作状态保存在TaskManager的内存中，并将状态检查点存储在文件系统
//通常是复制的高可用性文件系统，例如<a href="https://hadoop.apache.org/"> HDFS </a>，
//<a href="https://ceph.com/"> Ceph </a>，
//<a href =“ https://aws.amazon.com/documentation/s3/">S3 </a>，
//<a href="https://cloud.google.com/storage/"> GCS </a>等）。
//<p> {@ code RocksDBStateBackend}将工作状态存储在<a href="http://rocksdb.org/"> RocksDB </a>中，
//并默认将状态检查点指向文件系统（类似于{@code FsStateBackend}）。
//<h2>原始字节存储和后端</ h2> {@code StateBackend}为<i>原始字节存储</ i>
//以及<i>键控状态</ i>和<i>运算符状态创建服务</ </我>。 <p> <i>原始字节存储</ i>
//（通过{@link CheckpointStreamFactory}）是一种基本服务，它仅以容错方式存储字节。
//JobManager使用此服务来存储检查点和恢复元数据，并且键控和操作员状态后端也通常使用此服务来存储检查点状态。
// <p>此状态创建的{@link AbstractKeyedStateBackend}和{@link OperatorStateBackend}
//  后端定义如何保持键和操作员的工作状态。他们还定义了如何检查点
// 该状态，通常使用原始字节存储空间（通过{@code CheckpointStreamFactory}）。
// 但是，也有可能例如键控状态后端只是实现了到
// 键/值存储，并且不需要在检查点将任何内容存储在原始字节存储中。
// <h2>可序列化</ h2>
// 状态后端需要{@link java.io.Serializable serializable}，因为它们是分布式的
// 跨并行进程（用于分布式执行）以及流应用程序代码。
// <p>因此，{@ code StateBackend}实现（通常是子类）
// {@link AbstractStateBackend}的意思就像创建适当的工厂的<i>工厂</ i>
// 状态存储，这些存储提供对持久性存储的访问，并保留键控和运算符
// 状态数据结构。这样，状态后端可以非常轻量级（仅包含
// 配置），从而更易于序列化。
@PublicEvolving
public interface StateBackend extends java.io.Serializable {

	// ------------------------------------------------------------------------
	//  Checkpoint storage - the durable persistence of checkpoint data
	// ------------------------------------------------------------------------

	/**
	 * Resolves the given pointer to a checkpoint/savepoint into a checkpoint location. The location
	 * supports reading the checkpoint metadata, or disposing the checkpoint storage location.
	 *
	 * <p>If the state backend cannot understand the format of the pointer (for example because it
	 * was created by a different state backend) this method should throw an {@code IOException}.
	 *
	 * @param externalPointer The external checkpoint pointer to resolve.
	 * @return The checkpoint location handle.
	 *
	 * @throws IOException Thrown, if the state backend does not understand the pointer, or if
	 *                     the pointer could not be resolved due to an I/O error.
	 */
	CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException;

	/**
	 * Creates a storage for checkpoints for the given job. The checkpoint storage is
	 * used to write checkpoint data and metadata.
	 *
	 * @param jobId The job to store checkpoint data for.
	 * @return A checkpoint storage for the given job.
	 *
	 * @throws IOException Thrown if the checkpoint storage cannot be initialized.
	 */
	CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException;

	// ------------------------------------------------------------------------
	//  Structure Backends 
	// ------------------------------------------------------------------------
	/**
	 * Creates a new {@link AbstractKeyedStateBackend} that is responsible for holding <b>keyed state</b>
	 * and checkpointing it.
	 *
	 * <p><i>Keyed State</i> is state where each value is bound to a key.
	 *
	 * @param env                  The environment of the task.
	 * @param jobID                The ID of the job that the task belongs to.
	 * @param operatorIdentifier   The identifier text of the operator.
	 * @param keySerializer        The key-serializer for the operator.
	 * @param numberOfKeyGroups    The number of key-groups aka max parallelism.
	 * @param keyGroupRange        Range of key-groups for which the to-be-created backend is responsible.
	 * @param kvStateRegistry      KvStateRegistry helper for this task.
	 * @param ttlTimeProvider      Provider for TTL logic to judge about state expiration.
	 * @param metricGroup          The parent metric group for all state backend metrics.
	 * @param stateHandles         The state handles for restore.
	 * @param cancelStreamRegistry The registry to which created closeable objects will be registered during restore.
	 * @param <K>                  The type of the keys by which the state is organized.
	 *
	 * @return The Keyed State Backend for the given job, operator, and key group range.
	 *
	 * @throws Exception This method may forward all exceptions that occur while instantiating the backend.
	 */
	<K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
		Environment env,
		JobID jobID,
		String operatorIdentifier,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		TaskKvStateRegistry kvStateRegistry,
		TtlTimeProvider ttlTimeProvider,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry) throws Exception;
	
	/**
	 * Creates a new {@link OperatorStateBackend} that can be used for storing operator state.
	 *
	 * <p>Operator state is state that is associated with parallel operator (or function) instances,
	 * rather than with keys.
	 *
	 * @param env The runtime environment of the executing task.
	 * @param operatorIdentifier The identifier of the operator whose state should be stored.
	 * @param stateHandles The state handles for restore.
	 * @param cancelStreamRegistry The registry to register streams to close if task canceled.
	 *
	 * @return The OperatorStateBackend for operator identified by the job and operator identifier.
	 *
	 * @throws Exception This method may forward all exceptions that occur while instantiating the backend.
	 */
	OperatorStateBackend createOperatorStateBackend(
		Environment env,
		String operatorIdentifier,
		@Nonnull Collection<OperatorStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry) throws Exception;
}
