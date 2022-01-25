package com.atguigu.day06;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Flink09_State_Backend {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 设置状态后端为内存级别的
        env.setStateBackend(new MemoryStateBackend());

        //TODO 设置状态后端为文件系统
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));

        //TODO 设置状态后端为RocksDB
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/ck/rocksDb"));

        //TODO barrier不对齐(会有可能导致数据重复)
        env.getCheckpointConfig().enableUnalignedCheckpoints();
    }
}
