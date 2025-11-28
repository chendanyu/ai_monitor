#!/usr/bin/env python3
"""
修复版本的Kafka消费者服务 - 解决分区分配和消息处理问题
"""

import json
import logging
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List
from pathlib import Path

import mysql.connector
from mysql.connector import Error, pooling
from kafka import KafkaConsumer
import schedule

# 创建日志目录
log_dir = Path("/tmp/ai-pipeline-logs")
log_dir.mkdir(parents=True, exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_dir / 'kafka_consumer_fixed.log')
    ]
)
logger = logging.getLogger(__name__)

# 配置 - 简化配置，使用单个消费者
KAFKA_CONFIG = {
    'bootstrap_servers': 'localhost:9093',
    'group_id': 'ai_pipeline_consumer_group_fixed',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'auto_commit_interval_ms': 5000,
    'max_poll_records': 50,
    'session_timeout_ms': 30000,
    'heartbeat_interval_ms': 10000
}

DB_CONFIG = {
    'host': 'localhost',
    'database': 'ai_pipeline',
    'user': 'aiuser',
    'password': 'aipassword',
    'charset': 'utf8mb4'
}

class SimpleDatabaseManager:
    def __init__(self):
        self.connection_pool = None
        self._init_connection_pool()
    
    def _init_connection_pool(self):
        """初始化数据库连接池"""
        try:
            self.connection_pool = pooling.MySQLConnectionPool(
                pool_name="simple_ai_pipeline_pool",
                pool_size=20,
                **DB_CONFIG
            )
            logger.info("✅ Database connection pool initialized successfully")
        except Error as e:
            logger.error(f"❌ Failed to initialize connection pool: {e}")
            raise
    
    def save_message(self, message: Dict[str, Any]):
        """保存单个消息到数据库"""
        connection = None
        try:
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor()
            
            # 插入AI模型数据
            insert_query = """
            INSERT INTO ai_model_data 
            (session_id, model_type, input_data, output_data, processing_time_ms, timestamp, status, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                message['session_id'],
                message['model_type'],
                json.dumps(message.get('input_data', {}), ensure_ascii=False),
                json.dumps(message.get('output_data', {}), ensure_ascii=False),
                message.get('processing_time_ms', 0),
                datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00')),
                message.get('status', 'success'),
                json.dumps(message.get('metadata', {}), ensure_ascii=False)
            ))
            
            # 更新会话信息
            self._update_session(cursor, message)
            
            connection.commit()
            logger.info(f"✅ Saved message for session: {message['session_id']}")
            return True
            
        except Error as e:
            logger.error(f"❌ Error saving message: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()
    
    def _update_session(self, cursor, message: Dict[str, Any]):
        """更新会话信息"""
        session_id = message['session_id']
        
        # 检查会话是否存在
        check_query = "SELECT session_id FROM sessions WHERE session_id = %s"
        cursor.execute(check_query, (session_id,))
        
        if cursor.fetchone():
            # 更新现有会话
            update_query = """
            UPDATE sessions 
            SET updated_at = %s, 
                total_processing_time_ms = total_processing_time_ms + %s
            WHERE session_id = %s
            """
            cursor.execute(update_query, (
                datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00')),
                message.get('processing_time_ms', 0),
                session_id
            ))
        else:
            # 插入新会话
            insert_query = """
            INSERT INTO sessions (session_id, created_at, updated_at, total_processing_time_ms)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                session_id,
                datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00')),
                datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00')),
                message.get('processing_time_ms', 0)
            ))

class SimpleKafkaConsumer:
    def __init__(self, db_manager: SimpleDatabaseManager):
        self.db_manager = db_manager
        self.consumer = None
        self.running = False
        self.thread_pool = ThreadPoolExecutor(max_workers=5)
    
    def start(self):
        """启动消费者"""
        try:
            self.consumer = KafkaConsumer(
                'ai_model_data',
                **KAFKA_CONFIG,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            self.running = True
            logger.info("✅ Kafka consumer started successfully")
            
            # 开始消费消息
            self._consume_messages()
            
        except Exception as e:
            logger.error(f"❌ Failed to start Kafka consumer: {e}")
    
    def _consume_messages(self):
        """消费消息的主循环"""
        logger.info("🔄 Starting to consume messages...")
        processed_count = 0
        
        while self.running:
            try:
                # 拉取消息
                message_batch = self.consumer.poll(timeout_ms=1000, max_records=10)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        logger.info(f"📨 Received {len(messages)} messages from partition {topic_partition}")
                        
                        for message in messages:
                            processed_count += 1
                            logger.info(f"🔍 Processing message {processed_count}: {message.value.get('session_id')}")
                            
                            # 在线程池中处理消息
                            # 返回一个 Future 对象。这个 Future 对象代表一个异步计算，它会在未来某个时间点完成，并持有任务执行的结果或异常。
                            future = self.thread_pool.submit(self._process_single_message, message.value)
                            future.add_done_callback(self._handle_processing_result)
                
                # 定期提交偏移量
                self.consumer.commit_async()
                
            except Exception as e:
                logger.error(f"❌ Error in consume loop: {e}")
                time.sleep(1)
    
    def _process_single_message(self, message: Dict[str, Any]):
        """处理单个消息"""
        try:
            success = self.db_manager.save_message(message)
            if success:
                logger.debug(f"✅ Successfully processed message: {message.get('session_id')}")
            else:
                logger.error(f"❌ Failed to process message: {message.get('session_id')}")
            return success
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
            return False
    
    def _handle_processing_result(self, future):
        """处理处理结果"""
        try:
            success = future.result()
            if not success:
                logger.warning("⚠️ Message processing failed")
        except Exception as e:
            logger.error(f"❌ Error in message processing result: {e}")
    
    def stop(self):
        """停止消费者"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        self.thread_pool.shutdown(wait=True)
        logger.info("✅ Kafka consumer stopped")

def main():
    """主函数"""
    logger.info("🚀 Starting Kafka Consumer Service...")
    
    # 等待服务就绪
    logger.info("⏳ Waiting for services to be ready...")
    time.sleep(10)
    
    db_manager = None
    consumer = None
    
    try:
        # 初始化组件
        db_manager = SimpleDatabaseManager()
        consumer = SimpleKafkaConsumer(db_manager)
        
        # 启动消费者,循环获取kafka消息
        consumer.start()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        if consumer:
            consumer.stop()
        logger.info("✅ Fixed Kafka Consumer Service shutdown complete")

if __name__ == "__main__":
    main()