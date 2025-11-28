#!/usr/bin/env python3
"""
AI Model Pipeline Web API Service - 直接保存到数据库
接收客户端提供的完整AI处理数据并直接保存到MySQL数据库
"""

import json
import uuid
import logging
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from contextlib import asynccontextmanager

import mysql.connector
from mysql.connector import Error, pooling
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
import uvicorn

# 配置
log_dir = Path("/tmp/ai-pipeline-logs")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_dir / 'web_api_service_direct.db.log')
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': 'localhost',
    'database': 'ai_pipeline',
    'user': 'aiuser',
    'password': 'aipassword',
    'charset': 'utf8mb4'
}

MAX_WORKERS = 10

class BaseModelConfig:
    model_config = {"protected_namespaces": ()}

# 数据模型 - 包含所有需要保存到数据库的字段
class ASRRequest(BaseModelConfig, BaseModel):
    audio_data: str = Field(..., description="Base64编码的音频数据或音频文件路径")
    session_id: str = Field(..., description="会话ID")
    language: str = Field("zh-CN", description="语言代码")
    sample_rate: int = Field(16000, description="采样率")
    # 以下字段由客户端提供，将保存到数据库
    output_data: str = Field(..., description="ASR输出数据（字符串格式）")
    processing_time_ms: int = Field(..., description="处理时间(毫秒)")
    timestamp: datetime = Field(..., description="时间戳")
    status: str = Field("success", description="状态: success, error, processing")
    metadata: Optional[Dict[str, Any]] = Field(None, description="元数据")

class LLMRequest(BaseModelConfig, BaseModel):
    prompt: str = Field(..., description="输入提示")
    session_id: str = Field(..., description="会话ID")
    max_tokens: int = Field(1024, description="最大token数")
    temperature: float = Field(0.7, description="温度参数")
    model_name: str = Field("gpt-3.5-turbo", description="模型名称")
    # 以下字段由客户端提供，将保存到数据库
    output_data: str = Field(..., description="LLM输出数据（字符串格式）")
    processing_time_ms: int = Field(..., description="处理时间(毫秒)")
    timestamp: datetime = Field(..., description="时间戳")
    status: str = Field("success", description="状态: success, error, processing")
    metadata: Optional[Dict[str, Any]] = Field(None, description="元数据")

class TTSRequest(BaseModelConfig, BaseModel):
    text: str = Field(..., description="要合成的文本")
    session_id: str = Field(..., description="会话ID")
    voice: str = Field("alloy", description="语音类型")
    speed: float = Field(1.0, description="语速")
    audio_format: str = Field("mp3", description="音频格式")
    # 以下字段由客户端提供，将保存到数据库
    output_data: str = Field(..., description="TTS输出数据（字符串格式）")
    processing_time_ms: int = Field(..., description="处理时间(毫秒)")
    timestamp: datetime = Field(..., description="时间戳")
    status: str = Field("success", description="状态: success, error, processing")
    metadata: Optional[Dict[str, Any]] = Field(None, description="元数据")

# 网关响应模型
class GatewayResponse(BaseModelConfig, BaseModel):
    session_id: str = Field(..., description="会话ID")
    model_type: str = Field(..., description="模型类型")
    status: str = Field(..., description="状态")
    message: str = Field(..., description="响应消息")
    timestamp: datetime = Field(..., description="时间戳")
    storage_method: str = Field(..., description="存储方式")

# 数据库管理器
class DatabaseManager:
    def __init__(self):
        self.connection_pool = None
        self._init_connection_pool()
    
    def _init_connection_pool(self):
        """初始化数据库连接池"""
        try:
            self.connection_pool = pooling.MySQLConnectionPool(
                pool_name="web_api_db_pool",
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
            
            logger.info(f"🔄 Attempting to save message for session: {message['session_id']}")
            
            # 插入AI模型数据
            insert_query = """
            INSERT INTO ai_model_data 
            (session_id, model_type, input_data, output_data, processing_time_ms, timestamp, status, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # 准备数据
            input_data = message.get('input_data', {})
            if not isinstance(input_data, str):
                input_data = json.dumps(input_data, ensure_ascii=False)
                
            output_data = message.get('output_data', '')
            if not isinstance(output_data, str):
                output_data = json.dumps(output_data, ensure_ascii=False)
            
            timestamp = message['timestamp']
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            
            cursor.execute(insert_query, (
                message['session_id'],
                message['model_type'],
                input_data,
                output_data,
                message.get('processing_time_ms', 0),
                timestamp,
                message.get('status', 'success'),
                json.dumps(message.get('metadata', {}), ensure_ascii=False)
            ))
            
            # 更新会话信息
            self._update_session(cursor, message)
            
            connection.commit()
            logger.info(f"✅ Successfully saved message for session: {message['session_id']}")
            return True
            
        except Error as e:
            logger.error(f"❌ Database error saving message: {e}")
            logger.error(f"Message data: {message}")
            if connection:
                connection.rollback()
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error saving message: {e}")
            logger.error(f"Message data: {message}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
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
        
        timestamp = message['timestamp']
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        if cursor.fetchone():
            # 更新现有会话
            update_query = """
            UPDATE sessions 
            SET updated_at = %s, 
                total_processing_time_ms = total_processing_time_ms + %s
            WHERE session_id = %s
            """
            cursor.execute(update_query, (
                timestamp,
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
                timestamp,
                timestamp,
                message.get('processing_time_ms', 0)
            ))
    
    def check_connection(self):
        """检查数据库连接是否正常"""
        try:
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            connection.close()
            return True
        except Error as e:
            logger.error(f"Database connection check failed: {e}")
            return False

# 全局变量，用于在后台任务中访问数据库管理器
db_manager = None

def _save_to_database_async(message: Dict[str, Any]):
    """异步保存消息到数据库"""
    global db_manager
    try:
        logger.info(f"🔄 Starting async DB save for session: {message.get('session_id')}")
        success = db_manager.save_message(message)
        if success:
            logger.info(f"✅ Async DB save successful for session: {message.get('session_id')}")
        else:
            logger.error(f"❌ Async DB save failed for session: {message.get('session_id')}")
        return success
    except Exception as e:
        logger.error(f"❌ Error in async DB save for session {message.get('session_id')}: {e}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return False

# 生命周期管理
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_manager
    # 启动时初始化
    db_manager = DatabaseManager()
    app.state.db_manager = db_manager
    app.state.thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    
    logger.info("API Gateway with direct DB storage startup complete")
    
    yield
    
    # 关闭时清理
    logger.info("API Gateway shutdown started")
    app.state.thread_pool.shutdown(wait=True)
    logger.info("API Gateway shutdown complete")

# 创建FastAPI应用
app = FastAPI(
    title="AI Model Pipeline Data Collection API - Direct DB Storage",
    description="接收客户端提供的完整AI处理数据并直接保存到数据库",
    version="1.0.0",
    lifespan=lifespan
)

@app.post("/asr/process", response_model=GatewayResponse)
async def process_asr(request: ASRRequest, background_tasks: BackgroundTasks):
    """接收ASR处理结果并直接保存到数据库"""
    try:
        # 构建数据库消息 - 包含所有需要保存到数据库的字段
        db_message = {
            "session_id": request.session_id,
            "model_type": "ASR",
            "input_data": {"audio_data": request.audio_data, "language": request.language, "sample_rate": request.sample_rate},
            "output_data": request.output_data,
            "processing_time_ms": request.processing_time_ms,
            "timestamp": request.timestamp.isoformat(),
            "status": request.status,
            "metadata": request.metadata or {}
        }
        
        # 在后台任务中直接保存到数据库
        background_tasks.add_task(_save_to_database_async, db_message)
        
        logger.info(f"ASR data received for session {request.session_id} - queued for direct DB storage")
        
        return GatewayResponse(
            session_id=request.session_id,
            model_type="ASR",
            status="accepted",
            message="ASR data received and queued for direct database storage",
            timestamp=datetime.now(),
            storage_method="direct_database"
        )
        
    except Exception as e:
        logger.error(f"ASR data processing error for session {request.session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"ASR data processing failed: {str(e)}")

@app.post("/llm/process", response_model=GatewayResponse)
async def process_llm(request: LLMRequest, background_tasks: BackgroundTasks):
    """接收LLM处理结果并直接保存到数据库"""
    try:
        # 构建数据库消息 - 包含所有需要保存到数据库的字段
        db_message = {
            "session_id": request.session_id,
            "model_type": "LLM",
            "input_data": {"prompt": request.prompt, "max_tokens": request.max_tokens, "temperature": request.temperature, "model_name": request.model_name},
            "output_data": request.output_data,
            "processing_time_ms": request.processing_time_ms,
            "timestamp": request.timestamp.isoformat(),
            "status": request.status,
            "metadata": request.metadata or {}
        }
        
        # 在后台任务中直接保存到数据库
        background_tasks.add_task(_save_to_database_async, db_message)
        
        logger.info(f"LLM data received for session {request.session_id} - queued for direct DB storage")
        
        return GatewayResponse(
            session_id=request.session_id,
            model_type="LLM",
            status="accepted",
            message="LLM data received and queued for direct database storage",
            timestamp=datetime.now(),
            storage_method="direct_database"
        )
        
    except Exception as e:
        logger.error(f"LLM data processing error for session {request.session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"LLM data processing failed: {str(e)}")

@app.post("/tts/process", response_model=GatewayResponse)
async def process_tts(request: TTSRequest, background_tasks: BackgroundTasks):
    """接收TTS处理结果并直接保存到数据库"""
    try:
        # 构建数据库消息 - 包含所有需要保存到数据库的字段
        db_message = {
            "session_id": request.session_id,
            "model_type": "TTS",
            "input_data": {"text": request.text, "voice": request.voice, "speed": request.speed, "audio_format": request.audio_format},
            "output_data": request.output_data,
            "processing_time_ms": request.processing_time_ms,
            "timestamp": request.timestamp.isoformat(),
            "status": request.status,
            "metadata": request.metadata or {}
        }
        
        # 在后台任务中直接保存到数据库
        background_tasks.add_task(_save_to_database_async, db_message)
        
        logger.info(f"TTS data received for session {request.session_id} - queued for direct DB storage")
        
        return GatewayResponse(
            session_id=request.session_id,
            model_type="TTS",
            status="accepted",
            message="TTS data received and queued for direct database storage",
            timestamp=datetime.now(),
            storage_method="direct_database"
        )
        
    except Exception as e:
        logger.error(f"TTS data processing error for session {request.session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"TTS data processing failed: {str(e)}")

@app.post("/llm/process-sync", response_model=GatewayResponse)
async def process_llm_sync(request: LLMRequest):
    """同步版本：接收LLM处理结果并直接保存到数据库（用于调试）"""
    try:
        # 构建数据库消息
        db_message = {
            "session_id": request.session_id,
            "model_type": "LLM",
            "input_data": {"prompt": request.prompt, "max_tokens": request.max_tokens, "temperature": request.temperature, "model_name": request.model_name},
            "output_data": request.output_data,
            "processing_time_ms": request.processing_time_ms,
            "timestamp": request.timestamp.isoformat(),
            "status": request.status,
            "metadata": request.metadata or {}
        }
        
        # 同步保存到数据库
        success = app.state.db_manager.save_message(db_message)
        
        if success:
            logger.info(f"✅ Sync DB save successful for session {request.session_id}")
            return GatewayResponse(
                session_id=request.session_id,
                model_type="LLM",
                status="saved",
                message="LLM data successfully saved to database",
                timestamp=datetime.now(),
                storage_method="direct_database_sync"
            )
        else:
            logger.error(f"❌ Sync DB save failed for session {request.session_id}")
            raise HTTPException(status_code=500, detail="Failed to save data to database")
        
    except Exception as e:
        logger.error(f"LLM sync data processing error for session {request.session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"LLM data processing failed: {str(e)}")

@app.get("/debug/db-check")
async def debug_db_check():
    """调试端点：检查数据库连接和表结构"""
    try:
        connection = app.state.db_manager.connection_pool.get_connection()
        cursor = connection.cursor()
        
        # 检查表是否存在
        cursor.execute("SHOW TABLES LIKE 'ai_model_data'")
        ai_table_exists = cursor.fetchone() is not None
        
        cursor.execute("SHOW TABLES LIKE 'sessions'")
        sessions_table_exists = cursor.fetchone() is not None
        
        # 获取实际的表结构
        ai_columns = []
        if ai_table_exists:
            cursor.execute("DESCRIBE ai_model_data")
            ai_columns = [column[0] for column in cursor.fetchall()]
        
        sessions_columns = []
        if sessions_table_exists:
            cursor.execute("DESCRIBE sessions")
            sessions_columns = [column[0] for column in cursor.fetchall()]
        
        # 检查数据数量
        cursor.execute("SELECT COUNT(*) as count FROM ai_model_data")
        total_records = cursor.fetchone()[0]
        
        # 使用timestamp字段查询最近的数据
        if ai_table_exists and 'timestamp' in ai_columns:
            cursor.execute("SELECT session_id, model_type, timestamp FROM ai_model_data ORDER BY timestamp DESC LIMIT 5")
            recent_records = cursor.fetchall()
        else:
            recent_records = []
        
        cursor.close()
        connection.close()
        
        return {
            "database_connected": True,
            "tables": {
                "ai_model_data": ai_table_exists,
                "sessions": sessions_table_exists
            },
            "ai_model_data_columns": ai_columns,
            "sessions_columns": sessions_columns,
            "total_records": total_records,
            "recent_records": recent_records
        }
        
    except Exception as e:
        logger.error(f"Debug DB check failed: {e}")
        return {
            "database_connected": False,
            "error": str(e)
        }

@app.get("/health")
async def health_check():
    """健康检查端点"""
    db_healthy = (hasattr(app.state, 'db_manager') and 
                 app.state.db_manager.check_connection())
    
    return {
        "status": "healthy" if db_healthy else "degraded",
        "timestamp": datetime.now().isoformat(),
        "database_connected": db_healthy,
        "storage_method": "direct_database",
        "service": "ai_data_collection_gateway_direct_db"
    }

@app.get("/")
async def root():
    """根端点"""
    return {
        "message": "AI Model Pipeline Data Collection API - Direct DB Storage",
        "version": "1.0.0",
        "description": "接收客户端提供的完整AI处理数据并直接保存到数据库",
        "storage_method": "direct_database",
        "endpoints": {
            "asr": "/asr/process",
            "llm": "/llm/process", 
            "tts": "/tts/process",
            "llm_sync_debug": "/llm/process-sync",
            "debug_db": "/debug/db-check",
            "health": "/health"
        }
    }

if __name__ == "__main__":
    uvicorn.run(
        app,  # 直接使用app对象，而不是字符串
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )