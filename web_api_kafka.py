#!/usr/bin/env python3
"""
AI Model Pipeline Web API Service - 数据收集网关
接收客户端提供的完整AI处理数据并转发到Kafka
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

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from kafka import KafkaProducer
import uvicorn

# 配置
log_dir = Path("/tmp/ai-pipeline-logs")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_dir / 'web_api_service.log')
    ]
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9093'
KAFKA_TOPIC = 'ai_model_data'
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
    kafka_topic: str = Field(..., description="Kafka主题")

# Kafka生产者管理器
class KafkaProducerManager:
    def __init__(self):
        self.producer = None
        self.connected = False
        self._init_producer()
    
    def _init_producer(self):
        """初始化Kafka生产者"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                acks='all',
                retries=3
            )
            self.connected = True
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Kafka producer initialization failed: {e}")
            self.connected = False
    
    def send_message(self, topic: str, message: Dict[str, Any]):
        """发送消息到Kafka"""
        if not self.connected or not self.producer:
            logger.warning("Kafka not connected, message not sent")
            return False
            
        try:
            self.producer.send(topic, message)
            logger.debug(f"Message sent to {topic} for session {message.get('session_id')}")
            return True
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")
            return False
    
    def close(self):
        """关闭生产者"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")

# 生命周期管理
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时初始化
    app.state.kafka_manager = KafkaProducerManager()
    app.state.thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    logger.info("API Gateway startup complete")
    
    yield
    
    # 关闭时清理
    logger.info("API Gateway shutdown started")
    app.state.kafka_manager.close()
    app.state.thread_pool.shutdown(wait=True)
    logger.info("API Gateway shutdown complete")

# 创建FastAPI应用
app = FastAPI(
    title="AI Model Pipeline Data Collection API",
    description="接收客户端提供的完整AI处理数据并转发到Kafka",
    version="1.0.0",
    lifespan=lifespan
)

@app.post("/asr/process", response_model=GatewayResponse)
async def process_asr(request: ASRRequest, background_tasks: BackgroundTasks):
    """接收ASR处理结果并转发到Kafka"""
    try:
        # 构建Kafka消息 - 包含所有需要保存到数据库的字段
        kafka_message = {
            "session_id": request.session_id,
            "model_type": "ASR",
            "input_data": request.audio_data,
            "output_data": request.output_data,  # 直接使用字符串
            "processing_time_ms": request.processing_time_ms,
            "timestamp": request.timestamp.isoformat(),
            "status": request.status,
            "metadata": request.metadata or {}
        }
        
        # 在后台任务中发送到Kafka
        background_tasks.add_task(app.state.kafka_manager.send_message, KAFKA_TOPIC, kafka_message)
        
        logger.info(f"ASR data received for session {request.session_id}")
        
        return GatewayResponse(
            session_id=request.session_id,
            model_type="ASR",
            status="accepted",
            message="ASR data received and forwarded to Kafka",
            timestamp=datetime.now(),
            kafka_topic=KAFKA_TOPIC
        )
        
    except Exception as e:
        logger.error(f"ASR data processing error for session {request.session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"ASR data processing failed: {str(e)}")

@app.post("/llm/process", response_model=GatewayResponse)
async def process_llm(request: LLMRequest, background_tasks: BackgroundTasks):
    """接收LLM处理结果并转发到Kafka"""
    try:
        # 构建Kafka消息 - 包含所有需要保存到数据库的字段
        kafka_message = {
            "session_id": request.session_id,
            "model_type": "LLM",
            "input_data": request.prompt,
            "output_data": request.output_data,  # 直接使用字符串
            "processing_time_ms": request.processing_time_ms,
            "timestamp": request.timestamp.isoformat(),
            "status": request.status,
            "metadata": request.metadata or {}
        }
        
        # 在后台任务中发送到Kafka
        background_tasks.add_task(app.state.kafka_manager.send_message, KAFKA_TOPIC, kafka_message)
        
        logger.info(f"LLM data received for session {request.session_id}")
        
        return GatewayResponse(
            session_id=request.session_id,
            model_type="LLM",
            status="accepted",
            message="LLM data received and forwarded to Kafka",
            timestamp=datetime.now(),
            kafka_topic=KAFKA_TOPIC
        )
        
    except Exception as e:
        logger.error(f"LLM data processing error for session {request.session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"LLM data processing failed: {str(e)}")

@app.post("/tts/process", response_model=GatewayResponse)
async def process_tts(request: TTSRequest, background_tasks: BackgroundTasks):
    """接收TTS处理结果并转发到Kafka"""
    try:
        # 构建Kafka消息 - 包含所有需要保存到数据库的字段
        kafka_message = {
            "session_id": request.session_id,
            "model_type": "TTS",
            "input_data": request.text,
            "output_data": request.output_data,  # 直接使用字符串
            "processing_time_ms": request.processing_time_ms,
            "timestamp": request.timestamp.isoformat(),
            "status": request.status,
            "metadata": request.metadata or {}
        }
        
        # 在后台任务中发送到Kafka
        background_tasks.add_task(app.state.kafka_manager.send_message, KAFKA_TOPIC, kafka_message)
        
        logger.info(f"TTS data received for session {request.session_id}")
        
        return GatewayResponse(
            session_id=request.session_id,
            model_type="TTS",
            status="accepted",
            message="TTS data received and forwarded to Kafka",
            timestamp=datetime.now(),
            kafka_topic=KAFKA_TOPIC
        )
        
    except Exception as e:
        logger.error(f"TTS data processing error for session {request.session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"TTS data processing failed: {str(e)}")

@app.get("/health")
async def health_check():
    """健康检查端点"""
    kafka_healthy = (hasattr(app.state, 'kafka_manager') and 
                    app.state.kafka_manager.connected)
    
    return {
        "status": "healthy" if kafka_healthy else "degraded",
        "timestamp": datetime.now().isoformat(),
        "kafka_connected": kafka_healthy,
        "service": "ai_data_collection_gateway"
    }

@app.get("/")
async def root():
    """根端点"""
    return {
        "message": "AI Model Pipeline Data Collection API",
        "version": "1.0.0",
        "description": "接收客户端提供的完整AI处理数据并转发到Kafka",
        "endpoints": {
            "asr": "/asr/process",
            "llm": "/llm/process", 
            "tts": "/tts/process",
            "health": "/health"
        }
    }

if __name__ == "__main__":
    uvicorn.run(
        "web_api_service:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )