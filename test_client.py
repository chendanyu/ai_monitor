#!/usr/bin/env python3
"""
测试客户端 - 提供完整的AI处理数据
模拟已经完成AI处理的客户端，发送完整数据到收集网关
"""

import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# API基础URL
BASE_URL = "http://localhost:8000"

def test_asr():
    """测试ASR接口 - 提供完整的ASR处理数据"""
    data = {
        "audio_data": "base64_encoded_audio_data_here",
        "session_id": f"session_asr_{int(time.time())}",
        "language": "zh-CN",
        "sample_rate": 16000,
        # 以下字段由客户端提供，将保存到数据库
        "output_data": "这是从音频中识别出的文本内容",
        "processing_time_ms": 50,
        "timestamp": datetime.now().isoformat(),
        "status": "success",
        "metadata": {"source": "test_client", "user_id": "test_user_001", "language": "zh-CN"}
    }
    
    try:
        response = requests.post(f"{BASE_URL}/asr/process", json=data)
        if response.status_code == 200:
            result = response.json()
            print(f"✅ ASR Test - Session: {result['session_id']}, Status: {result['status']}")
            print(f"   Message: {result['message']}")
            return True
        else:
            print(f"❌ ASR Test Failed - Status: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ ASR Test Error: {e}")
        return False

def test_llm():
    """测试LLM接口 - 提供完整的LLM处理数据"""
    data = {
        "prompt": "请解释一下人工智能的基本概念",
        "session_id": f"session_llm_{int(time.time())}",
        "max_tokens": 200,
        "temperature": 0.7,
        "model_name": "gpt-3.5-turbo",
        # 以下字段由客户端提供，将保存到数据库
        "output_data": "人工智能是计算机科学的一个分支，旨在创造能够执行通常需要人类智能的任务的机器和软件。",
        "processing_time_ms": 150,
        "timestamp": datetime.now().isoformat(),
        "status": "success",
        "metadata": {"source": "test_client", "user_id": "test_user_001", "tokens_used": 45}
    }
    
    try:
        response = requests.post(f"{BASE_URL}/llm/process", json=data)
        if response.status_code == 200:
            result = response.json()
            print(f"✅ LLM Test - Session: {result['session_id']}, Status: {result['status']}")
            print(f"   Message: {result['message']}")
            return True
        else:
            print(f"❌ LLM Test Failed - Status: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ LLM Test Error: {e}")
        return False

def test_tts():
    """测试TTS接口 - 提供完整的TTS处理数据"""
    data = {
        "text": "这是一个文本转语音的测试示例",
        "session_id": f"session_tts_{int(time.time())}",
        "voice": "alloy",
        "speed": 1.0,
        "audio_format": "mp3",
        # 以下字段由客户端提供，将保存到数据库
        "output_data": "/data/aaaa.wav",
        "processing_time_ms": 80,
        "timestamp": datetime.now().isoformat(),
        "status": "success",
        "metadata": {"source": "test_client", "user_id": "test_user_001", "duration": 3.2, "sample_rate": 22050}
    }
    
    try:
        response = requests.post(f"{BASE_URL}/tts/process", json=data)
        if response.status_code == 200:
            result = response.json()
            print(f"✅ TTS Test - Session: {result['session_id']}, Status: {result['status']}")
            print(f"   Message: {result['message']}")
            return True
        else:
            print(f"❌ TTS Test Failed - Status: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ TTS Test Error: {e}")
        return False

def test_health():
    """测试健康检查"""
    try:
        response = requests.get(f"{BASE_URL}/health")
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Health Check - Status: {result['status']}, Kafka: {result['kafka_connected']}")
            return True
        else:
            print(f"❌ Health Check Failed - Status: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Health Check Error: {e}")
        return False

def stress_test(num_requests=30):
    """压力测试"""
    print(f"🚀 Starting stress test with {num_requests} requests...")
    
    start_time = time.time()
    success_count = 0
    
    def make_request(request_id):
        try:
            data = {
                "prompt": f"压力测试请求 #{request_id}",
                "session_id": f"stress_test_{request_id}_{int(time.time())}",
                "max_tokens": 50,
                "temperature": 0.7,
                "model_name": "gpt-3.5-turbo",
                "output_data": f"这是对压力测试请求 #{request_id} 的回复",  
                "processing_time_ms": 100,
                "timestamp": datetime.now().isoformat(),
                "status": "success",
                "metadata": {"stress_test": True, "request_id": request_id, "tokens_used": 20}
            }
            response = requests.post(f"{BASE_URL}/llm/process", json=data, timeout=10)
            return response.status_code == 200
        except:
            return False
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(make_request, range(num_requests)))
    
    success_count = sum(results)
    total_time = time.time() - start_time
    
    print(f"📊 Stress Test Results:")
    print(f"   Total Requests: {num_requests}")
    print(f"   Successful: {success_count}")
    print(f"   Failed: {num_requests - success_count}")
    print(f"   Total Time: {total_time:.2f}s")
    print(f"   Requests/sec: {num_requests/total_time:.2f}")
    print(f"   Success Rate: {success_count/num_requests*100:.1f}%")

def main():
    """主测试函数"""
    print("🧪 Starting AI Data Collection Tests...")
    print("📝 Testing complete AI processing data submission")
    
    # 等待服务启动
    print("⏳ Waiting for services to start...")
    time.sleep(3)
    
    # 基础测试
    print("\n🔍 Running Basic Tests...")
    test_health()
    test_asr()
    test_llm()
    test_tts()
    
    # 并发测试
    print("\n⚡ Running Concurrent Tests...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for i in range(10):
            if i % 3 == 0:
                futures.append(executor.submit(test_asr))
            elif i % 3 == 1:
                futures.append(executor.submit(test_llm))
            else:
                futures.append(executor.submit(test_tts))
        
        results = [f.result() for f in futures]
        success_rate = sum(results) / len(results) * 100
        print(f"📊 Concurrent Test Success Rate: {success_rate:.1f}%")
    
    # 压力测试
    print("\n🚀 Running Stress Test...")
    stress_test(20)
    
    print("\n🎉 All data collection tests completed!")
    print("💡 All AI processing data is provided by client and forwarded to Kafka")

if __name__ == "__main__":
    main()