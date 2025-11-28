CREATE DATABASE IF NOT EXISTS ai_pipeline;
USE ai_pipeline;

-- AI 模型数据表
CREATE TABLE IF NOT EXISTS ai_model_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    session_id VARCHAR(255) NOT NULL,
    model_type ENUM('ASR', 'LLM', 'TTS') NOT NULL,
    input_data TEXT,
    output_data TEXT,
    timestamp DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    processing_time_ms INT,
    status ENUM('success', 'error', 'processing') DEFAULT 'success',
    error_message TEXT,
    metadata JSON,
    INDEX idx_session_id (session_id),
    INDEX idx_timestamp (timestamp),
    INDEX idx_model_type (model_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 会话表
CREATE TABLE IF NOT EXISTS sessions (
    session_id VARCHAR(255) PRIMARY KEY,
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    status ENUM('active', 'completed', 'failed') DEFAULT 'active',
    total_processing_time_ms INT DEFAULT 0  --累计该会话所有处理步骤的总耗时
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 性能监控表
CREATE TABLE IF NOT EXISTS performance_metrics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    service_name VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DOUBLE NOT NULL,
    timestamp DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_service_metric (service_name, metric_name),
    INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;