
# AI应用数据监控框架 

## 一、技术背景
  企业级AI应用通常需要对模型的输入输出数据进行持久化存储，用于：

1.   模型效果分析与优化
2.   数据审计与合规性
3.   用户行为分析
4.   故障排查与回放

## 二、文件说明
1. Docker Compose 配置 (docker-compose.yml)

  volumes:
    - mysql_data:/var/lib/mysql
    - ./init.sql:/docker-entrypoint-initdb.d/init.sql
           
  含义：         
  `mysql_data`：在Docker Compose文件顶部定义的**命名卷(named volume)**
  `/var/lib/mysql`：MySQL容器内存储数据库文件的目录
  **作用：**
     - 创建一个持久化的数据卷来存储MySQL的所有数据
     - 即使容器被删除或重启，数据库数据也不会丢失
     - 数据保存在Docker管理的存储区域

  查看和管理：

  ```bash
  # 查看所有数据卷
  docker volume ls

  # 查看具体卷信息
  docker volume inspect ai-pipeline_mysql_data

  # 删除数据卷（谨慎操作，会丢失数据！）
  docker volume rm ai-pipeline_mysql_data
  ```

  含义：
    ./init.sql：宿主机上的SQL初始化脚本文件
    /docker-entrypoint-initdb.d/init.sql：容器内的初始化脚本目录

  作用：
    将本地文件挂载到容器内
    MySQL容器启动时会自动执行/docker-entrypoint-initdb.d/目录下的所有.sql文件
    用于数据库的初始化和表结构创建

2. MySQL 初始化脚本 (init.sql)

3. Web API 服务 

   方案一：web_api_kafka.py，将数据推送给kafka消息队列，后面有kafka_consumer_service.py获取消息并保存到数据库中

   方案二：web_api_db.py，将数据直接保存到mysql

4. Kafka 消费者服务 (kafka_consumer_service.py)

5. 测试客户端 (test_client.py)

6. 环境配置和依赖 (requirements.txt)

## 三、部署和运行指南
### 第一步：启动基础设施
#创建项目目录

cd /home/chen

mkdir ai-pipeline
cd ai-pipeline

#将上述文件保存到对应位置

#启动Kafka和MySQL
docker-compose up -d

#检查服务状态
docker-compose ps

如果像停止docker：

```
# 停止现有服务
docker-compose down

# 删除旧的数据卷（如果需要完全重置）
docker volume rm ai-pipeline_mysql_data
```



### 第二步：安装Python依赖

conda activate onnx

pip install -r requirements.txt


### 第三步：启动Web API服务
python web_api_service.py


### 第四步：启动Kafka消费者服务
python kafka_consumer_service.py


### 第五步：测试系统
python test_client.py

```
#进入容器中mysql，查询数据：
docker-compose exec mysql mysql -u aiuser -paipassword ai_pipeline

#在MySQL提示符下运行查询
SELECT * FROM ai_model_data ORDER BY timestamp DESC LIMIT 1;
SELECT model_type, COUNT(*) as count FROM ai_model_data GROUP BY model_type;
SELECT * FROM sessions ORDER BY created_at DESC LIMIT 3;
EXIT;
```


## 四、性能优化特性
高并发处理: 使用多线程和异步处理

连接池: MySQL连接池管理数据库连接

批量操作: Kafka消费者批量处理消息

缓冲区: 消息缓冲区减少数据库写入频率

压缩: Kafka消息压缩减少网络传输

多消费者: 多个Kafka消费者实例提高吞吐量

健康监控: 完整的健康检查和性能指标

这个解决方案提供了完整的AI模型数据处理管道，具有高并发、高可靠性和良好的扩展性。



五、源码

请移步https://github.com/chendanyu/ai_monitor