FROM python:3.9-slim

WORKDIR /app

# 复制项目文件
COPY . .

# 安装依赖
RUN pip install --no-cache-dir -e .

# 暴露端口
EXPOSE 8080

# 设置环境变量
ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8080
ENV SEATUNNEL_API_URL=http://seatunnel:8090

# 启动服务
CMD ["python", "-m", "src.seatunnel_mcp"] 