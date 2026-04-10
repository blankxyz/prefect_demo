FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 将源码全部复制进来
COPY . /app/

EXPOSE 8080
# 运行后端 API 服务
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
