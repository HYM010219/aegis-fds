# 온체인 수집기 이미지 (k3s 16-collector-deployment.yaml).
# 빌드 위치는 저장소 루트: docker build -f collector/Dockerfile -t aegis-collector:latest .
# 넣을 파일은 같은 폴더의 Dockerfile.dockerignore가 정한다 (collector/, bots/rate_control.py만).
FROM python:3.12-slim
WORKDIR /app
COPY collector/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY bots/rate_control.py ./bots/rate_control.py
COPY collector ./collector
CMD ["python", "-m", "collector.collector_alchemy"]
