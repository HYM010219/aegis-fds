# 빌드 위치는 fds 폴더 (docker-compose.yml의 context: ./fds). 공용 파일 common/을 같이 넣기 위해서다.
FROM python:3.12-slim
WORKDIR /app
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY app ./app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
