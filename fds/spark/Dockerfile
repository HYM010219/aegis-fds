# 빌드 위치는 fds 폴더 (docker-compose.yml의 context: ./fds). 공용 파일 common/을 같이 넣기 위해서다.
# 넣을 파일은 같은 폴더의 Dockerfile.dockerignore가 정한다 (spark/, common/만).
# 로컬 compose는 코드를 볼륨으로 덮어써서 재시작만으로 반영되고, k3s는 이미지 안의 코드를 그대로 쓴다.
FROM apache/spark:3.5.7
USER root
COPY spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY common /app/common
COPY spark /app/spark
