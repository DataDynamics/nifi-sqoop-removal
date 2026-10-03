FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 \
    LCA_CONFIG=/etc/load-control/config.yaml
WORKDIR /app

COPY pyproject.toml ./
COPY src/load_control ./src/load_control
RUN pip install .

COPY config/alembic.ini ./config/alembic.ini
COPY src/migrations ./src/migrations

RUN useradd --system --uid 10001 lca \
    && mkdir -p /var/log/load-control && chown lca /var/log/load-control
USER lca
EXPOSE 8080

# 설정 파일은 /etc/load-control/config.yaml로 마운트한다(config/config.example.yaml 참고).
# 컨테이너에서는 logging.file.path를 /var/log/load-control/load-control.log처럼 절대 경로로 둔다.
# api 프로세스. bind address·port·workers는 config.yaml의 server 섹션에서 읽는다.
# worker는 같은 이미지에서 명령만 바꾼다: python -m load_control.worker
# migration은 배포 파이프라인에서 별도로 실행한다: alembic -c config/alembic.ini upgrade head
CMD ["python", "-m", "load_control.server"]
