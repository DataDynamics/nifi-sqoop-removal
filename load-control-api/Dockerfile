FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 \
    LCA_CONFIG=/etc/load-control/config.yaml
WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
RUN pip install .

COPY alembic.ini ./
COPY alembic ./alembic

RUN useradd --system --uid 10001 lca \
    && mkdir -p /var/log/load-control && chown lca /var/log/load-control
USER lca
EXPOSE 8080

# 설정 파일은 /etc/load-control/config.yaml로 마운트한다(config.example.yaml 참고).
# api 프로세스. bind address·port·workers는 config.yaml의 server 섹션에서 읽는다.
# worker는 같은 이미지에서 명령만 바꾼다: python -m load_control.worker
# migration은 배포 파이프라인에서 별도로 실행한다: alembic upgrade head
CMD ["python", "-m", "load_control.server"]
