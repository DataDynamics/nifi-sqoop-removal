FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 \
    LCA_CONFIG=/etc/load-control/config.yaml
WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
RUN pip install .

COPY alembic.ini ./
COPY alembic ./alembic

RUN useradd --system --uid 10001 lca
USER lca
EXPOSE 8080

# 설정 파일은 /etc/load-control/config.yaml로 마운트한다(config.example.yaml 참고).
# api 프로세스. migration은 배포 파이프라인에서 별도로 실행한다: alembic upgrade head
CMD ["gunicorn", "load_control.main:create_app()", "-k", "uvicorn.workers.UvicornWorker", \
     "-w", "4", "-b", "0.0.0.0:8080", "--timeout", "60", "--graceful-timeout", "30"]
