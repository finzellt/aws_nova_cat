# Fargate container image for artifact_generator.
#
# Plain Python entrypoint (not a Lambda handler). The Step Functions
# workflow launches this container via ECS RunTask .sync, passing
# PLAN_ID as a container override environment variable.
#
# Build context: services/  (same as DockerImageFunctions in this project).
# contracts/ is available as a symlink (services/contracts -> ../contracts).
# nova_common is installed directly from source (not as a layer) because
# Lambda layers are not available to Fargate tasks.
#
# Epic 2 scope: boto3 + pydantic only. Epic 3 adds astropy for FITS I/O
# and the base image may change to accommodate compiled dependencies.

FROM python:3.11-slim

WORKDIR /app

COPY artifact_generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy nova_common shared utilities
COPY nova_common_layer/python/nova_common /app/nova_common/

# Copy shared contracts models (pydantic regeneration models)
COPY contracts/ /app/contracts/

# Copy the task entrypoint
COPY artifact_generator/main.py .

CMD ["python", "main.py"]
