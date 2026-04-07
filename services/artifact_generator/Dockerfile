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
# Epic 3 scope: astropy (coordinate formatting, MJD conversion, FITS I/O),
# numpy (array operations), and scipy (kd-tree for band offsets) are added
# to support the artifact generators.  The python:3.11-slim base image is
# retained — compiled wheels for these packages are available on PyPI and
# do not require additional system-level dependencies.

FROM python:3.11-slim

WORKDIR /app

COPY artifact_generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy nova_common shared utilities
COPY nova_common_layer/python/nova_common /app/nova_common/

# Copy shared contracts models (pydantic regeneration models)
COPY contracts/ /app/contracts/

# Copy the task entrypoint and generator modules
COPY artifact_generator/main.py .
COPY artifact_generator/release_publisher.py .
COPY artifact_generator/generators/ ./generators/
COPY artifact_generator/structured_logging.py .

# Band registry — loaded once per Fargate execution (§8.2).
# Sourced from the photometry_ingestor package where it is maintained.
COPY photometry_ingestor/band_registry/band_registry.json ./band_registry.json

CMD ["python", "main.py"]
