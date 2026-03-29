# Lambda container image for ticket_ingestor.
#
# Uses the AWS-provided Python 3.11 Lambda base image. astropy is required
# for FITS construction (spectra branch), so container deployment is mandatory.
#
# Build context: services/  (same as other DockerImageFunctions in this project).
# contracts/ is available as a symlink (services/contracts -> ../contracts).
# nova_common is installed directly from source (not as a layer) because
# Lambda layers are not available to container-based functions.

FROM public.ecr.aws/lambda/python:3.11-arm64

# Install dependencies first — separate layer from code so Docker cache is
# reused on code-only changes.
COPY ticket_ingestor/requirements.txt /var/task/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy nova_common shared utilities (replaces the Lambda layer used by zip functions)
COPY nova_common_layer/python/nova_common ${LAMBDA_TASK_ROOT}/nova_common/

# Copy shared contracts models (pydantic ticket + entity models).
# contracts/ is available via the services/contracts symlink.
COPY contracts/ ${LAMBDA_TASK_ROOT}/contracts/

# Copy photometry_ingestor.band_registry (shared band/filter registry used by ticket_ingestor)
COPY photometry_ingestor/ ${LAMBDA_TASK_ROOT}/photometry_ingestor/

# Copy ticket_ingestor as a package — handler.py imports from ticket_ingestor.*
COPY ticket_ingestor/ ${LAMBDA_TASK_ROOT}/ticket_ingestor/

# Copy handler entrypoint to task root so CMD = handler.handle resolves correctly
COPY ticket_ingestor/handler.py ${LAMBDA_TASK_ROOT}/

RUN chmod -R 755 ${LAMBDA_TASK_ROOT}/

CMD ["handler.handle"]
