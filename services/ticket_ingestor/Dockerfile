# Lambda container image for archive_resolver.
#
# Uses the AWS-provided Python 3.11 Lambda base image. astroquery (and its
# astropy dependency) include compiled C extensions that exceed the Lambda
# zip/layer size limit, requiring container deployment.
#
# nova_common is installed directly from source (not as a layer) because
# Lambda layers are not available to container-based functions.

FROM public.ecr.aws/lambda/python:3.11-arm64

# Install dependencies first — separate layer from code so Docker cache is
# reused on code-only changes.
COPY ticket_ingestor/requirements.txt /var/task/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy nova_common shared utilities (replaces the Lambda layer used by zip functions)
COPY nova_common_layer/python/nova_common ${LAMBDA_TASK_ROOT}/nova_common/

# Copy service code (handler + adapters package)
COPY ticket_ingestor/handler.py ${LAMBDA_TASK_ROOT}/
COPY ticket_ingestor/ddb_writer.py ${LAMBDA_TASK_ROOT}/
COPY ticket_ingestor/photometry_reader.py ${LAMBDA_TASK_ROOT}/
COPY ticket_ingestor/band_registry.py ${LAMBDA_TASK_ROOT}/
COPY ticket_ingestor/spectra_reader.py ${LAMBDA_TASK_ROOT}/
COPY ticket_ingestor/spectra_writer.py ${LAMBDA_TASK_ROOT}/

RUN chmod -R 755 ${LAMBDA_TASK_ROOT}/

CMD ["handler.handle"]
