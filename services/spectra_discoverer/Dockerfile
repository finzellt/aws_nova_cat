# Lambda container image for spectra_discoverer.
#
# Provider adapters (e.g. eso.py) use astropy for coordinate handling.
# Compiled C extensions require container deployment.
#
# nova_common is installed directly from source (not as a layer) because
# Lambda layers are not available to container-based functions.

FROM public.ecr.aws/lambda/python:3.11-arm64

COPY spectra_discoverer/requirements.txt /var/task/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy nova_common shared utilities (replaces the Lambda layer used by zip functions)
COPY nova_common_layer/python/nova_common ${LAMBDA_TASK_ROOT}/nova_common/

# Copy service code (handler + adapters package)
COPY spectra_discoverer/handler.py ${LAMBDA_TASK_ROOT}/
COPY spectra_discoverer/adapters/ ${LAMBDA_TASK_ROOT}/adapters/

RUN chmod -R 755 ${LAMBDA_TASK_ROOT}/

CMD ["handler.handle"]
