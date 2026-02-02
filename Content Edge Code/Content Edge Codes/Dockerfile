# Define custom function directory
ARG FUNCTION_DIR="/function"

# FROM mcr.microsoft.com/playwright/python:v1.31.0-focal as build-image
FROM mcr.microsoft.com/playwright/python:v1.55.0-jammy as build-image

# Install aws-lambda-cpp build dependencies
RUN apt-get update && \
  apt-get install -y \
  g++ \
  make \
  cmake \
  unzip \
  libcurl4-openssl-dev \
  software-properties-common \
  fonts-liberation \
  libappindicator3-1 \
  libasound2 \
  libatk-bridge2.0-0 \
  libatk1.0-0 \
  libcups2 \
  libdbus-1-3 \
  libgdk-pixbuf2.0-0 \
  libnspr4 \
  libnss3 \
  pciutils \
  xdg-utils

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Copy function code
RUN mkdir -p ${FUNCTION_DIR}
COPY . ${FUNCTION_DIR}
# Add .env file into the function directory
COPY .env ${FUNCTION_DIR}/.env
# Install the function's dependencies
RUN pip install \
    --target ${FUNCTION_DIR} \
        awslambdaric

# Add requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --target ${FUNCTION_DIR} -r requirements.txt

# Multi-stage build: grab a fresh copy of the base image
FROM mcr.microsoft.com/playwright/python:v1.55.0-jammy

# Include global arg in this stage of the build
ARG FUNCTION_DIR
# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

# Copy in the built dependencies
COPY --from=build-image ${FUNCTION_DIR} ${FUNCTION_DIR}

# Set runtime interface client as default command for the container runtime
ENTRYPOINT [ "python", "-m", "awslambdaric" ]
# Pass the name of the function handler as an argument to the runtime
CMD [ "app.lambda_handler" ]
