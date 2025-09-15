FROM python:3.10-slim

ARG SNOWFLAKE_ACCOUNT_NAME
ARG SNOWFLAKE_USER
ARG SNOWFLAKE_PASSWORD
ARG SNOWFLAKE_ROLE

ENV SNOWFLAKE_ACCOUNT_NAME=$SNOWFLAKE_ACCOUNT_NAME
ENV SNOWFLAKE_USER=$SNOWFLAKE_USER
ENV SNOWFLAKE_PASSWORD=$SNOWFLAKE_PASSWORD
ENV SNOWFLAKE_ROLE=$SNOWFLAKE_ROLE

# Install dependencies
RUN pip install --upgrade pip && pip install snowflake-snowpark-python

# Set the working directory in the container
WORKDIR /app/src
# Copy only the code files from current directory into the container at /app/src
COPY src/app.py .
COPY src/configs ./configs
COPY src/dq ./dq
COPY src/transform ./transform
COPY requirements.txt /app/
# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
ENV PYTHONPATH=/app
CMD ["python", "app.py"]