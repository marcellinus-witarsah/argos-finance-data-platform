# Install Image with Python Version 3.10.11
ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim AS spark-base

WORKDIR /web-app

COPY ./infrastructures/web_app/streamlit/requirements.txt /web-app/requirements.txt
RUN pip install --no-cache-dir -r /web-app/requirements.txt

COPY /src /web-app/src
COPY /app.py /web-app/app.py

# =======================================================================
# Run init script
# =======================================================================
ENTRYPOINT [ "streamlit", "run", "app.py",  "--server.port", "8080"]