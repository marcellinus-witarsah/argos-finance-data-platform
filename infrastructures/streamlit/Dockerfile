# Install Image with Python Version 3.10.11
ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-bullseye AS spark-base

WORKDIR /web-app

COPY ./infrastructures/streamlit/requirements.txt /web-app/requirements.txt
RUN pip install -r /web-app/requirements.txt

COPY /src /web-app/src
COPY /app.py /web-app/app.py

# =======================================================================
# Run init script
# =======================================================================
ENTRYPOINT [ "streamlit", "run", "app.py",  "--server.port", "8080"]