FROM python:3.8

LABEL maintainer="jloeve/LonelySteve"


COPY ./main.py ./requirements.txt /app/
COPY ./src /app/src

RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r /app/requirements.txt

ENV CONFIG_FILE_PATH=${CONFIG_FILE_PATH:-"/data/config.ini"}
ENV LOGGER_LOG_FILE_PATH=${LOGGER_LOG_FILE_PATH:-"/data/logs/nmdm-fetcher.log"}

VOLUME [ "/data" ]

EXPOSE 8000

WORKDIR /app

ENTRYPOINT ["uvicorn", "--host", "0.0.0.0", "main:app"]
