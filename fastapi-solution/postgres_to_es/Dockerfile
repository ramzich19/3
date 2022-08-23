FROM python:3.10

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY . .

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt

ENTRYPOINT ["python", "main.py"]


