FROM python:3.10

# set work directory
WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install dependencies
COPY ./requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt --no-cache-dir

# copy project
COPY . .

EXPOSE 8000

CMD ["python", "main.py"]
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0"]
