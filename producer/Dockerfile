FROM python:3.10-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
#RUN pip install supervisor
COPY . /app
#COPY supervisor.conf /etc/supervisor/conf.d/supervisord.conf
EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--loop", "asyncio"]
#CMD [ "supervisord" ]