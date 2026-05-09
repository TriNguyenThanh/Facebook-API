FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 5001

CMD ["gunicorn", "core.wsgi:application", "--bind", "0.0.0.0:5001", "--workers", "3", "--threads", "2", "--timeout", "60"]
