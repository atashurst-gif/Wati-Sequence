FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY wati_sequence.py .
CMD ["python", "wati_sequence.py"]
