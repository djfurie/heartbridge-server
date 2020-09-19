FROM tiangolo/uvicorn-gunicorn
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 8000
COPY ./app .
# CMD ["uvicorn", "main:app", "--reload"]
