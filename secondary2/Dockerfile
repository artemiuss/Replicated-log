FROM python:3

WORKDIR /app

COPY . . 

RUN pip install --root-user-action=ignore --no-cache-dir -r requirements.txt

EXPOSE 8081 8082

CMD ["python", "./secondary.py"]
