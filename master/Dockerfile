FROM python:3

WORKDIR /app

COPY . . 

RUN pip install --root-user-action=ignore --no-cache-dir -r requirements.txt

EXPOSE 8080

CMD ["python", "./master.py"]
