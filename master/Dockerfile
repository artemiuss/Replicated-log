FROM python:3

RUN pip install --root-user-action=ignore --no-cache-dir -r requirements.txt

WORKDIR /app

COPY . . 

#EXPOSE 5000

CMD [ "python", "./master.py"]
