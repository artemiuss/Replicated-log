FROM python:3

WORKDIR /app

COPY . . 

RUN apt-get update && apt-get -y install jq
RUN jq '.Hosts = [(.Hosts[] | .hostname = .type + (if .type != "master" then (.id|tostring) else "" end))]' config.json > config.json.tmp && mv config.json.tmp config.json

RUN pip install --root-user-action=ignore --no-cache-dir -r requirements.txt

EXPOSE 8080

CMD ["python", "./master.py"]
