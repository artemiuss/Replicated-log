FROM python:3-slim-bullseye AS builder
LABEL stage=builder
WORKDIR /app
COPY requirements.txt config.json ./ 
RUN pip install --root-user-action=ignore --no-cache-dir --user -r requirements.txt
# Adapt the config.json for the Docker
RUN apt-get update && apt-get -y install jq
RUN jq '.Hosts = [(.Hosts[] | .hostname = .type + (if .type != "master" then (.id|tostring) else "" end))]' config.json > config.json.tmp && mv config.json.tmp config.json

FROM python:3-slim-bullseye
WORKDIR /app
COPY --from=builder /root/.local /root/.local
COPY --from=builder /app/config.json /app/config.json
COPY master.py secondary.py ./ 
EXPOSE 8080 8081 8082
ENTRYPOINT ["python"]
CMD [""]
