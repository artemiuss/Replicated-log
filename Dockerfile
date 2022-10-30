FROM python:3

VOLUME ["/code"]
ADD . /code
WORKDIR /code

EXPOSE 5000
#CMD ["python", "-m", "http.server", "5000"]
CMD ["./1.py"]