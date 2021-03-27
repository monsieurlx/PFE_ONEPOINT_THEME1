FROM python:3.6

WORKDIR /app

ENV FLASK_APP=appOP.py

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["python", "appOP.py"]
