FROM python:3.12.2-bookworm

WORKDIR /python-docker

COPY requirements.txt requirements.txt
COPY resource/wbryamlgenerator-0.1.tar.gz resource/wbryamlgenerator-0.1.tar.gz
RUN python -m pip install --upgrade pip
RUN pip --no-cache-dir install -r requirements.txt
RUN pip install resource/wbryamlgenerator-0.1.tar.gz

COPY . .

EXPOSE 5001

CMD ["waitress-serve", "--port=5001", "src.controller:app"]
