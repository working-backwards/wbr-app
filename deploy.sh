git reset --hard
git pull
docker build -t wbr:dev .
docker rm -f wbr  || true
docker run -d -v ~/.aws:/root/.aws/ --env environment='organisation' --name wbr -p 5001:5001 wbr:dev

