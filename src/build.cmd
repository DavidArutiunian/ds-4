@echo off
docker build -t joblogger:%1 -f Dockerfile.joblogger .
docker build -t backendapi:%1 -f Dockerfile.backendapi .
docker build -t frontendclient:%1 -f Dockerfile.frontendclient .

md ..\build\build_%1
cp start.cmd ..\build\build_%1
cp stop.cmd ..\build\build_%1
cp delete.cmd ..\build\build_%1
cp config.cmd ..\build\build_%1
