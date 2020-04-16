@echo off
docker rmi joblogger:%1;
docker rmi backendapi:%1
docker rmi frontendclient:%1
docker image prune -f
