@echo off
call config.cmd
docker run -p %NATS_PORT%:4222 --rm -d --name nats nats:2-alpine
docker run -p %REDIS_PORT%:6379 --rm -d --name redis redis:5-alpine
docker run -e NATS_HOST=%NATS_HOST%:%NATS_PORT% -e REDIS_HOST=%REDIS_HOST%:%REDIS_PORT% -d --link %NATS_HOST% --link %REDIS_HOST% --rm --name joblogger joblogger:%1
docker run -e NATS_HOST=%NATS_HOST%:%NATS_PORT% -e REDIS_HOST=%REDIS_HOST%:%REDIS_PORT% --link %NATS_HOST% --link %REDIS_HOST% -d --rm -p %BACKEND_API_PORT%:5000 --name backendapi backendapi:%1
docker run -e BACKEND_API_HOST=%BACKEND_API_HOST% -d --rm --link %BACKEND_API_HOST% -p %FRONTEND_CLIENT_PORT%:80 --name frontendclient frontendclient:%1
