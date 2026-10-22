@echo off
rem Levanta OTRA instancia de un servicio en una ventana nueva (funciona desde CMD o PowerShell).
rem Uso:      instancia.cmd ^<servicio^> ^<puerto^> ["argumentos extra de Spring, entre comillas"]
rem Ejemplos: instancia.cmd cocina 8094
rem           instancia.cmd cocina 8095 "--spring.kafka.consumer.group-id=auditoria --spring.kafka.consumer.auto-offset-reset=latest"
rem Para detener la instancia: Ctrl+C en su ventana.

if "%~2"=="" (
  echo Uso: instancia.cmd ^<cocina^|envios^|facturacion^|pedidos^> ^<puerto^> ["argumentos extra"]
  exit /b 1
)
if not exist "%~dp0%~1-service\mvnw.cmd" (
  echo No existe la carpeta %~1-service
  exit /b 1
)
if not defined JAVA_HOME set "JAVA_HOME=C:\Program Files\Apache NetBeans\jdk"

set "EXTRA="
if not "%~3"=="" set "EXTRA= %~3"

start "%~1 :%~2" /D "%~dp0%~1-service" cmd /k .\mvnw.cmd -q spring-boot:run "-Dspring-boot.run.arguments=--server.port=%~2%EXTRA%"
