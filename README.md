# Restaurante Kafka

Proyecto de práctica para ver **Apache Kafka por dentro** con un ejemplo de restaurante: un mozo (productor) anota comandas en una libreta (topic) y tres áreas (consumidores) las leen, cada una a su ritmo.

Con él se puede ver en vivo: varios consumer groups leyendo el mismo topic, offsets y lag por grupo, rebalanceo de particiones, key → partición, `auto-offset-reset` earliest/latest, reseteo de offsets, y réplicas/líderes/ISR al apagar brokers.

> 📖 **¿Cómo funciona por dentro?** Lee [docs/COMO-FUNCIONA.md](docs/COMO-FUNCIONA.md): explica cada concepto con la analogía del restaurante y su nombre técnico.

```
                    ┌───────────────── Kafka (Docker) ─────────────────┐
pedidos-service ──▶ │  topic "pedidos": 3 particiones × 3 réplicas      │ ──▶ cocina-service       (group.id = cocina)
  (productor)       │  brokers kafka-1, kafka-2, kafka-3 + controller   │ ──▶ envios-service       (group.id = envios)
   :8090            │  KRaft                                            │ ──▶ facturacion-service  (group.id = facturacion)
                    └───────────────────────────────────────────────────┘
```

## Tecnologías

| Pieza | Versión |
|---|---|
| Java | 21 (probado con JDK 26) |
| Spring Boot | 4.1.1 (Spring for Apache Kafka 4.1.1, kafka-clients 4.2.1) |
| Apache Kafka | 4.1.0, imagen oficial `apache/kafka`, modo **KRaft** (sin ZooKeeper) |
| Kafka UI | kafbat `v1.3.0` |
| Build | Maven Wrapper (`mvnw`), no hace falta instalar Maven |

## 1. Requisitos

- **Git**
- **Docker Desktop** encendido, con al menos ~3 GB de RAM libres para los contenedores.
- **JDK 21 o superior**. Puede ser el que trae NetBeans (`C:\Program Files\Apache NetBeans\jdk`) o cualquier Temurin/OpenJDK.
- Opcional: **NetBeans** o cualquier IDE con soporte Maven.
- Puertos libres: `8080`, `8090`–`8093`, `9092`–`9094`.

## 2. Clonar

```bash
git clone https://github.com/escobedocarrascodiego/restaurant-kafka.git
cd restaurant-kafka
```

## 3. Levantar Kafka (Docker)

```bash
docker compose up -d
```

Esto levanta:

| Contenedor | Qué es | Acceso desde tu PC |
|---|---|---|
| `controller` | Controller KRaft: decide líderes e ISR | — |
| `kafka-1`, `kafka-2`, `kafka-3` | Los 3 brokers | `localhost:9092`, `9093`, `9094` |
| `kafka-init` | Crea el topic `pedidos` (3 particiones, RF 3, `min.insync.replicas=2`) y termina | — |
| `kafka-ui-restaurante` | Kafka UI | http://localhost:8080 |

Comprobar que quedó bien (debe mostrar las 3 particiones con `Isr: x,y,z`):

```bash
docker logs kafka-init
```

## 4. Levantar los microservicios

Levanta los 4, cada uno en su propia consola (o en NetBeans). El orden recomendado es: pedidos, cocina, envios, facturacion.

| Servicio | Rol | Puerto | group.id |
|---|---|---|---|
| `pedidos-service` | Productor + página web | 8090 | — |
| `cocina-service` | Consumidor | 8091 | `cocina` |
| `envios-service` | Consumidor | 8092 | `envios` |
| `facturacion-service` | Consumidor | 8093 | `facturacion` |

### Opción A: consola (Windows PowerShell)

Dentro de la carpeta de cada servicio. Cambia la ruta del JDK por la tuya:

```powershell
$env:JAVA_HOME="C:\Program Files\Apache NetBeans\jdk"; .\mvnw.cmd spring-boot:run
```

En Linux o macOS:

```bash
./mvnw spring-boot:run
```

### Opción B: NetBeans

1. **File → Open Project** → selecciona las 4 carpetas `*-service`.
2. Clic derecho en cada proyecto → **Run**.
3. Para detener uno, usa el cuadrado rojo de su pestaña en **Output**.

Cada área está lista cuando su log muestra:

```
[COCINA] ===> REBALANCEO (instancia :8091): me asignaron las particiones [0, 1, 2]
```

## 5. Usarlo

- **Página del restaurante en vivo:** http://localhost:8090. Tiene el mozo (crear, cobrar y ráfaga de pedidos), las 3 bandejas con líder/réplicas/ISR, y cada área con su offset, lag y últimas comandas. Se actualiza cada segundo.
- **Kafka UI:** http://localhost:8080 (Topics, Consumers, Brokers).

### API de pedidos-service

| Método y ruta | Qué hace |
|---|---|
| `POST /api/pedidos` con `{"mesa":5,"plato":"Ceviche"}` | Publica `PEDIDO_CREADO` (key = id del pedido) |
| `POST /api/pedidos/{id}/pagar` | Publica `PEDIDO_PAGADO` del mismo pedido (misma key) |
| `POST /api/pedidos/rafaga?cantidad=20` | Publica muchos pedidos de golpe |
| `GET /api/pedidos` | Últimos 15 pedidos |
| `GET /api/kafka/estado` | Brokers, particiones, grupos, offsets (lo usa la página) |

Cada área expone además `GET /api/historial` con sus últimas comandas procesadas.

Ejemplos en PowerShell:

```powershell
curl.exe -X POST localhost:8090/api/pedidos -H "Content-Type: application/json" -d '{\"mesa\":5,\"plato\":\"Ceviche\"}'
curl.exe -X POST localhost:8090/api/pedidos/1/pagar
curl.exe -X POST "localhost:8090/api/pedidos/rafaga?cantidad=20"
```

### Más instancias y grupos nuevos

La forma más fácil en Windows es el script [`instancia.cmd`](instancia.cmd) de la raíz. Abre la instancia en una ventana nueva y funciona desde CMD o PowerShell; se detiene con Ctrl+C en su ventana:

```bat
instancia.cmd cocina 8094
instancia.cmd cocina 8095 "--spring.kafka.consumer.group-id=auditoria --spring.kafka.consumer.auto-offset-reset=latest"
```

Si `JAVA_HOME` no está definido, el script usa el JDK de NetBeans (`C:\Program Files\Apache NetBeans\jdk`).

A mano, segunda instancia de cocina (mismo grupo, se reparten las particiones):

```powershell
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.arguments=--server.port=8094"
```

En NetBeans: clic derecho → Properties → Run → VM Options: `-Dserver.port=8094`.

Un grupo nuevo que solo lee mensajes nuevos (`latest`):

```powershell
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.arguments=--server.port=8095 --spring.kafka.consumer.group-id=auditoria --spring.kafka.consumer.auto-offset-reset=latest"
```

Pausa por mensaje del consumidor (por defecto 1000 ms): `--restaurante.pausa-ms=500`.

## 6. Comandos útiles de Kafka

Ver el topic (líder, réplicas e ISR de cada partición):

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic pedidos
```

Ver los grupos con su offset y lag:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --all-groups
```

Ver qué instancia tiene qué partición:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --all-groups --members --verbose
```

Resetear los offsets de un grupo al inicio (el grupo debe estar **apagado**; cambia `--execute` por `--dry-run` para solo simular):

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --group cocina --topic pedidos --reset-offsets --to-earliest --execute
```

Apagar y encender un broker:

```bash
docker stop kafka-2
```

```bash
docker start kafka-2
```

Devolver el liderazgo a los líderes preferidos (el controller también lo hace solo cada 30 s):

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server kafka-1:19092 --election-type preferred --all-topic-partitions
```

## 7. Apagar todo

```bash
docker compose down
```

Los datos de Kafka viven **dentro de los contenedores** (no hay volúmenes): `down` borra los mensajes y los offsets, y el próximo `up` empieza desde cero. Para pausar sin perder nada, usa `docker compose stop` y luego `docker compose start`.

> ⚠️ Si cambias `docker-compose.yml` con el clúster encendido, haz `docker compose down` antes del `up`. Si Docker recrea solo el contenedor `controller`, se pierden los metadatos del clúster y los brokers quedan inconsistentes.

## 8. Problemas comunes

| Síntoma | Causa y solución |
|---|---|
| `ClassNotFoundException: ${start-class}` al dar Run en NetBeans | Falta `<start-class>` en el pom. Ya está corregido en este repo. |
| Proyectos sin nombre en NetBeans | El pom tenía `<name/>` vacío. Ya está corregido; si persiste: clic derecho → **Reload POM**. |
| `Port 8090 was already in use` | Sigue corriendo una instancia anterior: detenla antes de volver a ejecutar. |
| `Whitelabel Error Page` (404) en http://localhost:8090 | pedidos-service está corriendo con código viejo. Reinícialo. |
| `container name "/kafka-ui" is already in use` | Otro proyecto usa ese nombre. Aquí el contenedor se llama `kafka-ui-restaurante`. |
| `JAVA_HOME not found` al usar `mvnw` | Define `JAVA_HOME` con la ruta de tu JDK (ver paso 4). |
| Los pedidos vuelven a empezar en #1 | Los pedidos se guardan en memoria en pedidos-service; al reiniciarlo, el contador se reinicia. |
| Al apagar un consumidor de golpe, el rebalanceo tarda ~10 s | Es el `session.timeout.ms`. Con Ctrl+C (cierre ordenado) es inmediato. |

## Estructura

```
restaurant-kafka/
├── docker-compose.yml        controller + 3 brokers + kafka-init + Kafka UI
├── docs/COMO-FUNCIONA.md     explicación de los conceptos
├── pedidos-service/          productor (KafkaTemplate) + página web en static/index.html
├── cocina-service/           consumidor (@KafkaListener), group.id = cocina
├── envios-service/           consumidor, group.id = envios
└── facturacion-service/      consumidor, group.id = facturacion
```
