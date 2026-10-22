# Cómo funciona Kafka en este restaurante

Cada concepto se explica primero con la **analogía del restaurante** y después con su **nombre técnico**, que es el que se usa en una entrevista.

## 1. El diccionario

| En el restaurante | Nombre técnico | En este proyecto |
|---|---|---|
| El mozo que anota comandas | **Productor** (*producer*) | `pedidos-service`, con `KafkaTemplate` |
| La libreta de comandas | **Topic** | `pedidos` |
| Las bandejas de la libreta | **Particiones** | 0, 1 y 2 |
| El número de hoja dentro de una bandeja | **Offset** | `partición 1 \| offset 5` |
| El número de pedido que decide la bandeja | **Key** | `key = "7"` |
| Los mostradores que guardan las bandejas | **Brokers** | `kafka-1`, `kafka-2`, `kafka-3` |
| Fotocopias de cada bandeja | **Réplicas** (*replication factor*) | 3 |
| El mostrador con el original | **Líder** (*leader*) | `Leader: 3` |
| Las fotocopias que están al día | **ISR** (*in-sync replicas*) | `Isr: 3,1,2` |
| El jefe de sala que decide quién es líder | **Controller** (KRaft) | contenedor `controller` |
| Un área del restaurante (el color del mandil) | **Consumer group** | `group.id=cocina` |
| Un trabajador de un área | **Consumidor / instancia** | `cocina-8091` |
| El separador de cada área | **Offset confirmado** (*committed offset*) | `CURRENT-OFFSET` |
| Las comandas que un área aún no lee | **Lag** | `LAG` |
| Repartir las bandejas entre compañeros | **Rebalanceo** (*rebalance*) | log `REBALANCEO` |

> **Kafka no es un microservicio.** Es la infraestructura que conecta microservicios. El topic no tiene lógica: solo guarda mensajes en orden. Los productores y los consumidores son tus aplicaciones.

## 2. El viaje de un pedido

1. El cliente hace `POST /api/pedidos` → `pedidos-service` crea el evento `PEDIDO_CREADO` en JSON.
2. `KafkaTemplate.send("pedidos", key = id del pedido, json)`.
3. El productor calcula la **partición** con la key y envía el mensaje al **líder** de esa partición.
4. El líder lo guarda y espera a que las réplicas del **ISR** lo copien (`acks=all`). Recién entonces responde OK con **partición y offset**.
5. Cada consumer group (`cocina`, `envios`, `facturacion`) hace `poll()`, recibe el mensaje, lo procesa y **confirma su offset** (`ack-mode=record`).
6. Cuando el cliente cobra (`POST /api/pedidos/{id}/pagar`), se publica `PEDIDO_PAGADO` **con la misma key**: cae en la **misma partición**, detrás del `PEDIDO_CREADO`.

## 3. La key decide la partición

Kafka no reparte por plato, ni por turno, ni por par o impar:

```
partición = murmur2(key) % número_de_particiones
```

- **murmur2** es una "licuadora" matemática: convierte la key en un número grande que parece aleatorio.
- **% 3** es el residuo de dividir entre 3, y da 0, 1 o 2.

| key (pedido) | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| partición | 0 | 2 | 2 | 1 | 0 | 1 | 0 | 0 | 2 | 1 |

- **Misma key → misma partición, siempre.** Por eso los eventos de un mismo pedido llegan **en orden**.
- **Keys distintas → se reparten parejo**, pero sin un patrón que se pueda adivinar.
- **Sin key**, Kafka reparte por lotes entre particiones (*sticky partitioner*) y no hay garantía de orden por entidad.
- **Mala key:** una con pocos valores o muy desbalanceada (por ejemplo, `plato` cuando el 80 % pide pollo). Una partición se satura y las demás quedan vacías: es una **hot partition**.
- **El orden solo se garantiza dentro de una partición**, no en todo el topic.

## 4. Offsets: numeran mensajes, no pedidos

- Cada partición numera sus mensajes por separado: 0, 1, 2…
- Un pedido creado y luego pagado son **2 mensajes con 2 offsets distintos** (en la misma partición).
- **log-end-offset**: el número que tendrá el **próximo** mensaje de la partición (en la página, la casilla "próxima").
- **log-start-offset**: el primer mensaje que todavía existe (sube cuando la retención borra los viejos; la numeración nunca se reinicia).
- **Offset confirmado** de un grupo: el **próximo** mensaje que ese grupo va a leer (no el último que leyó).
- **Lag** = log-end-offset − offset confirmado = lo que el grupo tiene pendiente.

## 5. Leer no borra

Los mensajes se quedan en el topic hasta que se cumple la **retención** (`retention.ms`, por defecto 7 días, o `retention.bytes`). Leer no borra nada.

- Esa es la gran diferencia con una cola clásica (RabbitMQ), donde el mensaje desaparece cuando el consumidor lo confirma. Kafka es un **log**: solo se agrega al final.
- Gracias a eso, un grupo nuevo puede leer el pasado, y un grupo puede **reprocesar** desde el inicio.

## 6. Consumer groups: copia para todos o reparto

| Situación | Qué pasa | Modelo |
|---|---|---|
| **Distinto `group.id`** (cocina, envios, facturacion) | Cada grupo recibe **todos** los mensajes, con su propio offset | *Publish/subscribe* |
| **Mismo `group.id`, varias instancias** (dos cocineros) | Se **reparten** las particiones: cada mensaje lo procesa **una sola** instancia del grupo | Cola de trabajo (*competing consumers*) |

- Dentro de un grupo, **una partición la lee una sola instancia a la vez**. Por eso el máximo paralelismo útil es el **número de particiones**: con 3 particiones, una 4.ª instancia se queda **sin partición** (de reserva).
- Cuando una instancia entra o sale, el grupo hace un **rebalanceo** y reparte de nuevo las particiones. En este proyecto la asignación usa `RangeAssignor`: con 2 instancias, una toma `[0, 1]` y la otra `[2]`.
- Si una instancia muere sin avisar, el grupo lo detecta al pasar `session.timeout.ms` (aquí 10 s) sin heartbeats. Con un cierre ordenado (Ctrl+C), avisa y el rebalanceo es inmediato.
- **No hay orden entre grupos.** Cocina no va antes que envíos: cada grupo lee a su ritmo.

### ¿Dónde se guarda el separador de cada área?

En Kafka, en un topic interno llamado **`__consumer_offsets`**, no en el microservicio. Si facturación se cae 10 minutos, al volver le pregunta a Kafka dónde se quedó el grupo `facturacion` y continúa desde ahí. Como se confirma después de procesar (*at-least-once*), en el peor caso se repite el mensaje que se estaba procesando cuando cayó.

## 7. Grupo nuevo: `auto-offset-reset`

Solo aplica cuando el grupo **no tiene offset guardado** (un grupo nuevo, o uno cuyos offsets expiraron):

| Valor | Empieza en | Útil para |
|---|---|---|
| `earliest` | El primer mensaje disponible (log-start-offset) | Procesar todo el historial |
| `latest` | El próximo mensaje que llegue (log-end-offset) | Solo lo nuevo desde que arranca |

Si el grupo **ya tiene** offset guardado, esta propiedad se ignora y continúa desde su offset.

## 8. Reprocesar: resetear offsets

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --group cocina --topic pedidos --reset-offsets --to-earliest --execute
```

- El grupo debe estar **inactivo** (sin instancias corriendo). Si no, Kafka responde *"Assignments can only be reset if the group is inactive"*.
- Otras opciones: `--to-latest`, `--to-offset 10`, `--shift-by -5`, `--to-datetime 2026-10-22T10:00:00.000`.
- Casos de uso: se corrigió un bug y hay que recalcular, o se alimenta un sistema nuevo con el historial.

## 9. Réplicas, líder e ISR

Cada partición tiene 3 copias, una por broker. El **líder** recibe las escrituras y atiende las lecturas; los **seguidores** copian del líder.

| Partición | Réplicas | Líder preferido |
|---|---|---|
| 0 | 3, 1, 2 | broker 3 |
| 1 | 1, 2, 3 | broker 1 |
| 2 | 2, 3, 1 | broker 2 |

Los líderes están repartidos para que cada broker haga una parte del trabajo. El primero de la lista es el **líder preferido**.

### `acks=all` + `min.insync.replicas=2`

- `acks=all`: el líder responde OK solo cuando **todas las réplicas del ISR** guardaron el mensaje.
- `min.insync.replicas=2`: si el ISR tiene **menos de 2** réplicas, el líder **rechaza** las escrituras con `NotEnoughReplicasException`. Prefiere fallar a arriesgarse a perder datos.

### Qué pasa al apagar brokers

| Brokers caídos | Qué pasa |
|---|---|
| 1 | El **controller** elige como nuevo líder a otra réplica **del ISR** (la siguiente de la lista). El ISR baja a 2 → **todo sigue funcionando**, sin perder mensajes. |
| 2 | Queda 1 réplica en el ISR (< 2) → el productor recibe **`NotEnoughReplicasException`**. Reintenta durante `delivery.timeout.ms` (aquí 15 s) y luego falla. **No se pierde nada en silencio.** Los consumidores pueden seguir leyendo lo que ya estaba guardado. |
| Vuelve uno | Primero **copia lo que se perdió** y vuelve al ISR. Luego el controller le **devuelve el liderazgo** que le corresponde (*preferred leader election*), revisando cada `leader.imbalance.check.interval.seconds` (aquí 30 s; por defecto 300 s). |

> Por qué el controller está en un contenedor aparte: si cada broker fuera también controller, al apagar 2 de 3 se perdería la mayoría del quorum KRaft y el clúster se congelaría (el productor solo vería timeouts). En producción se usan 3 controllers.

**La partición la decide la key, no el broker.** Si un broker queda como líder de 2 particiones, siguen siendo 2 particiones distintas, cada una con su numeración. El productor pide la **metadata** ("¿quién es el líder de la partición 1?") y envía al líder actual. El código de la aplicación no se entera.

## 10. Si un paso depende de otro: encadenar eventos

Kafka no garantiza que cocina procese antes que envíos. Si el negocio lo exige:

- **Coreografía** (*event choreography*): cocina, al terminar, publica `PLATO_LISTO` en otro topic (`cocina-eventos`), con la misma key, y envíos escucha ese topic en vez de `pedidos`.
- **Orquestación**: un servicio central ordena los pasos ("cocina: prepara" → espera "listo" → "envíos: despacha").
- **Patrón Saga**: un proceso de negocio de varios pasos entre microservicios, con **compensaciones** si un paso falla (por ejemplo, cancelar el pedido si el pago es rechazado).

## 11. La configuración que importa

**Productor** (`pedidos-service/src/main/resources/application.properties`):

| Propiedad | Valor | Para qué |
|---|---|---|
| `bootstrap-servers` | los 3 brokers | Basta uno para descubrir el resto, pero así sobrevive si uno está caído al arrancar |
| `acks` | `all` | Máxima durabilidad: espera a todas las réplicas del ISR |
| `delivery.timeout.ms` | 15000 | Cuánto reintenta antes de declarar el envío fallido (por defecto 2 min) |
| key / value serializer | `StringSerializer` | Key = id del pedido; value = JSON |

**Consumidores** (`*-service/src/main/resources/application.properties`):

| Propiedad | Valor | Para qué |
|---|---|---|
| `group-id` | `cocina` / `envios` / `facturacion` | Identifica al grupo: offsets propios |
| `client-id` | `cocina-8091` | Nombre de cada instancia dentro del grupo |
| `auto-offset-reset` | `earliest` | Dónde empezar si el grupo no tiene offset |
| `ack-mode` | `record` | Confirma el offset después de **cada** mensaje |
| `max-poll-records` | 10 | Pocos por vuelta, para no pasarse de `max.poll.interval.ms` (5 min) con la pausa de 1 s |
| `session.timeout.ms` | 10000 | Detectar rápido una instancia caída |
| `restaurante.pausa-ms` | 1000 | Pausa artificial para que el lag se note |

## 12. Experimentos para practicar

1. **Varios grupos, mismo topic:** con las 3 áreas encendidas, crea un pedido y míralo en los 3 logs.
2. **Offset por grupo:** apaga facturación, crea una ráfaga de 20 y mira su lag subir; enciéndela y mira cómo baja.
3. **Rebalanceo:** levanta una 2.ª instancia de cocina (puerto 8094), luego una 3.ª y una 4.ª.
4. **Key → partición:** crea un pedido, cóbralo y mira que ambos eventos caen en la misma partición, uno detrás del otro.
5. **Grupo nuevo:** levanta un grupo `auditoria` con `earliest` y otro con `latest`.
6. **Reprocesar:** apaga cocina, resetea sus offsets a `earliest` y vuelve a encenderla.
7. **Brokers:** apaga 1 broker y luego 2, crea pedidos y mira los líderes, el ISR y la respuesta del productor.

Antes de cada uno, conviene **predecir qué va a pasar** y después comprobarlo en la página (http://localhost:8090), en Kafka UI y en los logs.
