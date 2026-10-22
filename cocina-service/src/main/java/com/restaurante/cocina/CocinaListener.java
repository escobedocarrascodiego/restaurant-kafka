package com.restaurante.cocina;

import java.time.LocalTime;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class CocinaListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(CocinaListener.class);

    private final JsonMapper jsonMapper;
    private final long pausaMs;
    private final String area;
    private final String puerto;

    // Solo para la pagina index.html: ultimas comandas procesadas y particiones asignadas
    private final Deque<Map<String, Object>> ultimos = new ConcurrentLinkedDeque<>();
    private volatile List<Integer> asignadas = List.of();

    public CocinaListener(JsonMapper jsonMapper,
                          @Value("${restaurante.pausa-ms}") long pausaMs,
                          @Value("${spring.kafka.consumer.group-id}") String groupId,
                          @Value("${server.port}") String puerto) {
        this.jsonMapper = jsonMapper;
        this.pausaMs = pausaMs;
        this.area = groupId.toUpperCase();
        this.puerto = puerto;
    }

    // Sin groupId aqui: usa spring.kafka.consumer.group-id del application.properties
    @KafkaListener(topics = "pedidos")
    public void recibir(ConsumerRecord<String, String> record) throws InterruptedException {
        PedidoEvento evento = jsonMapper.readValue(record.value(), PedidoEvento.class);

        log.info("[{}] pedido #{} {} | partición {} | offset {} | hilo {}",
                area, evento.pedidoId(), evento.tipo(), record.partition(), record.offset(),
                Thread.currentThread().getName());
        ultimos.addFirst(Map.of("pedidoId", evento.pedidoId(), "tipo", evento.tipo(), "particion", record.partition(),
                "offset", record.offset(), "hora", LocalTime.now().withNano(0).toString()));
        while (ultimos.size() > 10) {
            ultimos.pollLast();
        }

        if (PedidoEvento.PEDIDO_CREADO.equals(evento.tipo())) {
            log.info("         -> preparando {} para la mesa {}", evento.plato(), evento.mesa());
        } else {
            log.info("         -> pedido pagado, se limpia la mesa {}", evento.mesa());
        }

        Thread.sleep(pausaMs);
    }

    // Lo llama Spring cuando el grupo termina un REBALANCEO y a esta instancia le tocan bandejas
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        asignadas = assignments.keySet().stream().map(TopicPartition::partition).sorted().toList();
        log.info("[{}] ===> REBALANCEO (instancia :{}): me asignaron las particiones {}",
                area, puerto, assignments.isEmpty() ? "NINGUNA (me quedé sin bandeja)" : numeros(assignments.keySet()));
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        List<Integer> quitadas = partitions.stream().map(TopicPartition::partition).toList();
        asignadas = asignadas.stream().filter(p -> !quitadas.contains(p)).toList();
        log.info("[{}] <=== REBALANCEO (instancia :{}): me quitaron las particiones {}",
                area, puerto, numeros(partitions));
    }

    public Map<String, Object> historial() {
        return Map.of("grupo", area.toLowerCase(), "puerto", puerto, "particiones", asignadas, "ultimos", List.copyOf(ultimos));
    }

    private static String numeros(Collection<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::partition).sorted().toList().toString();
    }
}
