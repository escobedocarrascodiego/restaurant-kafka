package com.restaurante.pedidos;

import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
public class PedidoPublisher {

    private static final Logger log = LoggerFactory.getLogger(PedidoPublisher.class);
    public static final String TOPIC = "pedidos";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper jsonMapper;

    public PedidoPublisher(KafkaTemplate<String, String> kafkaTemplate, JsonMapper jsonMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.jsonMapper = jsonMapper;
    }

    public CompletableFuture<SendResult<String, String>> publicar(PedidoEvento evento) {
        // La KEY es el id del pedido: Kafka hace hash(key) % 3 particiones,
        // asi todos los eventos del mismo pedido caen en la MISMA particion (y en orden).
        String key = String.valueOf(evento.pedidoId());
        String json = jsonMapper.writeValueAsString(evento);

        return kafkaTemplate.send(TOPIC, key, json).whenComplete((resultado, error) -> {
            if (error == null) {
                var meta = resultado.getRecordMetadata();
                recordar(meta.partition(), meta.offset(), evento);
                log.info("ENVIADO   pedido #{} {} | key={} | partición {} | offset {}",
                        evento.pedidoId(), evento.tipo(), key, meta.partition(), meta.offset());
            } else {
                log.error("FALLÓ     pedido #{} {} | key={} | {}",
                        evento.pedidoId(), evento.tipo(), key, causaRaiz(error));
            }
        });
    }

    // Solo para la pagina index.html: las ultimas comandas enviadas a cada particion (bandeja)
    private final Map<Integer, Deque<Map<String, Object>>> enviados = new ConcurrentHashMap<>();

    private void recordar(int particion, long offset, PedidoEvento evento) {
        Deque<Map<String, Object>> bandeja = enviados.computeIfAbsent(particion, p -> new ConcurrentLinkedDeque<>());
        bandeja.addLast(Map.of("offset", offset, "pedidoId", evento.pedidoId(), "tipo", evento.tipo(),
                "mesa", evento.mesa(), "plato", evento.plato()));
        while (bandeja.size() > 12) {
            bandeja.pollFirst();
        }
    }

    public List<Map<String, Object>> enviados(int particion) {
        return enviados.getOrDefault(particion, new ConcurrentLinkedDeque<>()).stream()
                .sorted(Comparator.comparingLong(m -> (Long) m.get("offset")))
                .toList();
    }

    static String causaRaiz(Throwable error) {
        Throwable causa = error;
        while (causa.getCause() != null) {
            causa = causa.getCause();
        }
        return causa.getClass().getSimpleName() + ": " + causa.getMessage();
    }
}
