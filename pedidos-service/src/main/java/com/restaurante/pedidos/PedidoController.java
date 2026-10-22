package com.restaurante.pedidos;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/pedidos")
public class PedidoController {

    record CrearPedidoRequest(int mesa, String plato) {}

    private static final List<String> PLATOS =
            List.of("Lomo saltado", "Ceviche", "Aji de gallina", "Arroz chaufa", "Causa limeña", "Pollo a la brasa");

    private final PedidoPublisher publisher;
    private final AtomicLong secuencia = new AtomicLong();
    // "Base de datos" en memoria: solo para saber mesa y plato al pagar
    private final Map<Long, CrearPedidoRequest> pedidos = new ConcurrentHashMap<>();
    private final Set<Long> pagados = ConcurrentHashMap.newKeySet();

    public PedidoController(PedidoPublisher publisher) {
        this.publisher = publisher;
    }

    // POST /api/pedidos  {"mesa": 5, "plato": "Ceviche"}
    @PostMapping
    public ResponseEntity<Map<String, Object>> crear(@RequestBody CrearPedidoRequest request) {
        long id = secuencia.incrementAndGet();
        pedidos.put(id, request);
        return enviarYEsperar(new PedidoEvento(id, PedidoEvento.PEDIDO_CREADO, request.mesa(), request.plato(), Instant.now()));
    }

    // POST /api/pedidos/{id}/pagar  -> mismo pedido = misma key = misma particion
    @PostMapping("/{id}/pagar")
    public ResponseEntity<Map<String, Object>> pagar(@PathVariable long id) {
        CrearPedidoRequest pedido = pedidos.get(id);
        if (pedido == null) {
            return ResponseEntity.status(404).body(Map.of("error", "No existe el pedido #" + id));
        }
        var respuesta = enviarYEsperar(new PedidoEvento(id, PedidoEvento.PEDIDO_PAGADO, pedido.mesa(), pedido.plato(), Instant.now()));
        if (respuesta.getStatusCode().is2xxSuccessful()) {
            pagados.add(id);
        }
        return respuesta;
    }

    // GET /api/pedidos -> ultimos 15 pedidos (para la pagina index.html)
    @GetMapping
    public List<Map<String, Object>> listar() {
        return pedidos.entrySet().stream()
                .sorted(Map.Entry.<Long, CrearPedidoRequest>comparingByKey().reversed())
                .limit(15)
                .map(e -> Map.<String, Object>of("id", e.getKey(), "mesa", e.getValue().mesa(),
                        "plato", e.getValue().plato(), "pagado", pagados.contains(e.getKey())))
                .toList();
    }

    // POST /api/pedidos/rafaga?cantidad=20  -> muchos pedidos de golpe (sin esperar respuesta de cada uno)
    @PostMapping("/rafaga")
    public Map<String, Object> rafaga(@RequestParam(defaultValue = "20") int cantidad) {
        long primero = secuencia.get() + 1;
        for (int i = 0; i < cantidad; i++) {
            long id = secuencia.incrementAndGet();
            var request = new CrearPedidoRequest(
                    ThreadLocalRandom.current().nextInt(1, 21),
                    PLATOS.get(ThreadLocalRandom.current().nextInt(PLATOS.size())));
            pedidos.put(id, request);
            publisher.publicar(new PedidoEvento(id, PedidoEvento.PEDIDO_CREADO, request.mesa(), request.plato(), Instant.now()));
        }
        return Map.of("enviados", cantidad, "desde", primero, "hasta", secuencia.get());
    }

    // Espera la confirmacion del broker para mostrar particion/offset (o el error) en la respuesta HTTP
    private ResponseEntity<Map<String, Object>> enviarYEsperar(PedidoEvento evento) {
        try {
            var meta = publisher.publicar(evento).join().getRecordMetadata();
            return ResponseEntity.ok(Map.of(
                    "pedidoId", evento.pedidoId(), "tipo", evento.tipo(),
                    "particion", meta.partition(), "offset", meta.offset()));
        } catch (Exception e) {
            return ResponseEntity.status(503).body(Map.of(
                    "pedidoId", evento.pedidoId(), "tipo", evento.tipo(),
                    "error", PedidoPublisher.causaRaiz(e)));
        }
    }
}
