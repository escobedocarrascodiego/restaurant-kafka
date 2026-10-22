package com.restaurante.cocina;

import java.util.Map;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

// Solo para la pagina index.html (servida por pedidos-service en :8090), por eso permite CORS
@RestController
@CrossOrigin
public class HistorialController {

    private final CocinaListener listener;

    public HistorialController(CocinaListener listener) {
        this.listener = listener;
    }

    @GetMapping("/api/historial")
    public Map<String, Object> historial() {
        return listener.historial();
    }
}
