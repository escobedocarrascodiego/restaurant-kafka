package com.restaurante.cocina;

import java.time.Instant;

// Copia local del evento: cada servicio tiene su propia clase, solo comparten el formato JSON
public record PedidoEvento(long pedidoId, String tipo, int mesa, String plato, Instant fecha) {

    public static final String PEDIDO_CREADO = "PEDIDO_CREADO";
    public static final String PEDIDO_PAGADO = "PEDIDO_PAGADO";
}
