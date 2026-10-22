package com.restaurante.pedidos;

import java.time.Instant;

// El "mensaje" que viaja por Kafka. Se envia como JSON en el value.
public record PedidoEvento(long pedidoId, String tipo, int mesa, String plato, Instant fecha) {

    public static final String PEDIDO_CREADO = "PEDIDO_CREADO";
    public static final String PEDIDO_PAGADO = "PEDIDO_PAGADO";
}
