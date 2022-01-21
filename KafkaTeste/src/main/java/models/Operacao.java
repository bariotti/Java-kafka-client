package models;

import java.math.BigDecimal;

public class Operacao {

    private final int id;
    private final BigDecimal valor;
    private final String referencia;

    public Operacao(int id, BigDecimal valor, String referencia) {
        this.id = id;
        this.valor = valor;
        this.referencia = referencia;
    }

    public int getId() {
        return id;
    }

    public BigDecimal getValor() {
        return valor;
    }

    public String getReferencia() {
        return referencia;
    }
}
