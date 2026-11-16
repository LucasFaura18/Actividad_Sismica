-- Base lista para los cinco servicios.
-- El modelo de eventos, versiones y cuarentena se añade en el punto de modelado.
CREATE TABLE IF NOT EXISTS ingesta_estado (
    clave text PRIMARY KEY,
    valor text NOT NULL,
    actualizado_en timestamptz NOT NULL DEFAULT now()
);
