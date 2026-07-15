# Diccionario de datos y reglas de calidad — Capa Silver

Este documento define las reglas de limpieza y validacion aplicadas al
transformar datos de Bronze a Silver. La filosofia general del proyecto:
**no se descartan datos silenciosamente**. Cuando un registro tiene un
problema, se marca con un flag de calidad en vez de eliminarse, para
mantener trazabilidad y permitir que capas posteriores decidan si
incluirlo o no segun el analisis que se necesite.

## Tabla: citas

| Columna | Regla | Accion si falla | Flag |
|---|---|---|---|
| fecha_hora | Debe ser una fecha valida, no nula | Se conserva la fila, se marca | `missing_fecha_hora` |
| tiempo_espera_minutos | Debe ser >= 0 | Se corrige a 0 si es negativo, se marca | `tiempo_espera_corregido` |
| id_paciente | Debe existir en dim_paciente (Gold) | Se conserva, se valida en Gold | — |
| id_doctor | Debe existir en dim_doctor (Gold) | Se conserva, se valida en Gold | — |
| duplicados | id_cita no debe repetirse | Se elimina el duplicado, se conserva el primero | `duplicado_eliminado` (solo en log, no en fila ya que se elimina) |

## Tabla: pacientes

| Columna | Regla | Accion si falla | Flag |
|---|---|---|---|
| email | Debe tener formato valido (usuario@dominio) | Se conserva, se marca | `email_invalido` |
| fecha_nacimiento | Debe ser anterior a hoy y posterior a 1900 | Se conserva, se marca | `fecha_nacimiento_invalida` |
| duplicados | id_paciente no debe repetirse | Se elimina el duplicado, se conserva el primero | — |

## Tabla: doctores

| Columna | Regla | Accion si falla | Flag |
|---|---|---|---|
| anios_experiencia | Debe estar entre 0 y 60 | Se conserva, se marca | `experiencia_fuera_de_rango` |
| duplicados | id_doctor no debe repetirse | Se elimina el duplicado | — |

## Tabla: resultados_lab

| Columna | Regla | Accion si falla | Flag |
|---|---|---|---|
| id_cita | Debe existir en la tabla citas | Se conserva, se valida en Gold | — |
| resultado | Debe ser numerico y positivo | Se conserva, se marca | `resultado_fuera_de_rango` |

## Columna estandar: data_quality_flag

Cada tabla en Silver incluye una columna `data_quality_flag` de tipo
array de strings. Si el registro no tiene ningun problema, el array
esta vacio. Si tiene multiples problemas, se acumulan todos los flags
correspondientes (ej. `["missing_fecha_hora", "tiempo_espera_corregido"]`).

Esto permite en Gold o en el dashboard filtrar facilmente:
- Analisis "solo con datos limpios": `WHERE size(data_quality_flag) = 0`
- Auditoria de calidad de datos: `WHERE size(data_quality_flag) > 0`

## Por que esta decision (flag vs. descartar)

Se opto por marcar en vez de descartar porque:
1. Descartar es una decision de negocio, no tecnica — un ingeniero de
   datos no deberia decidir unilateralmente que informacion "no importa"
2. Permite trazabilidad completa: se puede reportar exactamente cuantos
   y cuales registros tienen problemas de captura en el sistema origen
3. Preserva informacion util incluso en filas con problemas parciales
   (ej. una cita sin fecha valida aun aporta al analisis de ingresos)

La unica excepcion son los duplicados exactos por llave primaria, que
si se eliminan, porque representan un error de extraccion (no un dato
de negocio incompleto) y conservarlos duplicaria las metricas.
