# Plantillas de WhatsApp — Altrans Bot

## Convenciones (para subir a Meta)

- **Categoría:** Utility (utilidad) — todas. No usar lenguaje promocional o Meta las reclasifica a Marketing.
- **Idioma:** Español (es).
- **Variables:** `{{1}}` = número de manifiesto (todas); `{{2}}` = fecha (solo `saldo_plazo_vigente`) o monto (`pago_realizado`); `{{3}}` = fecha de pago (`pago_realizado`).
- **Footer:** el descargo de responsabilidad va en el Footer (texto gris, máx. 60 caracteres), igual en las 5.
- **Sin links, sin botones** (no hacen falta y reducen riesgo de rechazo).

**Footer común (las 5 plantillas):**
> Mensaje automático de ALTRANS. Puede contener errores.

> [!NOTE]
> El descargo original era más largo ("...no representa una comunicación oficial..."). El Footer de Meta solo admite 60 caracteres, por eso se resumió. Si Altrans quiere el texto completo, va al final del Body en lugar del Footer.

---

## 1. `saldo_falta_factura`
**Motivo:** el saldo no se ha pagado porque falta la factura electrónica que debe emitir el propietario.

**Body:**
```
Buen día, estimado transportador.

Le informo que el manifiesto {{1}} no se ha pagado porque no se ha legalizado mediante la factura electrónica que debe enviar el propietario, quien está obligado a hacerlo según el RUT. Por favor enviarla lo antes posible a facturaelectronica@altrans.com.co.

En caso de haberla enviado correctamente, por favor reenviarla a la persona que contrató su servicio.
```
**Footer:** `Mensaje automático de ALTRANS. Puede contener errores.`

**Ejemplo con datos:** manifiesto `{{1}}` = 33429.

---

## 2. `saldo_falta_documentacion`
**Motivo:** el saldo no se ha pagado porque no se ha cumplido formalmente con la documentación original firmada.

**Body:**
```
Buen día, estimado transportador.

Le informo que el manifiesto {{1}} no se ha pagado porque no se ha cumplido formalmente con la documentación original firmada que nos permite evidenciar que el transporte concluyó satisfactoriamente. Por favor regularizar esta situación según las instrucciones de quien contrató su servicio.

En caso de haber enviado los documentos por una empresa de mensajería, rastree y envíe la guía a la persona que contrató su servicio.
```
**Footer:** `Mensaje automático de ALTRANS. Puede contener errores.`

**Ejemplo con datos:** manifiesto `{{1}}` = 33201.

---

## 3. `saldo_novedad_pendiente`
**Motivo:** el saldo no se ha pagado porque hay una novedad sin resolver (averías, faltantes, etc.).

**Body:**
```
Buen día, estimado transportador.

Le informo que el saldo del manifiesto {{1}} no se ha pagado debido a una novedad sin resolver, que puede ser averías, faltantes o situaciones similares. Por favor comunicarse con la persona que contrató su servicio o adelantar las instrucciones dadas por ella.
```
**Footer:** `Mensaje automático de ALTRANS. Puede contener errores.`

**Ejemplo con datos:** manifiesto `{{1}}` = 32989.

---

## 4. `saldo_plazo_vigente`
**Motivo:** el saldo aún no se paga porque el plazo acordado (15 días hábiles desde el completado) todavía está corriendo. Se le da una fecha estimada.

**Body:**
```
Buen día, estimado transportador.

Le informo que el saldo del manifiesto {{1}} aún no se ha pagado porque no se ha completado el plazo pactado para realizarlo. Nuestro acuerdo fue pagarlo dentro de los 15 días hábiles siguientes al completado formal del transporte.

Le pedimos amablemente una espera hasta aproximadamente el {{2}}.
```
**Footer:** `Mensaje automático de ALTRANS. Puede contener errores.`

**Ejemplo con datos:** manifiesto `{{1}}` = 33429, fecha `{{2}}` = "21 de junio de 2026".

> [!NOTE]
> El mensaje original tenía 3 variables seguidas (`día XXXX del mes XXXX del año XXXX`). Aquí se unificaron en UNA sola variable de fecha `{{2}}` (ej: "21 de junio de 2026") para evitar errores al rellenar.

---

## 5. `pago_realizado` (NUEVA)
**Motivo:** el pago del manifiesto fue registrado exitosamente. Se informa al conductor el monto, la fecha y el medio de pago.

**Body:**
```
Buen día, estimado transportador.

Le informamos que el saldo del manifiesto {{1}} por un monto de ${{2}} ha sido pagado exitosamente el día {{3}} mediante transferencia bancaria. Por favor revise sus extractos bancarios.

Gracias por su servicio.
```
**Footer:** `Mensaje automático de ALTRANS. Puede contener errores.`

**Variables:** `{{1}}` = manifiesto, `{{2}}` = monto (ej: 1.500.000), `{{3}}` = fecha de pago (ej: "15 de julio de 2026").

---

## Cambios respecto a los mensajes originales

1. **Ortografía y tildes corregidas** ("día", "factura electrónica", "automático", "documentación", "mensajería").
2. **Descargo en el Footer de las 5** (el #3 no lo tenía → ya incluido).
3. **`saldo_plazo_vigente`:** las 3 variables de fecha se unificaron en una sola (`{{2}}`).
4. **`XXXX` → variables numeradas de Meta** (`{{1}}` manifiesto, `{{2}}` fecha).
5. **`pago_realizado` (nueva):** 3 variables (`{{1}}` manifiesto, `{{2}}` monto, `{{3}}` fecha de pago).

