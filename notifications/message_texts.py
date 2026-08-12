"""
Textos de las plantillas de notificación (mismo contenido que la WABA).

Se usan para el modo de texto libre (WA_SEND_MODE=text): permiten enviar el
contenido real de cada plantilla como mensaje de texto mientras las plantillas
oficiales están pendientes de aprobación en Meta. Al aprobarse, se pasa a
WA_SEND_MODE=template y el contenido lo envía Meta directamente.

Los placeholders {{1}}, {{2}} coinciden con los parámetros posicionales de
las plantillas de la WABA (scripts/crear_plantillas_altrans.py).
"""

TEMPLATE_TEXTS = {
    "saldo_falta_factura": {
        "header": "Pendiente de pago: factura electrónica",
        "body": (
            "Buen día, estimado transportador.\n\n"
            "Le informamos que el pago del manifiesto *{{1}}* no se ha efectuado "
            "porque no se ha legalizado la factura electrónica que debe enviar el "
            "propietario, quien está obligado a hacerlo según el RUT.\n\n"
            "Por favor envíela lo antes posible a facturaelectronica@altrans.com.co. "
            "Si ya la envió correctamente, reenvíela a la persona que contrató su servicio.\n\n"
            "Mensaje automático de ALTRANS."
        ),
    },
    "saldo_falta_documentacion": {
        "header": "Pendiente de pago: documentación",
        "body": (
            "Buen día, estimado transportador.\n\n"
            "Le informamos que el pago del manifiesto *{{1}}* no se ha efectuado "
            "porque no se ha cumplido formalmente con la documentación original firmada "
            "que nos permite evidenciar que el transporte concluyó satisfactoriamente.\n\n"
            "Por favor regularice esta situación según las instrucciones de quien contrató "
            "su servicio. Si envió los documentos por mensajería, rastree y envíe la guía "
            "a la persona que contrató su servicio.\n\n"
            "Mensaje automático de ALTRANS."
        ),
    },
    "saldo_novedad_pendiente": {
        "header": "Pendiente de pago: novedad sin resolver",
        "body": (
            "Buen día, estimado transportador.\n\n"
            "Le informamos que el pago del manifiesto *{{1}}* no se ha efectuado "
            "debido a una novedad sin resolver, que puede ser averías, faltantes "
            "o situaciones similares.\n\n"
            "Por favor comuníquese con la persona que contrató su servicio o "
            "adelante las instrucciones que ella le haya dado.\n\n"
            "Mensaje automático de ALTRANS."
        ),
    },
    "saldo_plazo_vigente": {
        "header": "Pago dentro del plazo pactado",
        "body": (
            "Buen día, estimado transportador.\n\n"
            "Le informamos que el saldo del manifiesto *{{1}}* aún no se ha pagado "
            "porque no se ha completado el plazo pactado para realizarlo.\n\n"
            "Nuestro acuerdo fue pagarlo dentro de los 15 días hábiles siguientes "
            "al completado formal del transporte. Le pedimos una espera hasta "
            "aproximadamente el *{{2}}*.\n\n"
            "Mensaje automático de ALTRANS."
        ),
    },
    "pago_realizado": {
        "header": "Pago realizado",
        "body": (
            "Buen día, estimado transportador.\n\n"
            "Le informamos que el saldo del manifiesto *{{1}}* ha sido pagado "
            "exitosamente el día *{{2}}* mediante transferencia bancaria.\n\n"
            "Por favor revise sus extractos bancarios. Gracias por su servicio.\n\n"
            "Mensaje automático de ALTRANS."
        ),
    },
}


def render_text(template_name: str, *params) -> str:
    """Renderiza el texto de una plantilla sustituyendo {{1}}, {{2}}, ...

    Args:
        template_name: Clave interna (saldo_falta_factura, pago_realizado, ...).
        params: Valores posicionales en orden {{1}}, {{2}}, ...
    """
    cfg = TEMPLATE_TEXTS.get(template_name)
    if cfg is None:
        raise ValueError(f"Plantilla desconocida: {template_name}")

    body = cfg["body"]
    for i, value in enumerate(params, start=1):
        body = body.replace(f"{{{{{i}}}}}", str(value))

    header = cfg.get("header")
    if header:
        return f"*{header}*\n\n{body}"
    return body
