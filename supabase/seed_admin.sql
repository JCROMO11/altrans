-- =============================================================================
-- SEED ADMIN USUARIOS — Contraseñas con bcrypt (12 rounds)
--
-- Antes de ejecutar, reemplaza los números de WhatsApp reales.
-- Contraseña tentativa: Altrans2026
-- Cámbiala después de preguntarles a Julio y Julian si quieren dejarla o no.
-- =============================================================================

-- Julio Fuertes
INSERT INTO public.admin_usuarios (wa_from, nombre, password_hash)
VALUES (
    '+57XXXXXXXXXX',           -- ← REEMPLAZAR con el número real de Julio
    'Julio Fuertes',
    '$2b$12$iMYrpXAFc0dhxfm3hLmqJ.MTWuThPJ5TUfN/hxyBEQNjQSkccB0IW'
);

-- Julian Fuertes
INSERT INTO public.admin_usuarios (wa_from, nombre, password_hash)
VALUES (
    '+57XXXXXXXXXX',           -- ← REEMPLAZAR con el número real de Julian
    'Julian Fuertes',
    '$2b$12$gXyAgULdn0lfAolhE1WLV.dNvbFxtdRtdN1QEOZ71ENEljqNxJvpK'
);