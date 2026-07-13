-- =============================================================================
-- SEED ADMIN USUARIOS — Contraseñas con bcrypt (12 rounds)
--
-- Contraseña tentativa: Altrans2026
-- Cámbiala después de preguntarles a Julio y Julian si quieren dejarla o no.
-- =============================================================================

-- Julio Fuertes
INSERT INTO public.admin_usuarios (wa_from, nombre, password_hash)
VALUES (
    '+573184871084',
    'Julio Fuertes',
    '$2b$12$iMYrpXAFc0dhxfm3hLmqJ.MTWuThPJ5TUfN/hxyBEQNjQSkccB0IW'
);

-- Julian Fuertes
INSERT INTO public.admin_usuarios (wa_from, nombre, password_hash)
VALUES (
    '+573004724887',
    'Julian Fuertes',
    '$2b$12$gXyAgULdn0lfAolhE1WLV.dNvbFxtdRtdN1QEOZ71ENEljqNxJvpK'
);