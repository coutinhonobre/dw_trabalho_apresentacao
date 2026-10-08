

INSERT INTO acidentes (id, data, horario, local_id, veiculos, causa_acidente, tipo_acidente, sentido_via)
VALUES (
    (SELECT MAX(id) + 1 FROM acidentes),
    '2026-07-31', '08:15:00', NULL, 2,
    'Acessar a via sem observar a presença dos outros veículos',
    'Atropelamento de Pedestre',
    'Crescente'
);
