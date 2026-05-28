"""
Test del backup en producción (el que se envía por email los domingos).

Genera el ZIP en memoria llamando al mismo código que corre en producción
y compara los conteos del backup contra la DB en vivo. NO envía email.

Uso:
    python -m notifications.test_backup_consistency
    make verify-backup-email

Exit code:
    0 → backup íntegro (todas las tablas coinciden con DB en vivo)
    1 → hay discrepancias (revisar output)
"""
import os
import sys

from backup_email import _build_zip, verify_consistency
from logging_config import setup_logging


def main() -> int:
    setup_logging(os.getenv("LOG_LEVEL", "WARNING"))

    print("Generando backup (mismo código que corre en producción)…")
    zip_bytes, counts = _build_zip()
    print(f"  ZIP generado: {len(zip_bytes) // 1024} KB")
    print()

    print("Verificando consistencia contra DB en vivo…")
    consistency = verify_consistency(counts)
    print()

    width = max(len(t) for t in consistency)
    all_ok = True
    for table, v in consistency.items():
        if v["ok"]:
            print(f"  ✅ {table.ljust(width)}  backup={v['backup']:>7,}  live={v['live']:>7,}")
        else:
            all_ok = False
            print(f"  ❌ {table.ljust(width)}  backup={v['backup']:>7,}  live={v['live']:>7,}  — MISMATCH")

    print()
    if all_ok:
        print("✅ Backup íntegro — los conteos del ZIP coinciden con la DB en vivo")
        return 0
    print("❌ Hay discrepancias entre el ZIP y la DB en vivo")
    return 1


if __name__ == "__main__":
    sys.exit(main())
