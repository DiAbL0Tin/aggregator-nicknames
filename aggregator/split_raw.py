"""
Module pour fractionner les fichiers bruts en plusieurs fichiers txt de taille limitée.
"""

from pathlib import Path
from rich.console import Console


def split_raw_files(
    input_dir: Path,
    output_dir: Path,
    max_lines: int = 5_000_000,
    console: Console = None
) -> None:
    """
    Scinde tous les fichiers .txt, .csv, .tsv de input_dir en fichiers txt
    de taille max max_lines lignes, dans output_dir.
    """
    # Créer le dossier de sortie
    output_dir.mkdir(parents=True, exist_ok=True)
    file_index = 1
    line_count = 0
    # Ouvrir le premier fichier de sortie
    out_path = output_dir / f"chunk_{file_index:03d}.txt"
    f_out = out_path.open("w", encoding="utf-8")

    for pattern in ("*.txt", "*.csv", "*.tsv"):
        for infile in input_dir.rglob(pattern):
            if console:
                console.print(f"Traitement de {infile}…")
            with infile.open("r", encoding="utf-8", errors="ignore") as f_in:
                for line in f_in:
                    f_out.write(line)
                    line_count += 1
                    if line_count >= max_lines:
                        f_out.close()
                        file_index += 1
                        line_count = 0
                        out_path = output_dir / f"chunk_{file_index:03d}.txt"
                        f_out = out_path.open("w", encoding="utf-8")

    # Fermer le dernier fichier
    f_out.close()
    if console:
        console.print(f"✓ Fichiers créés dans {output_dir}, total chunks: {file_index}")
