from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd


def save_dataframe_to_xlsx(df: pd.DataFrame, complex_name: str, output_dir: str = "./output") -> Path:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    safe_name = "".join(ch for ch in complex_name if ch not in '\\/:*?"<>|\r\n\t').strip() or "complex"
    today = date.today().strftime("%Y%m%d")
    out_path = target_dir / f"{safe_name}_{today}.xlsx"
    df.to_excel(out_path, index=False)
    return out_path


def save_dataframes_to_xlsx(
    frames: dict[str, pd.DataFrame],
    file_stem: str,
    output_dir: str = "./output",
) -> Path:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    safe_name = "".join(ch for ch in file_stem if ch not in '\\/:*?"<>|\r\n\t').strip() or "result"
    today = date.today().strftime("%Y%m%d")
    out_path = target_dir / f"{safe_name}_{today}.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for sheet_name, df in frames.items():
            safe_sheet = sheet_name[:31] or "Sheet1"
            df.to_excel(writer, sheet_name=safe_sheet, index=False)
    return out_path
