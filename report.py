"""
report.py — export a harness prediction answer to prediction.csv + prediction.md.
Usage: python harness.py -q "predict the next 24 h" > answer.txt
       python report.py answer.txt [outdir]      (also reads stdin if no file)
The harness prompt fixes the columns: horizon,time,area,metric,value,unit,low,high,confidence,drivers
"""

import csv, io, pathlib, re, sys

COLS = ("horizon", "time", "area", "metric", "value", "unit",
        "low", "high", "confidence", "drivers")


def _table(rows):
    """One csv row list → a markdown table."""
    line = lambda r: "| " + " | ".join(c.strip() for c in r) + " |"
    return "\n".join([line(rows[0]), "|" + "---|" * len(rows[0])] + [line(r) for r in rows[1:]])


def write_reports(answer: str, outdir="reports"):
    """Extract the fenced ```csv block(s) → prediction.csv and prediction.md. Returns (csv, md)."""
    blocks = re.findall(r"```csv[ \t]*\n(.*?)```", answer, re.S)
    rows, md = [], answer
    for b in blocks:
        r = [x for x in csv.reader(io.StringIO(b.strip())) if x and any(c.strip() for c in x)]
        if r and [c.strip().lower() for c in r[0]] != list(COLS):
            raise ValueError(f"csv block header {r[0]} != {list(COLS)}")
        rows += r[1:]                                # validated header, append the data rows
        md = md.replace(f"```csv\n{b}```", _table(r))
    out = pathlib.Path(outdir)
    out.mkdir(parents=True, exist_ok=True)
    with (out / "prediction.csv").open("w", newline="") as f:
        csv.writer(f).writerows([list(COLS)] + rows)
    (out / "prediction.md").write_text(md)
    return out / "prediction.csv", out / "prediction.md"


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    answer = pathlib.Path(args[0]).read_text() if args else sys.stdin.read()
    c, m = write_reports(answer, args[1] if len(args) > 1 else "reports")
    print(f"wrote {c} and {m}")
