"""Rewrite <src>/trip into <dst>/trip as N files of one row group each (snappy, row order kept); link zone."""
import os, sys, pyarrow.dataset as ds, pyarrow.parquet as pq
src, dst, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
t = ds.dataset(f"{src}/trip", format="parquet").to_table()
os.makedirs(f"{dst}/trip", exist_ok=True)
step = -(-t.num_rows // n)
for i in range(n):
    part = t.slice(i * step, step)
    pq.write_table(part, f"{dst}/trip/part-{i:03d}.parquet", compression="snappy", row_group_size=part.num_rows)
if not os.path.exists(f"{dst}/zone"):
    os.symlink(os.path.abspath(f"{src}/zone"), f"{dst}/zone")
print(dst, n, "files,", t.num_rows, "rows")
