#!/usr/bin/env python3
"""Sample script to imitate work."""
import sys
import time


def read_fasta(in_f):
    """Read fasta file.

    Not polished version, just for test."""
    f = open(in_f, "r")
    content = f.read().split(">")
    f.close()
    del content[0]
    name_to_seq = {}
    for elem in content:
        lines = elem.split("\n")
        header = lines[0]
        seq = "".join(lines[1:])
        name_to_seq[header] = seq
    return name_to_seq


def modify_seq(sequence):
    """Perform something time-consuming."""
    out = b""  # string concatenation if pretty heavy, fine
    for c in sequence:
        b = c.encode()
        c_num = ord(c)
        for i in range(255):
            if i == c_num:
                out = b + out
        out = out[::-1]
        time.sleep(0.01)
    return out.decode("utf-8")


def do_something_on_sequences(seqs):
    """Imitate some sequences analysis."""
    out_seqs = {}
    for k, v in seqs.items():
        out_seqs[k] = modify_seq(v)
    return out_seqs


def save_fasta(seq, out_f):
    """Save fasta file."""
    f = open(out_f, "w") if out_file != "stdout" else sys.stdout
    for k, v in seq.items():
        f.write(f">{k}\n{v}\n")
    f.close() if out_file != "stdout" else None


if __name__ == "__main__":
    try:
        in_file = sys.argv[1]
        out_file = sys.argv[2]
    except IndexError:
        sys.stderr.write(f"Usage: {sys.argv[0]} [in_fasta] [out_fasta]\n")
        sys.exit(0)

    sequences = read_fasta(in_file)
    out_sequences = do_something_on_sequences(sequences)
    save_fasta(out_sequences, out_file)
