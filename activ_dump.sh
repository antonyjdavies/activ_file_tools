awk -F \| '{ if ($1==86 && $2!="86") print FNR "|" $0; else print $0;}' | ~/dump_activ.py
