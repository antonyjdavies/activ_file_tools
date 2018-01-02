#!/usr/bin/python
import sys
import re
import activ_events
import activ_flds
import activ_tables

#message-type "|" version "|" timestamp "|" update-type "|" table-number "|" update-id "|" permission-id "|" event-type "|" symbol "|" flags "|" *([relationship-id ":"] field-id "=" field-value "|")
line_pattern = re.compile(r"^(?P<fileseq>[0-9]+)\|(?P<msg_type>[0-9]+)\|(?P<msg_ver>[0-9]+)\|(?P<time>[0-9:\.]+)\|(?P<operation>[AUD])\|(?P<table_num>[0-9]+)\|(?P<c4>.*?)\|(?P<c5>[0-9]*)\|(?P<event_type>[0-9]*)\|(?P<symbol>[A-Z0-9a-z\\.\\/=]+?)\|(?P<c7>.*?)\|(?P<kvpairs>.*?)$")

def parseKVData(data):
    acc = {}
    pairs = data.split('|')
    for pair in pairs:
        if pair != '' and pair is not None:
            key, value = pair.split("=")
            if key in activKeyMappings:
                acc[activKeyMappings[key]] = value
            else:
                acc[key] = value

    return acc

for line in sys.stdin:
    m = line_pattern.match(line)
    if m is not None:
        parsed = m.groupdict()
        kvdata = parsed['kvpairs']
        kvs = ''
        pairs = kvdata.split('|')
        for pair in pairs:
            if pair != '' and pairs is not None:
                rel_key, value = pair.split("=")
                if (':' in rel_key):
                    relationship, key = rel_key.split(':')
                else:
                    key = rel_key
                    relationship = None
                kvs = kvs + activ_flds.activ_flds[int(key)] + '=' + value + ','
        # LSE can pad out 2 character symbols to 3 adding '.'
        if (parsed['symbol'].count('.') == 1):
            parsed['sym'], parsed['venue'] = parsed['symbol'].split(".")
        else:
            parsed['sym'], dot, parsed['venue'] = parsed['symbol'].split(".")
            parsed['sym'] = parsed['sym'] + '.'
        print parsed['fileseq'], activ_events.activ_events[int(parsed['event_type'])], activ_tables.activ_tables[parsed['table_num']], parsed['symbol'], parsed['time'], parsed['operation'], kvs
