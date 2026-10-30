import csv
import io
import pathlib
import sys
out=pathlib.Path(sys.argv[1])
rows=[]
for repl in ('off','on'):
    for batch in (1,100):
        key=f'{repl}-{batch}'
        text=(out/f'murmur-{key}.txt').read_text()
        header=next(csv.reader([next(line for line in text.splitlines() if line.startswith('BENCH_HEADER,'))]))[1:]
        values=next(csv.reader([next(line for line in text.splitlines() if line.startswith('BENCH_CSV,'))]))[1:]
        murmur=dict(zip(header, values))
        hctree=dict(line.split(',',1) for line in (out/f'hctree-{key}.txt').read_text().splitlines() if ',' in line)
        a=float(murmur['ops_per_second'])
        b=float(hctree['ops_per_second'])
        for name,metric,count,ms in (('murmur',a,murmur['writes'],murmur['mean_write_transaction_ms']),('hctree',b,hctree['writes'],hctree['mean_write_transaction_ms'])):
            rows.append((repl,batch,name,metric,count,ms))
        print(f'BENCH_PAIR,{repl},{batch},{a:.2f},{b:.2f},{a/b:.3f}')
with (out/'summary.csv').open('w',newline='') as f:
    w=csv.writer(f)
    w.writerow(['replication','batch_size','backend','ops_per_second','writes','mean_transaction_ms'])
    w.writerows(rows)
print('BENCHMARK_COMPLETE')
