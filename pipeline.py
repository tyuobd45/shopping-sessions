#!/usr/bin/env python3
"""Shopping Sessions: exact LF CSV parsing, identity linking, sessions and ads.
Uses DATA_DIR, OUTPUT_DIR, SCRATCH_DIR; optional positional DATA OUTPUT paths.
No fixture answers, fixed dates, users, campaigns or seeds are used.
"""
from __future__ import annotations
import csv
import gc
import json
import mmap
import os
from pathlib import Path
import re
import sys
import tempfile
import time
import duckdb


def literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def raw_rows(folder: Path, columns: list[str]) -> str:
    """Validate whole lines before unquoting; preserve CR, spaces and case."""
    files = sorted(folder.glob('*.csv'))
    if not files:
        return 'SELECT ' + ', '.join(f'NULL::VARCHAR AS {c}' for c in columns) + ' WHERE FALSE'
    # DuckDB strips CR at line endings. Only use its faster CSV reader for
    # files proven to contain no CR; retain the exact LF reader for the rest.
    fast_files, exact_files = [], []
    for path in files:
        if path.stat().st_size:
            with path.open('rb') as stream:
                with mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ) as contents:
                    has_cr = contents.find(b'\r') != -1
        else:
            has_cr = True
        (exact_files if has_cr else fast_files).append(path)
    queries = []
    if fast_files:
        paths = '[' + ','.join(literal(p) for p in fast_files) + ']'
        types = ','.join(f"{literal(c)}:'VARCHAR'" for c in columns)
        select = ','.join(f"nullif(trim({c}, '\"'), '') AS {c}" for c in columns)
        field_pattern = literal('[^\"]*|\"[^\"]*\"')
        valid = ' AND '.join(f'({c} IS NULL OR regexp_full_match({c},{field_pattern}))'
                             for c in columns)
        queries.append(f"""SELECT {select} FROM read_csv(
            {paths},header=true,auto_detect=false,columns={{{types}}},
            quote='',escape='',new_line='\\n',nullstr='',
            strict_mode=true,ignore_errors=true,null_padding=false)
            WHERE {valid}""")
    if not exact_files:
        return queries[0]
    paths = '[' + ','.join(literal(p) for p in exact_files) + ']'
    field = '("[^",]*"|[^",]*)'
    pattern = field + (',' + field) * (len(columns)-1)
    select = ', '.join(f"nullif(trim(parts[{i}], '\"'), '') AS {c}"
                       for i,c in enumerate(columns,1))
    queries.append(f"""
        SELECT {select} FROM (
            SELECT string_split(line, ',') AS parts FROM (
                SELECT unnest(string_split(
                    CASE WHEN strpos(r.content,chr(10))>0
                         THEN substr(r.content,strpos(r.content,chr(10))+1)
                         ELSE '' END,chr(10))) AS line
                FROM read_text({paths}) r
            ) lines WHERE regexp_full_match(line,{literal(pattern)})
        ) parsed
    """)
    return ' UNION ALL '.join('('+q+')' for q in queries)


def linked_users(con, data: Path, work: Path) -> None:
    """Union by size, path compression, and the minimum ID in each component."""
    parent: dict[str,str] = {}
    sizes: dict[str,int] = {}
    smallest: dict[str,str] = {}
    valid = re.compile(rb'(?:"[^",]*"|[^",]*)\Z')
    def find(value):
        if value not in parent:
            parent[value]=value
            sizes[value]=1
            smallest[value]=value
            return value
        while parent[value]!=value:
            parent[value]=parent[parent[value]]
            value=parent[value]
        return value
    for path in sorted((data/'links').glob('*.csv')):
        with path.open('rb') as stream:
            next(stream,None)
            for line in stream:
                if line.endswith(b'\n'): line=line[:-1]
                fields=line.split(b',')
                if len(fields)!=2 or not all(valid.fullmatch(x) for x in fields): continue
                a,b=(x.strip(b'"').decode('utf-8') for x in fields)
                if not a or not b: continue
                ra,rb=find(a),find(b)
                if ra==rb: continue
                if sizes[ra]<sizes[rb]: ra,rb=rb,ra
                parent[rb]=ra
                sizes[ra]+=sizes.pop(rb)
                smallest[ra]=min(smallest[ra],smallest.pop(rb))
    mapping=work/'person_map.csv'
    with mapping.open('w',encoding='utf-8',newline='') as stream:
        writer=csv.writer(stream,quoting=csv.QUOTE_ALL,lineterminator='\n')
        writer.writerow(['original_id','person_id'])
        for user in parent: writer.writerow([user,smallest[find(user)]])
    con.execute(f"""CREATE TABLE people AS SELECT * FROM read_csv(
        {literal(mapping)},header=true,auto_detect=false,
        columns={{'original_id':'VARCHAR','person_id':'VARCHAR'}})""")


def main():
    started=time.perf_counter()
    data=Path(os.environ.get('DATA_DIR',sys.argv[1] if len(sys.argv)>1 else '/data'))
    output=Path(os.environ.get('OUTPUT_DIR',sys.argv[2] if len(sys.argv)>2 else '/output'))
    scratch=os.environ.get('SCRATCH_DIR')
    if scratch: Path(scratch).mkdir(parents=True,exist_ok=True)
    cpus=max(1,int(os.environ.get('NCPU',min(8,os.cpu_count() or 1))))
    memory=max(1,int(os.environ.get('MEM_GB','8')))
    def log(message):
        print(f'[{time.perf_counter()-started:.2f}s] {message}',file=sys.stderr,flush=True)
    with tempfile.TemporaryDirectory(prefix='shopping-',dir=scratch) as temporary:
        work=Path(temporary)
        con=duckdb.connect()
        con.execute(f'SET threads={cpus}')
        con.execute(f"SET memory_limit={literal(str(max(1,memory*3//4))+'GB')}")
        con.execute(f"SET temp_directory={literal(work/'spill')}")
        con.execute('SET preserve_insertion_order=false')
        con.execute("SET TimeZone='UTC'")
        manifest=json.loads((data/'manifest.json').read_text())
        lower,upper=con.execute("SELECT epoch_ms(strptime(?,'%Y-%m-%dT%H:%M:%SZ')),"
                               "epoch_ms(strptime(?,'%Y-%m-%dT%H:%M:%SZ'))",
                               [manifest['window_start'],manifest['window_end']]).fetchone()
        week=604800000
        base=r'[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]'
        con.execute(f"""CREATE MACRO parse_ms(s) AS CASE
          WHEN regexp_full_match(s,'[0-9]+') THEN try_cast(s AS BIGINT)
          WHEN regexp_full_match(s,{literal(base+'Z')})
            THEN epoch_ms(try_strptime(s,'%Y-%m-%dT%H:%M:%SZ'))
          WHEN regexp_full_match(s,{literal(base+r'[+-]([01][0-9]|2[0-3]):[0-5][0-9]')})
            THEN epoch_ms(try_strptime(substr(s,1,19),'%Y-%m-%dT%H:%M:%S'))
              - CASE WHEN substr(s,20,1)='+' THEN 1 ELSE -1 END
              * (try_cast(substr(s,21,2) AS BIGINT)*60+try_cast(substr(s,24,2) AS BIGINT))*60000
          ELSE NULL END""")
        linked_users(con,data,work)
        gc.collect()
        log('linked IDs mapped')
        for table,columns in [('products',['product_id','category']),('campaigns',['campaign_id','channel'])]:
            names=','.join(f"{literal(c)}:'VARCHAR'" for c in columns)
            con.execute(f'CREATE TABLE {table} AS SELECT * FROM read_csv('
                        f"{literal(data/(table+'.csv'))},header=true,columns={{{names}}})")
        raw=raw_rows(data/'events',['event_id','user_id','ts','ingest_ts','event_type','product_id','price_cents'])
        # No semantic filtering before arg_min: an invalid earliest copy must
        # beat a later valid copy, then be dropped by the next statement.
        con.execute(f"""CREATE TABLE kept AS
          SELECT arg_min(struct_pack(
            user_id:=user_id, ts:=parse_ms(ts),
            ingest_ms:=epoch_ms(try_strptime(ingest_ts,'%Y-%m-%dT%H:%M:%SZ')),
            kind:=CASE event_type WHEN 'view' THEN 1 WHEN 'cart' THEN 2 WHEN 'purchase' THEN 3 END,
            product_id:=product_id,
            price:=CASE WHEN event_type='purchase' AND regexp_full_match(price_cents,'[0-9]+')
                        THEN try_cast(price_cents AS BIGINT) ELSE NULL END
          ),ingest_ts) AS v FROM ({raw}) parsed
          WHERE event_id IS NOT NULL GROUP BY event_id""")
        con.execute(f"""CREATE TABLE events AS
          SELECT coalesce(p.person_id,v.user_id) AS user_id,v.ts AS ts,
            CASE WHEN v.kind=3 THEN v.price ELSE 0 END::BIGINT AS price,
            coalesce(pr.category,'unknown') AS category
          FROM kept k LEFT JOIN people p ON k.v.user_id=p.original_id
          LEFT JOIN products pr ON k.v.product_id=pr.product_id
          WHERE v.user_id IS NOT NULL AND v.ts>={lower} AND v.ts<{upper}
            AND v.kind IS NOT NULL AND v.product_id IS NOT NULL
            AND (v.kind<>3 OR v.price BETWEEN 1 AND 10000000)
            AND v.ingest_ms-v.ts<=86400000""")
        con.execute('DROP TABLE kept')
        log('events parsed, deduplicated and validated')
        con.execute("""CREATE TABLE annotated AS
          WITH gaps AS (
            SELECT *,ts-lag(ts) OVER(PARTITION BY user_id ORDER BY ts) AS gap FROM events
          ), starts AS (
            SELECT user_id,price,category,
              max(CASE WHEN gap IS NULL OR gap>1800000 THEN ts END)
              OVER(PARTITION BY user_id ORDER BY ts
                   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS start_ms FROM gaps
          ) SELECT *,CAST(epoch_ms(start_ms) AS DATE) AS day FROM starts""")
        con.execute('DROP TABLE events')
        con.execute("""CREATE TABLE sessions AS
          SELECT user_id,start_ms,day,count(*)::BIGINT AS events,sum(price)::BIGINT AS revenue_cents
          FROM annotated GROUP BY user_id,start_ms,day""")
        con.execute("""CREATE TABLE top_categories AS
          SELECT user_id,day,category AS top_category FROM annotated WHERE price>0
          GROUP BY user_id,day,category QUALIFY row_number() OVER(
            PARTITION BY user_id,day ORDER BY sum(price) DESC,category ASC)=1""")
        con.execute('DROP TABLE annotated')
        con.execute("""CREATE TABLE user_daily AS
          SELECT s.user_id,s.day,count(*)::BIGINT AS sessions,sum(s.events)::BIGINT AS events,
            sum(s.revenue_cents)::BIGINT AS revenue_cents,t.top_category
          FROM sessions s LEFT JOIN top_categories t USING(user_id,day)
          GROUP BY s.user_id,s.day,t.top_category""")
        con.execute('DROP TABLE top_categories')
        log('sessions and user daily totals computed')
        raw=raw_rows(data/'clicks',['click_id','user_id','campaign_id','ts'])
        # Click duplicates are identical; arg_min and interval union below
        # remove their multiplicity without a separate global deduplication.
        con.execute(f"""CREATE TABLE clicks AS
          WITH parsed AS(SELECT *,parse_ms(ts) AS click_ms FROM ({raw}) r)
          SELECT coalesce(p.person_id,r.user_id) AS user_id,r.click_id,r.campaign_id,r.click_ms AS ts
          FROM parsed r JOIN campaigns c USING(campaign_id)
          LEFT JOIN people p ON r.user_id=p.original_id
          WHERE r.click_id IS NOT NULL AND r.user_id IS NOT NULL
            AND r.click_ms>={lower-week} AND r.click_ms<{upper}""")
        con.execute('DROP TABLE people')
        con.execute("""CREATE TABLE credit_clicks AS
          SELECT user_id,ts,arg_min(campaign_id,click_id) AS campaign_id
          FROM clicks GROUP BY user_id,ts""")
        con.execute(f"""CREATE TABLE credited AS
          SELECT s.day,s.revenue_cents,
            CASE WHEN s.start_ms-c.ts<={week} THEN c.campaign_id ELSE 'organic' END AS campaign_id
          FROM sessions s ASOF LEFT JOIN credit_clicks c
            ON s.user_id=c.user_id AND s.start_ms>=c.ts""")
        con.execute('DROP TABLE credit_clicks')
        # Merge overlapping 7-day campaign intervals before joining sessions,
        # avoiding a potentially huge click-by-session join for whale users.
        con.execute(f"""CREATE TABLE assist_intervals AS
          WITH gaps AS (
            SELECT *,ts-lag(ts) OVER(PARTITION BY user_id,campaign_id ORDER BY ts) AS gap FROM clicks
          ), groups AS (
            SELECT *,sum(CASE WHEN gap IS NULL OR gap>{week} THEN 1 ELSE 0 END)
              OVER(PARTITION BY user_id,campaign_id ORDER BY ts
                   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp FROM gaps
          ) SELECT user_id,campaign_id,min(ts) AS lo,max(ts)+{week} AS hi
          FROM groups GROUP BY user_id,campaign_id,grp""")
        con.execute('DROP TABLE clicks')
        con.execute("""CREATE TABLE campaign_daily AS
          WITH totals AS (
            SELECT campaign_id,day,count(*)::BIGINT AS sessions,
              count(*) FILTER(WHERE revenue_cents>0)::BIGINT AS purchasing_sessions,
              sum(revenue_cents)::BIGINT AS revenue_cents,0::BIGINT AS assisted_sessions
            FROM credited GROUP BY campaign_id,day
            UNION ALL
            SELECT a.campaign_id,s.day,0,0,0,count(*)::BIGINT
            FROM sessions s JOIN assist_intervals a
              ON s.user_id=a.user_id AND s.start_ms BETWEEN a.lo AND a.hi
            GROUP BY a.campaign_id,s.day
          ) SELECT t.campaign_id,coalesce(c.channel,'organic') AS channel,t.day,
            sum(t.sessions)::BIGINT AS sessions,sum(t.purchasing_sessions)::BIGINT AS purchasing_sessions,
            sum(t.revenue_cents)::BIGINT AS revenue_cents,sum(t.assisted_sessions)::BIGINT AS assisted_sessions
          FROM totals t LEFT JOIN campaigns c USING(campaign_id)
          GROUP BY t.campaign_id,c.channel,t.day""")
        log('campaign credits and assists computed')
        for table in ('user_daily','campaign_daily'):
            folder=output/table
            folder.mkdir(parents=True,exist_ok=True)
            con.execute(f"COPY {table} TO {literal(folder/'part-0.parquet')} (FORMAT PARQUET)")
        con.close()
        log('wrote both Parquet tables')


if __name__=='__main__':
    main()
