import argparse
import io
import json
import logging
import os
import time
from datetime import date, timedelta
import boto3
import psycopg2
import requests
from botocore.client import Config
from psycopg2.extras import execute_values
from prometheus_client import Counter, Gauge, Histogram, start_http_server
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
LOG=logging.getLogger('marketplace')
RUNS=Counter('marketplace_pipeline_runs_total','Pipeline runs', ['status'])
DURATION=Histogram('marketplace_pipeline_duration_seconds','Run duration')
LAST_SUCCESS=Gauge('marketplace_pipeline_last_success_timestamp_seconds','Last successful run')
ROWS=Gauge('marketplace_pipeline_last_rows','Rows loaded')
API=os.getenv('API_URL','http://localhost:5000')
TOKEN=os.getenv('API_TOKEN','formation-token-2026')
def conn():
    return psycopg2.connect(host=os.getenv('PGHOST','localhost'),port=os.getenv('PGPORT','5433'),dbname=os.getenv('POSTGRES_DB','dwh'),user=os.getenv('POSTGRES_USER','dwh_user'),password=os.getenv('POSTGRES_PASSWORD','dwh_password_2026'))
def client():
    return boto3.client('s3',endpoint_url=os.getenv('MINIO_ENDPOINT','http://localhost:9000'),aws_access_key_id=os.getenv('MINIO_ROOT_USER','minio_admin'),aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD','minio_password_2026'),config=Config(signature_version='s3v4'),region_name='us-east-1')
def fetch(endpoint,params=None):
    for attempt in range(3):
        try:
            r=requests.get(f'{API}/{endpoint}',headers={'Authorization':f'Bearer {TOKEN}'},params=params,timeout=30)
            r.raise_for_status();data=r.json()
            if not isinstance(data,list): raise ValueError(f'{endpoint} must return a JSON list')
            return data
        except (requests.RequestException,ValueError):
            if attempt==2: raise
            time.sleep(2**attempt)
def quality(rows,kind,dt):
    if not rows: raise ValueError(f'Empty dataset: {kind}')
    idfield={'orders':'order_id','sellers':'seller_id','products':'product_id','customers':'customer_id'}[kind]
    ids=[r.get(idfield) for r in rows]
    if any(v is None for v in ids) or len(ids)!=len(set(ids)): raise ValueError(f'Invalid or duplicate IDs: {kind}')
    if kind=='orders':
        if date.fromisoformat(dt)>date.today(): raise ValueError('Future execution date')
        if any(r.get('order_date')!=dt or float(r.get('amount',-1))<0 or int(r.get('quantity',0))<=0 for r in rows): raise ValueError('Invalid orders')
def archive(s3,dt,kind,rows):
    s3.put_object(Bucket='data-lake',Key=f'raw/{kind}/dt={dt}/data.json',Body=json.dumps(rows,ensure_ascii=False).encode(),ContentType='application/json')
def ensure_bucket(s3):
    for i in range(15):
        try:
            s3.head_bucket(Bucket='data-lake');return
        except Exception:
            try: s3.create_bucket(Bucket='data-lake');return
            except Exception:
                if i==14: raise
                time.sleep(2)
def load(dt,data):
    with conn() as db:
        with db.cursor() as c:
            for kind,cols in [('sellers',['seller_id','seller_name','region']),('products',['product_id','seller_id','product_name','category','price']),('customers',['customer_id','customer_name'])]:
                values=[tuple(r.get(col) for col in cols) for r in data[kind]]
                updates=','.join(f'{col}=EXCLUDED.{col}' for col in cols[1:])
                execute_values(c,f"INSERT INTO staging.{kind} ({','.join(cols)}) VALUES %s ON CONFLICT ({cols[0]}) DO UPDATE SET {updates}",values)
            c.execute('DELETE FROM staging.orders WHERE dt=%s',(dt,))
            cols=['order_id','dt','seller_id','product_id','customer_id','quantity','unit_price','amount','status']
            vals=[(r['order_id'],dt,r['seller_id'],r['product_id'],r.get('customer_id'),r['quantity'],r['unit_price'],r['amount'],r.get('status')) for r in data['orders']]
            execute_values(c,f"INSERT INTO staging.orders ({','.join(cols)}) VALUES %s",vals)
            c.execute('''INSERT INTO dwh.dim_seller(seller_id,seller_name,region) SELECT seller_id,seller_name,region FROM staging.sellers ON CONFLICT(seller_id) DO UPDATE SET seller_name=EXCLUDED.seller_name,region=EXCLUDED.region''')
            c.execute('''INSERT INTO dwh.dim_product(product_id,product_name,category,price) SELECT product_id,product_name,category,price FROM staging.products ON CONFLICT(product_id) DO UPDATE SET product_name=EXCLUDED.product_name,category=EXCLUDED.category,price=EXCLUDED.price''')
            c.execute('''INSERT INTO dwh.dim_customer(customer_id,customer_name) SELECT customer_id,customer_name FROM staging.customers ON CONFLICT(customer_id) DO UPDATE SET customer_name=EXCLUDED.customer_name''')
            c.execute('''INSERT INTO dwh.dim_date(date_key,full_date,year,month,day) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',(int(dt.replace('-','')),dt,int(dt[:4]),int(dt[5:7]),int(dt[8:10])))
            c.execute('DELETE FROM dwh.fact_orders WHERE dt=%s',(dt,))
            c.execute('''INSERT INTO dwh.fact_orders(order_id,dt,seller_key,product_key,customer_key,date_key,quantity,amount,commission)
              SELECT o.order_id,o.dt,s.seller_key,p.product_key,cu.customer_key,dd.date_key,o.quantity,o.amount,ROUND(o.amount*0.12,2)
              FROM staging.orders o JOIN dwh.dim_seller s ON s.seller_id=o.seller_id JOIN dwh.dim_product p ON p.product_id=o.product_id
              LEFT JOIN dwh.dim_customer cu ON cu.customer_id=o.customer_id JOIN dwh.dim_date dd ON dd.full_date=o.dt WHERE o.dt=%s AND o.status='completed' ''',(dt,))
            c.execute('''INSERT INTO analytics.daily_summary(dt,total_orders,revenue,average_basket,commission)
              SELECT dt,COUNT(DISTINCT order_id),SUM(amount),ROUND(SUM(amount)/NULLIF(COUNT(DISTINCT order_id),0),2),SUM(commission)
              FROM dwh.fact_orders WHERE dt=%s GROUP BY dt
              ON CONFLICT(dt) DO UPDATE SET total_orders=EXCLUDED.total_orders,revenue=EXCLUDED.revenue,average_basket=EXCLUDED.average_basket,commission=EXCLUDED.commission''',(dt,))
            c.execute('''INSERT INTO analytics.sales_anomalies(dt,revenue,average_previous_7_days,ratio,is_anomaly)
              SELECT d.dt,d.revenue,prev.avg_rev,ROUND(d.revenue/NULLIF(prev.avg_rev,0),4),CASE WHEN prev.n=7 THEN d.revenue < 0.7*prev.avg_rev ELSE NULL END
              FROM analytics.daily_summary d CROSS JOIN LATERAL
              (SELECT AVG(revenue) AS avg_rev,COUNT(*) AS n FROM analytics.daily_summary p WHERE p.dt >= d.dt - INTERVAL '7 days' AND p.dt < d.dt) prev
              WHERE d.dt=%s ON CONFLICT(dt) DO UPDATE SET revenue=EXCLUDED.revenue,average_previous_7_days=EXCLUDED.average_previous_7_days,ratio=EXCLUDED.ratio,is_anomaly=EXCLUDED.is_anomaly''',(dt,))
            c.execute('SELECT COUNT(*) FROM dwh.fact_orders WHERE dt=%s',(dt,));return c.fetchone()[0]
def run(dt,replay=False):
    date.fromisoformat(dt)
    start=time.monotonic(); run_id=None
    with conn() as db:
        with db.cursor() as c:
            c.execute('INSERT INTO analytics.pipeline_runs(run_date,status) VALUES(%s,%s) RETURNING id',(dt,'running'));run_id=c.fetchone()[0]
    try:
        s3=client();ensure_bucket(s3)
        data={}
        for kind in ('sellers','products','customers','orders'):
            if replay:
                body=s3.get_object(Bucket='data-lake',Key=f'raw/{kind}/dt={dt}/data.json')['Body'].read()
                data[kind]=json.loads(body)
            else:
                data[kind]=fetch(kind,{'date':dt} if kind=='orders' else None)
            quality(data[kind],kind,dt)
            if not replay:archive(s3,dt,kind,data[kind])
        n=load(dt,data)
        with conn() as db:
            with db.cursor() as c:c.execute('UPDATE analytics.pipeline_runs SET status=%s,finished_at=NOW(),rows_loaded=%s WHERE id=%s',('success',n,run_id))
        ROWS.set(n);LAST_SUCCESS.set_to_current_time();RUNS.labels('success').inc();LOG.info('SUCCESS date=%s rows=%s',dt,n)
        return n
    except Exception as exc:
        RUNS.labels('failed').inc();LOG.exception('FAILED date=%s',dt)
        with conn() as db:
            with db.cursor() as c:c.execute('UPDATE analytics.pipeline_runs SET status=%s,finished_at=NOW(),error_message=%s WHERE id=%s',('failed',str(exc)[:1000],run_id))
        raise
    finally:DURATION.observe(time.monotonic()-start)
def main():
    parser=argparse.ArgumentParser();parser.add_argument('command',choices=['run','replay','backfill','serve']);parser.add_argument('--date');parser.add_argument('--days',type=int,default=7);parser.add_argument('--interval',type=int,default=86400);args=parser.parse_args()
    
    if args.command == 'serve':
        start_http_server(
            int(os.getenv('METRICS_PORT', '8000'))
        )
        LOG.info("Metrics server started")

        while True:
            time.sleep(60)

    elif args.command == 'backfill':

        end=date.fromisoformat(args.date) if args.date else date.today()-timedelta(days=1)
        for i in reversed(range(args.days)):run((end-timedelta(days=i)).isoformat())
    else:run(args.date or (date.today()-timedelta(days=1)).isoformat(),args.command=='replay')
if __name__=='__main__':main()
