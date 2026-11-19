import hashlib
import os
import random
from datetime import date
from flask import Flask, jsonify, request
app = Flask(__name__)
TOKEN = os.environ.get('API_TOKEN', 'formation-token-2026')
CATEGORIES = ['Mode', 'Maison', 'Beauté', 'Sport', 'Electronique']
def seed(value):
    return int(hashlib.md5(value.encode()).hexdigest()[:12], 16)
def sellers():
    return [{'seller_id': i, 'seller_name': f'Vendeur {i:03d}', 'region': ['Paris','Lyon','Lille','Marseille'][i%4]} for i in range(1, 61)]
def products():
    return [{'product_id': i, 'seller_id': 1+(i-1)%60, 'product_name': f'Produit {i:04d}', 'category': CATEGORIES[i%5], 'price': round(8+(i*13)%190+0.99,2)} for i in range(1, 301)]
def customers():
    return [{'customer_id': i, 'customer_name': f'Client {i:04d}'} for i in range(1, 501)]
@app.before_request
def auth():
    if request.path != '/health' and request.headers.get('Authorization') != 'Bearer '+TOKEN:
        return jsonify({'error':'Unauthorized'}),401
@app.get('/health')
def health(): return jsonify({'status':'ok','version':'demo-1.0'})
@app.get('/sellers')
def seller_route(): return jsonify(sellers()[:int(request.args.get('limit',60))])
@app.get('/products')
def product_route(): return jsonify(products())
@app.get('/customers')
def customer_route(): return jsonify(customers()[:int(request.args.get('limit',500))])
@app.get('/orders')
def orders():
    dt=request.args.get('date','')
    try: date.fromisoformat(dt)
    except ValueError: return jsonify({'error':'date must be YYYY-MM-DD'}),400
    rng=random.Random(seed(dt)); catalog=products(); result=[]
    for i in range(240):
        pr=rng.choice(catalog); qty=rng.randint(1,3); amount=round(qty*pr['price'],2)
        result.append({'order_id': f'{dt}-{i+1:05d}', 'order_date':dt, 'seller_id':pr['seller_id'], 'product_id':pr['product_id'], 'customer_id':rng.randint(1,500), 'quantity':qty, 'unit_price':pr['price'], 'amount':amount, 'status':'completed'})
    return jsonify(result)
if __name__=='__main__': app.run(host='0.0.0.0',port=5000)
