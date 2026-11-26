// Demo dataset for mogoDB: a small e-commerce shop.
// Run with: mongosh --quiet --port 27017 --file mogo/demo/seed.js

const db = db.getSiblingDB('shop');

db.dropDatabase();

db.createCollection('customers');
db.createCollection('products');
db.createCollection('orders');
db.createCollection('reviews');

db.customers.insertMany([
  { _id: 'c1', name: 'Ada Lovelace', email: 'ada@example.com', country: 'GB', joined: ISODate('2024-01-12'), vip: true },
  { _id: 'c2', name: 'Grace Hopper', email: 'grace@example.com', country: 'US', joined: ISODate('2024-03-04'), vip: true },
  { _id: 'c3', name: 'Alan Turing', email: 'alan@example.com', country: 'GB', joined: ISODate('2024-06-21'), vip: false },
  { _id: 'c4', name: 'Katherine Johnson', email: 'katherine@example.com', country: 'US', joined: ISODate('2024-09-09'), vip: false },
  { _id: 'c5', name: 'Linus Torvalds', email: 'linus@example.com', country: 'FI', joined: ISODate('2025-02-14'), vip: false },
]);

db.products.insertMany([
  { _id: 'p1', name: 'Mechanical Keyboard', category: 'peripherals', price: 129.99, stock: 42, tags: ['usb-c', 'hot-swap'] },
  { _id: 'p2', name: '27" 4K Monitor', category: 'displays', price: 449.0, stock: 11, tags: ['ips', 'hdr'] },
  { _id: 'p3', name: 'Ergonomic Mouse', category: 'peripherals', price: 79.5, stock: 87, tags: ['wireless'] },
  { _id: 'p4', name: 'Laptop Stand', category: 'accessories', price: 39.0, stock: 0, tags: ['aluminium'] },
  { _id: 'p5', name: 'USB-C Dock', category: 'accessories', price: 189.0, stock: 23, tags: ['usb-c', 'dual-monitor'] },
  { _id: 'p6', name: 'Noise-Cancelling Headset', category: 'audio', price: 299.0, stock: 5, tags: ['bluetooth', 'anc'] },
]);

db.orders.insertMany([
  { _id: 1001, customer: 'c1', items: [{ product: 'p1', qty: 1 }, { product: 'p3', qty: 2 }], total: 288.99, status: 'delivered', placed: ISODate('2025-01-05') },
  { _id: 1002, customer: 'c2', items: [{ product: 'p2', qty: 2 }], total: 898.0, status: 'delivered', placed: ISODate('2025-01-19') },
  { _id: 1003, customer: 'c1', items: [{ product: 'p6', qty: 1 }], total: 299.0, status: 'shipped', placed: ISODate('2025-02-02') },
  { _id: 1004, customer: 'c3', items: [{ product: 'p5', qty: 1 }], total: 189.0, status: 'processing', placed: ISODate('2025-02-20') },
  { _id: 1005, customer: 'c4', items: [{ product: 'p1', qty: 1 }, { product: 'p5', qty: 1 }], total: 318.99, status: 'processing', placed: ISODate('2025-03-01') },
  { _id: 1006, customer: 'c5', items: [{ product: 'p3', qty: 1 }], total: 79.5, status: 'cancelled', placed: ISODate('2025-03-08') },
]);

db.reviews.insertMany([
  { _id: 'r1', product: 'p1', customer: 'c1', rating: 5, title: 'Best keyboard I have owned', body: 'Hot-swap sockets are perfect.', date: ISODate('2025-01-10') },
  { _id: 'r2', product: 'p1', customer: 'c3', rating: 4, title: 'Great, a bit loud', body: 'Lubed stabilisers out of the box would be nice.', date: ISODate('2025-01-15') },
  { _id: 'r3', product: 'p2', customer: 'c2', rating: 5, title: 'Superb panel', body: 'HDR actually works.', date: ISODate('2025-01-25') },
  { _id: 'r4', product: 'p6', customer: 'c1', rating: 3, title: 'Good ANC, mediocre mic', body: 'Music sounds great; calls less so.', date: ISODate('2025-02-05') },
  { _id: 'r5', product: 'p3', customer: 'c4', rating: 5, title: 'Wrist pain gone', body: 'Took a week to adjust.', date: ISODate('2025-02-11') },
]);

db.products.createIndex({ category: 1, price: -1 });
db.orders.createIndex({ customer: 1, placed: -1 });
db.orders.createIndex({ status: 1 });
db.reviews.createIndex({ product: 1, rating: -1 });
db.customers.createIndex({ email: 1 }, { unique: true });

print('seeded shop: ' +
  db.customers.countDocuments() + ' customers, ' +
  db.products.countDocuments() + ' products, ' +
  db.orders.countDocuments() + ' orders, ' +
  db.reviews.countDocuments() + ' reviews');