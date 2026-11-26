// Second demo wave for the `shop` database: more customers, products, orders and reviews.
// Idempotent: re-running updates existing `_id`s instead of failing on duplicates.
// Run with: mongosh --quiet --port 27017 --file mogo/demo/seed_more.js

const db = db.getSiblingDB('shop');

db.customers.insertMany([
  { _id: 'c6', name: 'Margaret Hamilton', email: 'margaret@example.com', country: 'US', joined: ISODate('2025-04-02'), vip: false },
  { _id: 'c7', name: 'Dennis Ritchie', email: 'dennis@example.com', country: 'US', joined: ISODate('2025-04-18'), vip: false },
  { _id: 'c8', name: 'Barbara Liskov', email: 'barbara@example.com', country: 'US', joined: ISODate('2025-05-06'), vip: true },
  { _id: 'c9', name: 'Ken Thompson', email: 'ken@example.com', country: 'US', joined: ISODate('2025-05-27'), vip: false },
  { _id: 'c10', name: 'Radia Perlman', email: 'radia@example.com', country: 'US', joined: ISODate('2025-06-11'), vip: false },
  { _id: 'c11', name: 'Anders Hejlsberg', email: 'anders@example.com', country: 'DK', joined: ISODate('2025-06-30'), vip: false },
  { _id: 'c12', name: 'Yukihiro Matsumoto', email: 'yukihiro@example.com', country: 'JP', joined: ISODate('2025-07-14'), vip: false },
], { ordered: false });

db.products.insertMany([
  { _id: 'p7', name: '34" Ultrawide Monitor', category: 'displays', price: 899.0, stock: 7, tags: ['ips', 'ultrawide', 'hdr'] },
  { _id: 'p8', name: 'Mechanical Numpad', category: 'peripherals', price: 59.0, stock: 64, tags: ['usb-c', 'hot-swap'] },
  { _id: 'p9', name: 'Monitor Arm', category: 'accessories', price: 119.0, stock: 31, tags: ['gas-spring', 'vesa'] },
  { _id: 'p10', name: 'Webcam 4K', category: 'video', price: 149.0, stock: 18, tags: ['usb-c', 'hdr'] },
  { _id: 'p11', name: 'Studio Microphone', category: 'audio', price: 219.0, stock: 9, tags: ['usb-c', 'cardioid'] },
  { _id: 'p12', name: 'Footrest', category: 'accessories', price: 49.0, stock: 40, tags: ['adjustable'] },
], { ordered: false });

db.orders.insertMany([
  { _id: 1007, customer: 'c3', items: [{ product: 'p7', qty: 1 }], total: 899.0, status: 'delivered', placed: ISODate('2025-03-15') },
  { _id: 1008, customer: 'c6', items: [{ product: 'p8', qty: 2 }, { product: 'p3', qty: 1 }], total: 197.5, status: 'delivered', placed: ISODate('2025-03-22') },
  { _id: 1009, customer: 'c8', items: [{ product: 'p10', qty: 1 }, { product: 'p11', qty: 1 }], total: 368.0, status: 'shipped', placed: ISODate('2025-04-03') },
  { _id: 1010, customer: 'c2', items: [{ product: 'p9', qty: 3 }], total: 357.0, status: 'delivered', placed: ISODate('2025-04-19') },
  { _id: 1011, customer: 'c12', items: [{ product: 'p6', qty: 1 }], total: 299.0, status: 'processing', placed: ISODate('2025-05-02') },
  { _id: 1012, customer: 'c9', items: [{ product: 'p12', qty: 1 }, { product: 'p8', qty: 1 }], total: 108.0, status: 'processing', placed: ISODate('2025-05-21') },
  { _id: 1013, customer: 'c10', items: [{ product: 'p2', qty: 1 }, { product: 'p9', qty: 1 }], total: 568.0, status: 'cancelled', placed: ISODate('2025-06-04') },
  { _id: 1014, customer: 'c11', items: [{ product: 'p1', qty: 1 }, { product: 'p8', qty: 1 }], total: 188.99, status: 'shipped', placed: ISODate('2025-06-23') },
  { _id: 1015, customer: 'c4', items: [{ product: 'p7', qty: 1 }, { product: 'p10', qty: 1 }], total: 1048.0, status: 'processing', placed: ISODate('2025-07-08') },
], { ordered: false });

db.reviews.insertMany([
  { _id: 'r6', product: 'p7', customer: 'c3', rating: 5, title: 'Immense screen', body: 'Replaced two monitors with one.', date: ISODate('2025-03-20') },
  { _id: 'r7', product: 'p8', customer: 'c6', rating: 4, title: 'Solid numpad', body: 'Switches feel good; cable is short.', date: ISODate('2025-03-25') },
  { _id: 'r8', product: 'p10', customer: 'c8', rating: 5, title: 'Great in low light', body: 'Autofocus is quick.', date: ISODate('2025-04-08') },
  { _id: 'r9', product: 'p11', customer: 'c8', rating: 4, title: 'Warm sound', body: 'Needs a pop filter.', date: ISODate('2025-04-09') },
  { _id: 'r10', product: 'p9', customer: 'c2', rating: 5, title: 'Rock solid', body: 'No wobble at full extension.', date: ISODate('2025-04-22') },
  { _id: 'r11', product: 'p6', customer: 'c12', rating: 2, title: 'Stopped charging', body: 'Left ear cup died after a month.', date: ISODate('2025-05-05') },
  { _id: 'r12', product: 'p12', customer: 'c9', rating: 4, title: 'Good value', body: 'Height range is limited.', date: ISODate('2025-05-24') },
], { ordered: false });

print('shop now: ' +
  db.customers.countDocuments() + ' customers, ' +
  db.products.countDocuments() + ' products, ' +
  db.orders.countDocuments() + ' orders, ' +
  db.reviews.countDocuments() + ' reviews');