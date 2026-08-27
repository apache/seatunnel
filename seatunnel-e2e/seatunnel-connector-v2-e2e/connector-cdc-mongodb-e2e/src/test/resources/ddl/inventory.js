// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
//  -- this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
//  the License.  You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

db.getCollection('products').insertOne({"_id": ObjectId("100000000000000000000101"), "name": "scooter", "description": "Small 2-wheel scooter", "weight": "314"});
db.getCollection('products').insertOne({"_id": ObjectId("100000000000000000000102"), "name": "car battery", "description": "12V car battery", "weight": "81"});
db.getCollection('products').insertOne({"_id": ObjectId("100000000000000000000103"), "name": "12-pack drill bits", "description": "12-pack of drill bits with sizes ranging from #40 to #3", "weight": "8"});
db.getCollection('products').insertOne({"_id": ObjectId("100000000000000000000104"), "name": "hammer", "description": "12oz carpenter''s hammer", "weight": "75"});
db.getCollection('products').insertOne({"_id": ObjectId("100000000000000000000105"), "name": "hammer", "description": "12oz carpenter''s hammer", "weight": "875"});
db.getCollection('products').insertOne({"_id": ObjectId("100000000000000000000106"), "name": "hammer", "description": "12oz carpenter''s hammer", "weight": "10"});
db.getCollection('products').insertOne({"_id": ObjectId("100000000000000000000107"), "name": "rocks", "description": "box of assorted rocks", "weight": "53"});
db.getCollection('products').insertOne({"_id": ObjectId("100000000000000000000108"), "name": "jacket", "description": "water resistent black wind breaker", "weight": "1"});


db.getCollection('orders').insertOne({"_id": ObjectId("100000000000000000000101"),"order_number": 102482, "order_date": "2023-11-12", "quantity": 2 , "product_id": ObjectId("100000000000000000000101")});
db.getCollection('orders').insertOne({"_id": ObjectId("100000000000000000000102"),"order_number": 102483, "order_date": "2023-11-13", "quantity": 5 , "product_id": ObjectId("100000000000000000000102")});
db.getCollection('orders').insertOne({"_id": ObjectId("100000000000000000000103"),"order_number": 102484, "order_date": "2023-11-14", "quantity": 6 , "product_id": ObjectId("100000000000000000000103")});
db.getCollection('orders').insertOne({"_id": ObjectId("100000000000000000000104"),"order_number": 102485, "order_date": "2023-11-15", "quantity": 9 , "product_id": ObjectId("100000000000000000000104")});
db.getCollection('orders').insertOne({"_id": ObjectId("100000000000000000000105"),"order_number": 102486, "order_date": "2023-11-16", "quantity": 8 , "product_id": ObjectId("100000000000000000000105")});
