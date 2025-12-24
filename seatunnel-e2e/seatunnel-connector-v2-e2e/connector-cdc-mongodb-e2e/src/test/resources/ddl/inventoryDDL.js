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

db.getCollection('products').insertOne({"_id": ObjectId("100000000000000000000109"), "name": "bicycle", "description": "Mountain bike with 21 gears", "weight": "1200"});
db.getCollection('products').insertOne({"_id": ObjectId("100000000000000000000110"), "name": "headphones", "description": "Wireless headphones with noise cancellation", "weight": "200"});
db.getCollection('products').insertOne({"_id": ObjectId("100000000000000000000111"), "name": "laptop", "description": "13-inch ultrabook with 16GB RAM and SSD storage", "weight": "1100"});
db.getCollection('products').insertOne({"_id": ObjectId("100000000000000000000112"), "name": "blender", "description": "High-powered blender for smoothies and shakes", "weight": "400"});
db.getCollection('products').insertOne({"_id": ObjectId("100000000000000000000113"), "name": "notebook", "description": "Spiral-bound notebook with ruled pages", "weight": "300"});

db.getCollection('products').updateOne({"name": "scooter"}, {$set: {"weight": "350"}});
db.getCollection('products').updateOne({"name": "car battery"}, {$set: {"description": "High-performance car battery"}});
db.getCollection('products').updateOne({"name": "12-pack drill bits"}, {$set: {"description": "Set of 12 professional-grade drill bits"}});
db.getCollection('products').updateOne({"name": "hammer"}, {$set: {"weight": "100"}});
db.getCollection('products').updateOne({"name": "rocks"}, {$set: {"weight": "1000"}});

db.getCollection('products').deleteOne({"_id": ObjectId("100000000000000000000101")});
db.getCollection('products').deleteOne({"name": "car battery"});
db.getCollection('products').deleteOne({"name": "12-pack drill bits"});
db.getCollection('products').deleteOne({"name": "hammer", "weight": "875"});
db.getCollection('products').deleteOne({"name": "jacket"});


db.getCollection('orders').insertOne({"_id": ObjectId("100000000000000000000106"),"order_number": 102487, "order_date": "2023-11-12", "quantity": 2 , "product_id": ObjectId("100000000000000000000113")});
db.getCollection('orders').insertOne({"_id": ObjectId("100000000000000000000107"),"order_number": 102488, "order_date": "2023-11-13", "quantity": 5 , "product_id": ObjectId("100000000000000000000112")});
db.getCollection('orders').insertOne({"_id": ObjectId("100000000000000000000108"),"order_number": 102489, "order_date": "2023-11-14", "quantity": 6 , "product_id": ObjectId("100000000000000000000111")});
db.getCollection('orders').insertOne({"_id": ObjectId("100000000000000000000109"),"order_number": 102490, "order_date": "2023-11-15", "quantity": 9 , "product_id": ObjectId("100000000000000000000110")});
db.getCollection('orders').insertOne({"_id": ObjectId("100000000000000000000110"),"order_number": 102491, "order_date": "2023-11-16", "quantity": 8 , "product_id": ObjectId("100000000000000000000109")});

db.getCollection('orders').updateOne({"order_number": 102490}, {$set: {"quantity": 99}});

db.getCollection('orders').deleteOne({"order_number": 102487});
db.getCollection('orders').deleteOne({"order_number": 102488});
db.getCollection('orders').deleteOne({"order_number": 102489});
