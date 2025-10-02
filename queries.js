// queries.js
const { MongoClient } = require("mongodb");

async function run() {
  const uri = "mongodb://localhost:27017"; // change if using Atlas
  const client = new MongoClient(uri);

  try {
    await client.connect();
    const db = client.db("plp_bookstore"); // use your DB name
    const books = db.collection("books");

    // -----------------------
    // Task 3: Advanced Queries
    // -----------------------

    console.log("\n1. Books in stock after 2010:");
    const inStockBooks = await books.find(
      { in_stock: true, published_year: { $gt: 2010 } },
      { projection: { title: 1, author: 1, price: 1, _id: 0 } }
    ).toArray();
    console.log(inStockBooks);

    console.log("\n2. Books sorted by price ascending:");
    console.log(await books.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).sort({ price: 1 }).toArray());

    console.log("\n3. Books sorted by price descending:");
    console.log(await books.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).sort({ price: -1 }).toArray());

    console.log("\n4. Pagination (Page 1 - 5 books):");
    console.log(await books.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).sort({ title: 1 }).limit(5).skip(0).toArray());

    console.log("\n4. Pagination (Page 2 - 5 books):");
    console.log(await books.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).sort({ title: 1 }).limit(5).skip(5).toArray());

    // -----------------------
    // Task 4: Aggregation
    // -----------------------

    console.log("\n5. Average price of books by genre:");
    console.log(await books.aggregate([
      { $group: { _id: "$genre", averagePrice: { $avg: "$price" } } }
    ]).toArray());

    console.log("\n6. Author with most books:");
    console.log(await books.aggregate([
      { $group: { _id: "$author", totalBooks: { $sum: 1 } } },
      { $sort: { totalBooks: -1 } },
      { $limit: 1 }
    ]).toArray());

    console.log("\n7. Books grouped by decade:");
    console.log(await books.aggregate([
      { $project: { decade: { $multiply: [{ $floor: { $divide: ["$published_year", 10] } }, 10] } } },
      { $group: { _id: "$decade", booksCount: { $sum: 1 } } },
      { $sort: { _id: 1 } }
    ]).toArray());

    // -----------------------
    // Task 5: Indexing
    // -----------------------

    console.log("\n8. Creating index on title...");
    await books.createIndex({ title: 1 });

    console.log("9. Creating compound index on author + published_year...");
    await books.createIndex({ author: 1, published_year: -1 });

    console.log("\n10. Explain query without index:");
    console.log(await books.find({ title: "The Hobbit" }).explain("executionStats"));

    console.log("\n11. Explain query using compound index:");
    console.log(await books.find({ author: "J.K. Rowling", published_year: { $gt: 1990 } }).explain("executionStats"));

  } catch (err) {
    console.error(err);
  } finally {
    await client.close();
  }
}

run();