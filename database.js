const { queue } = require("async");
const { Pool, Client, Query } = require("pg");
const app = require("./app");
const connectionString =
  "postgres://lgnkviol:zFssn9YT3HTQkCUjSW_ENeNVL0874SoF@drona.db.elephantsql.com:5432/lgnkviol";
const pool = new Pool({
  connectionString: connectionString,
});

//====================/company/queue/====================
/* Create Queue */
function createQueue(company_id, queue_id, callback) {
  const query = {
    text: "INSERT INTO queues(company_id, queue_id) VALUES ($1, UPPER($2))",
    values: [company_id, queue_id],
  };
  console.log(query);
  pool.query(query, (err, res) => {
    if (err) {
      callback(err, null);
    } else {
      callback(null, res);
    }
  });
}

/* Update Queue */
function updateQueue(queue_id, status, callback) {
  var query = {
    text: "UPDATE queues SET status = $1 WHERE queue_id = UPPER($2)",
    values: [status, queue_id],
  };
  pool.query(query, (err, res) => {
    if (err) {
      callback(err.stack, null);
    } else {
      callback(res, res.rows[0]);
    }
  });
}

/* Server Available */
function serverAvailable(queue_id, callback) {
  var queue_id = queue_id;
  var value;
  //Check whether queue exists
  var query = {
    text: "SELECT id FROM queues WHERE queue_id = UPPER($1)",
    values: [queue_id],
    rowMode: "array",
  };
  pool.query(query, (err, res) => {
    if (err) {
      callback(err, null);
    } else if (res.rowCount < 1) {
      //   console.log("queue doesnt exist");
      callback(new Error("UNKNOWN_QUEUE"));
    } else {
      //   console.log("queue exists");
      //Check whether customer in queue
      query = {
        text:
          "SELECT customer_id FROM customers WHERE serveravailable = false AND queue_id = UPPER($1)",
        values: [queue_id],
        rowMode: "array",
      };
      pool.query(query, (err, res) => {
        if (err) {
          callback(err, null);
        } else if (res.rowCount < 1) {
          //if row count is 0, return "1": as indicator for empty queue
          callback(null, "0");
        } else {
          //takes the first one
          value = res.rows[0][0];
          query = {
            text:
              "DELETE from customers WHERE serveravailable = false AND queue_id = UPPER($1) AND customer_id = $2",
            // text: 'UPDATE customers SET serveravailable = true WHERE serveravailable = false AND queue_id = UPPER($1) AND customer_id = $2',
            values: [queue_id, value],
          };
          pool.query(query, (err, res) => {
            if (err) {
              callback(err, null);
            } else {
              callback(null, value);
            }
          });
        }
      });
    }
  });
}

/* Arrival Rate */
function arrivalRate(queue_id, from, duration, callback) {
  const query = {
    text:
      "SELECT COUNT(EXTRACT(EPOCH FROM jointime)), EXTRACT(EPOCH FROM jointime) as jointime FROM customers WHERE queue_id = UPPER($1) AND EXTRACT(EPOCH FROM jointime) BETWEEN EXTRACT(EPOCH FROM TIMESTAMP WITH TIME ZONE '" +
      from +
      "') AND (EXTRACT(EPOCH FROM TIMESTAMP WITH TIME ZONE '" +
      from +
      "') + $2 * 60) GROUP BY jointime",
    values: [queue_id, duration],
  };
  pool.query(query, (err, res) => {
    if (err) {
      callback(err.stack, null);
    } else {
      callback(null, res);
    }
  });
}

//====================/customer/queue/====================
/* Check Queue */
function checkQueue(queue_id, customer_id, callback) {
  var query = {};
  if (customer_id == undefined) {
    customer_id = null;
    query = {
      text:
        "SELECT (SELECT COUNT(id) FROM customers WHERE queue_id = UPPER($1)) AS total_people, -1 AS ahead, status FROM queues WHERE queue_id = UPPER($1)",
      values: [queue_id],
      rowMode: "array",
    };
  } else {
    query = {
      text:
        "SELECT (SELECT count(customer_id) FROM customers WHERE id < (SELECT id FROM customers WHERE customer_id = $2 AND queue_id = UPPER($1)) AND queue_id = UPPER($1)) AS ahead, (SELECT id FROM customers WHERE customer_id = $2 AND queue_id = UPPER($1)) AS checker, (SELECT COUNT(id) FROM customers WHERE queue_id = UPPER($1)) AS total_people, status FROM queues WHERE queue_id = UPPER($1)",
      values: [queue_id, customer_id],
      rowMode: "array",
    };
  }
  pool.query(query, (err, res) => {
    // console.log(res);
    if (err) {
      callback(err.stack, null);
    } else if (res.rowCount < 1) {
      callback(new Error("UNKNOWN_QUEUE"), null);
    } else if (res.rows[0][1] == "-1") {
      callback(new Error("NOT_IN_QUEUE"), res.rows[0]);
    } else if (res.rows[0][1] == null) {
      callback(new Error("NOT_IN_QUEUE_WITH_ID"), res.rows[0]);
    } else {
      callback(null, res.rows[0]);
    }
  });
}

/* Join Queue */
function joinQueue(queue_id, customer_id, callback) {
  var query = {
    text: "SELECT status FROM queues WHERE queue_id = UPPER($1);",
    values: [queue_id],
    rowMode: "array",
  };
  pool.query(query, (err, res) => {
    if (err) {
      callback(err.stack, null);
    } else if (res.rowCount < 1) {
      callback(new Error("UNKNOWN_QUEUE"));
    } else if (res.rows[0][0] == "INACTIVE") {
      callback(new Error("INACTIVE_QUEUE"));
    } else {
      query = {
        text:
          "INSERT INTO customers(customer_id,queue_id) VALUES ($1,UPPER($2))",
        values: [customer_id, queue_id],
      };
      pool.query(query, (err, res) => {
        if (err) {
          callback(err, null);
        } else {
          callback(null, res);
        }
      });
    }
  });
}

/* Reset Tables */
function resetTables() {
  const sql = "DELETE FROM customers; DELETE FROM queues";
  return new Promise((resolve, reject) => {
    pool
      .query(sql)
      .then(function (result) {
        resolve(null, result);
      })
      .catch(function (error) {
        reject(error, null);
      });
  });
}

/* Close Database Connections */
function closeDatabaseConnections() {
  return new Promise((resolve, reject) => {
    pool
      .query(sql)
      .then(function (result) {
        resolve(null, result);
      })
      .catch(function (error) {
        reject(error, null);
      });
  });
}

module.exports = {
  createQueue,
  checkQueue,
  joinQueue,
  updateQueue,
  resetTables,
  serverAvailable,
  closeDatabaseConnections,
  arrivalRate,
};
