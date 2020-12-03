const express = require("express"); // DO NOT DELETE
const cors = require("cors");
const morgan = require("morgan");
const app = express(); // DO NOT DELETE
const bodyParser = require("body-parser");

const database = require("./database");
const { queue } = require("async");

app.use(morgan("dev"));
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

/**
 * =====================================================================
 * ========================== CODE STARTS HERE =========================
 * =====================================================================
 */

/**
 * ========================== SETUP APP =========================
 */

/**
 * JSON Body
 */
const jsonschema = require("jsonschema");
function isValid(instance, schema) {
  return jsonschema.validate(instance, schema).valid;
}

/* Create Queue JSON Schema */
const createQueueSchema = {
  type: "object",
  properties: {
    company_id: {
      type: "integer",
      minimum: 1000000000,
      maximum: 9999999999,
    },
    queue_id: {
      type: "string",
      pattern: "^[a-zA-Z0-9]{10}$",
    },
  },
  required: ["company_id", "queue_id"],
};

/* Update Queue JSON Schema */
const statusSchema = {
  type: "object",
  properties: {
    status: {
      type: "string",
      pattern: "^(ACTIVATE|DEACTIVATE)$",
    },
  },
  required: ["status"],
};
const queueidSchema = {
  type: "object",
  properties: {
    queue_id: {
      type: "string",
      pattern: "^[a-zA-Z0-9]{10}$",
    },
  },
  required: ["queue_id"],
};

/* Arrival Rate JSON Schema */
const arrivalRateSchema = {
  type: "object",
  properties: {
    queue_id: {
      type: "string",
      pattern: "^[a-zA-Z0-9]{10}$",
    },
    from: {
      type: "string",
      pattern:
        "^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})[+-]([0-9]{2}):([0-9]{2})$",
    },
    duration: {
      type: "integer",
      minimum: 1,
      maximum: 1440,
    },
  },
  required: ["queue_id", "from", "duration"],
};

/* Join Queue JSON Schema */
const joinQueueSchema = {
  type: "object",
  properties: {
    customer_id: {
      type: "integer",
      minimum: 1000000000,
      maximum: 9999999999,
    },
    queue_id: {
      type: "string",
      pattern: "^[a-zA-Z0-9]{10}$",
    },
  },
  required: ["customer_id", "queue_id"],
};

/* Check Queue JSON Schema */
const checkQueueSchema = {
  type: "object",
  properties: {
    customer_id: {
      type: "integer",
      minimum: 1000000000,
      maximum: 9999999999,
    },
    queue_id: {
      type: "string",
      pattern: "^[a-zA-Z0-9]{10}$",
    },
  },
  required: ["queue_id"],
};

/* Server Available JSON Schema */
const serverAvailSchema = {
  type: "object",
  properties: {
    queue_id: {
      type: "string",
      pattern: "^[a-zA-Z0-9]{10}$",
    },
  },
  required: ["queue_id"],
};

/**
 * ========================== RESET API =========================
 */

/**
 * Reset API
 */
app.post("/reset", function (req, res) {
  database
    .resetTables()
    .then(function (results) {
      return res.status(200).send(results);
    })
    .catch(function (err) {
      res.status(500).send(error);
    });
});

/**
 * ========================== COMPANY =========================
 */

/**
 * Company: Create Queue
 */
app.post("/company/queue", function (req, res) {
  var queue_id = req.body.queue_id;
  var company_id = req.body.company_id;
  if (isValid(req.body, createQueueSchema)) {
    database.createQueue(company_id, queue_id, function (err, result) {
      if (!err) {
        res.status(201).send("success");
      } else if (err.code == "23505") {
        queueIdExist(res, queue_id);
      } else {
        unexpectedError(res);
      }
    });
  } else {
    invalidQuery("JSON", res);
  }
});

/**
 * Company: Update Queue
 */
app.put("/company/queue", function (req, res) {
  var queue_id = req.query.queue_id;
  var statusBody = req.body.status;
  var status;
  if (isValid(req.query, queueidSchema)) {
    if (isValid(req.body, statusSchema)) {
      queue_id = queue_id.toUpperCase();
      if (statusBody == "ACTIVATE") {
        status = "ACTIVE";
      } else if (statusBody == "DEACTIVATE") {
        status = "INACTIVE";
      }
      database.updateQueue(queue_id, status, function (err, result) {
        if (err.rowCount == 1) {
          const resObj = {
            queue_id: queue_id,
            status: status,
          };
          res.status(200).send(resObj);
        } else if (err.rowCount != 1) {
          queueNotFound(res, queue_id);
        } else {
          unexpectedError(res);
        }
      });
    } else {
      invalidQuery("JSON", res);
    }
  } else {
    invalidQuery("QUERY", res);
  }
});

/**
 * Company: Server Available
 */
app.put("/company/server", function (req, res) {
  var queue_id = req.body.queue_id;
  if (isValid(req.body, serverAvailSchema)) {
    database.serverAvailable(queue_id, function (err, result) {
      if (err) {
        err.name = "";
      }
      if (!err) {
        const obj = {
          customer_id: parseInt(result),
        };
        res.status(200).send(obj);
      } else if (result == "1") {
        const obj = {
          customer_id: 0,
        };
        res.status(200).send(obj);
      } else if (err == "UNKNOWN_QUEUE") {
        queueNotFound(res, queue_id);
      } else {
        res.send(err);
      }
    });
  } else {
    invalidQuery("JSON", res);
  }
});

/**
 * Company: Arrival Rate
 */
app.get("/company/arrival_rate", function (req, res) {
  req.query.duration = parseInt(req.query.duration);
  var queue_id = req.query.queue_id;
  var from = req.query.from;
  var duration = req.query.duration;
  var count, timestamp, resObj;
  resArray = [];
  if (isValid(req.query, arrivalRateSchema)) {
    database.arrivalRate(queue_id, from, duration, function (err, result) {
      if (!err) {
        if (result.rows.length > 0) {
          for (i = 0; i < result.rows.length; i++) {
            count = result.rows[i].count;
            timestamp = result.rows[i].jointime;
            resObj = {
              timestamp: timestamp,
              count: count,
            };
            resArray.push(resObj);
          }
          res.status(200).send(resArray);
        } else {
          queueNotFound(res, queue_id);
        }
      } else {
        unexpectedError(res);
      }
    });
  } else {
    invalidQuery("QUERY", res);
  }
});
/**
 * ========================== CUSTOMER =========================
 */

/**
 * Customer: Join Queue
 */
app.post("/customer/queue", function (req, res) {
  var customer_id = req.body.customer_id;
  var queue_id = req.body.queue_id;
  if (isValid(req.body, joinQueueSchema)) {
    database.joinQueue(queue_id, customer_id, function (err, result) {
      if (err) {
        err.name = "";
      }
      if (!err) {
        const resObj = {
          customer_id: customer_id,
          queue_id: queue_id,
        };
        res.status(201).send(resObj);
      } else if (err.code == "23505") {
        customerAlreadyInQueue(customer_id, queue_id);
      } else if (err == "INACTIVE_QUEUE") {
        // queueInActive(queue_id);
        const errObj = {
          error: "Queue " + queue_id + " Is Inactive!",
          code: "INACTIVE_QUEUE",
        };
        res.status(422).send(errObj);
      } else if (err == "UNKNOWN_QUEUE") {
        queueNotFound(res, queue_id);
      } else {
        unexpectedError(res);
      }
    });
  } else {
    invalidQuery("JSON", res);
  }
});

/**
 * Customer: Check Queue
 */
app.get("/customer/queue/", function (req, res) {
  var queue_id = req.query.queue_id;
  var customer_id = req.query.customer_id;
  var objChkQ = {};
  if (customer_id == undefined) {
    objChkQ = {
      queue_id: queue_id,
    };
  } else {
    objChkQ = {
      customer_id: parseInt(customer_id),
      queue_id: queue_id,
    };
  }
  // console.log(isValid(objChkQ,checkQueueSchema));
  if (isValid(objChkQ, checkQueueSchema)) {
    database.checkQueue(queue_id, customer_id, function (err, result) {
      if (err) {
        err.name = "";
      }
      if (!err) {
        const resObj = {
          total: parseInt(result[2]),
          ahead: parseInt(result[0]),
          status: result[3],
        };
        res.status(200).send(resObj);
      } else if (err == "UNKNOWN_QUEUE") {
        queueNotFound(res, queue_id);
      } else if (err == "NOT_IN_QUEUE") {
        const resObj = {
          total: parseInt(result[0]),
          ahead: -1,
          status: result[2],
        };
        res.status(200).send(resObj);
      } else if (err == "NOT_IN_QUEUE_WITH_ID") {
        const resObj = {
          total: parseInt(result[2]),
          ahead: -1,
          status: result[3],
        };
        res.status(200).send(resObj);
      } else {
        res.status(500).send("Error");
      }
    });
  } else {
    const obj = {
      error: "ID should be 10-digits",
      code: "INVALID_QUERY_STRING",
    };
    res.status(400).send(obj);
  }
});
/**
 * ========================== UTILS =========================
 */

/**
 * 404
 */
function queueNotFound(res, queue_id) {
  const obj = {
    error: "Queue Id: " + queue_id + " Not Found",
    code: "UNKNOWN_QUEUE",
  };
  return res.status(404).send(obj);
}

/**
 * Error Handler
 */
function invalidQuery(id, res) {
  var obj;
  if (id == "QUERY") {
    obj = {
      error: "You have an invalid query string!",
      code: "INVALID_QUERY_STRING",
    };
  } else if (id == "JSON") {
    obj = {
      error: "You have an invalid JSON body!",
      code: "INVALID_JSON_BODY",
    };
  }
  return res.status(400).send(obj);
}

function unexpectedError(res) {
  const obj = {
    error: "Unable to establish connection with database",
    code: "UNEXPECTED_ERROR",
  };
  return res.status(500).send(obj);
}

function queueIdExist(res, queue_id) {
  const obj = {
    error: "Queue Id: " + queue_id + " already exists",
    code: "QUEUE_EXISTS",
  };
  return res.status(422).send(obj);
}

function customerAlreadyInQueue(customer_id, queue_id) {
  const obj = {
    error: "Customer: " + customer_id + " already in Queue: " + queue_id,
    code: "ALREADY_IN_QUEUE",
  };
  return res.status(422).send(obj);
}

function queueInActive(queue_id) {
  const Obj = {
    error: "Queue " + queue_id + " Is Inactive!",
    code: "INACTIVE_QUEUE",
  };
  return res.status(422).send(Obj);
}

function tearDown() {
  // DO NOT DELETE
  return database.closeDatabaseConnections();
}

/**
 *  NOTE! DO NOT RUN THE APP IN THIS FILE.
 *
 *  Create a new file (e.g. server.js) which imports app from this file and run it in server.js
 */

module.exports = { app, tearDown }; // DO NOT DELETE
